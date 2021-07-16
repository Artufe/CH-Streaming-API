import copy
import os
import sys
import json
import time
import uuid

import pytz
import pika
import requests
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import CompanyProfileStream, Company, FilingsStream, InsolvencyStream, OfficersStream, ChargesStream, PersonsStream
from funcs import company_event_process, make_company_event_store

# Base class that contains all the shared methods
class Streamer:
    api_key = os.environ.get("CH_API_KEY")
    req_session = requests.session()
    req_session.headers = {"User-Agent": "Stream Receiver"}
    timezone = pytz.timezone("Europe/London")

    # DB connection init, class wide
    engine = create_engine('postgresql://postgres:BigBlueBattery123!@localhost:5500/ch')
    Session = sessionmaker(bind=engine)
    session = Session()

    # Connect to to RabbitMQ
    connection = pika.BlockingConnection(pika.URLParameters('amqp://arthur:FlaskTubCupp@localhost:5672/%2F'))
    channel = connection.channel()
    channel.queue_declare("CHStreamQ", arguments={"x-message-ttl": 300000})

    # model: SQLAlchemy model class appropriate to the stream type (url)
    # event_function: the function that maps the raw event JSON to model
    def __init__(self, url, model, event_function):
        self.url = url
        self.model = model
        self.event_to_model = event_function

        # Resume from last timepoint found in db
        last_timepoint = self.session.query(model.event_timepoint).order_by(model.event_timepoint.desc()).first()[0]
        print("Resuming stream from last timepoint of", last_timepoint)

        # If this is the companyprofile stream, load up latest event for each company in memory
        # Saves on querying database, which is slow and allows to keep up with realtime stream
        # This event_store is kept updated with each new event replacing old one,
        # or if new company detected it does the appending of a new key
        if self.model == CompanyProfileStream:
            self.events_store = make_company_event_store(self.session)

        # Actually connect to stream from Companies House
        self.stream = Streamer.req_session.get(url + f"?timepoint={last_timepoint+1}", stream=True, auth=requests.auth.HTTPBasicAuth(Streamer.api_key, ""))

    # The event and resource keys are consistent across all streams
    # Initialize the model with those fields populated
    def populate_model(self, event_dict, model):
        model = model()
        event = event_dict.copy()
        model.resource_id = event["resource_id"]
        model.resource_kind = event["resource_kind"]
        model.resource_uri = event["resource_uri"]

        model.event_fields_changed = event["event"].get("fields_changed", [])
        model.event_published_at = datetime.strptime(event["event"]["published_at"], "%Y-%m-%dT%H:%M:%S")
        model.event_published_at.astimezone(self.timezone)

        model.event_timepoint = int(event["event"]["timepoint"])
        if self.model == CompanyProfileStream:
            model.company_profile_stream = uuid.uuid4()
        else:
            model.id = uuid.uuid4()

        return model

    def compare_models(self, old_event, new_event):
        fields_changed = []
        for key, value in new_event.__dict__.items():
            if key[0] == "_" or key in ["company_profile_stream", "resource_id", "resource_kind", "resource_uri",
                                        "event_timepoint", "event_fields_changed", "event_published_at", "event_type",
                                        "data_etag"]:
                continue

            # Convert all fields/keys that are supposed to be ints, to ints.
            if key in ["data_accounts_accounting_reference_date_day", "data_accounts_accounting_reference_date_month",
                       "data_foreign_company_details_accounts_account_period_from_day", "data_foreign_company_details_accounts_account_period_from_month",
                       "data_foreign_company_details_accounts_account_period_to_day", "data_foreign_company_details_accounts_account_period_to_month",
                       "data_foreign_company_details_accounts_must_file_within_months"]:
                value = int(value)

            if old_event[key] != value:
                fields_changed.append(key)

        return fields_changed

    # Populate message with elements that are common between all of the streams
    def base_message_constructor(self, model, company_number):
        message = {"stream": self.url.split("/")[-1],
                   "event_type": None,
                   "event_published_at": model.event_published_at.strftime("%Y-%m-%d %H:%M:%S"),
                   "event_id": str(model.company_profile_stream) if self.model == CompanyProfileStream else str(model.id),
                   "company_number": company_number}

        return message

    # Takes a dictionary and sends it as a message to RabbitMQ instance
    def send_message(self, message):
        try:
            self.channel.basic_publish(exchange='',
                                       routing_key='CHStreamQ',
                                       body=json.dumps(message))
        except pika.exceptions.StreamLostError:
            # Re-establish the connection
            print("RabbitMQ connection dropped, re-creating")
            self.connection = pika.BlockingConnection(pika.URLParameters('amqp://arthur:FlaskTubCupp@localhost:5672/%2F'))
            self.channel = self.connection.channel()
            self.send_message(message)

    def read_from_stream(self):
        for line in self.stream.iter_lines():
            if line:
                event = json.loads(line.decode('utf-8'))
                # Extract the base attributes such as event_published_at, resource_id etc
                model = self.populate_model(event, self.model)
                model = self.pipeline(event, model)

                self.session.add(model)
                self.session.commit()

    def start_stream(self):
        # Infinite loop to restart the stream in case of crash (Chunked response)
        while True:
            try:
                self.read_from_stream()
            except (requests.exceptions.ChunkedEncodingError, requests.exceptions.StreamConsumedError) as e:
                print("Stream error encountered,", e)
                last_timepoint = self.session.query(self.model.event_timepoint)\
                                .order_by(self.model.event_timepoint.desc()).first()[0]
                print("Restarting stream from last timepoint of", last_timepoint)
                self.stream = Streamer.req_session.get(self.url + f"?timepoint={last_timepoint+1}", stream=True, auth=requests.auth.HTTPBasicAuth(Streamer.api_key, ""))


class CompanyStreamer(Streamer):

    def company_exists(self, company_number):
        q = self.session.query(Company.CompanyNumber).filter(Company.CompanyNumber == company_number.upper())
        if self.session.query(q.exists()).scalar():
            return True
        else:
            return False

    # All of the custom logic for processing the event individually goes here
    def pipeline(self, event, model):
        company = self.event_to_model(event, model)

        # Check if a event for this company exists in the event_store
        previous_event = self.events_store.get(company.data_company_number, False)

        self.events_store[company.data_company_number] = copy.deepcopy(company.__dict__)

        # Construct the base of a message for RabbitMQ
        message = self.base_message_constructor(company, company.data_company_number)

        if not previous_event:
            message["event_type"] = "company.new"

        # If a previous event was found, compare old row with new row and fire off event if any fields are changed
        if previous_event:
            message["event_type"] = "company.update"
            changed_fields = self.compare_models(previous_event, company)
            if changed_fields:
                message["changed_fields"] = ",".join(changed_fields)
            else:
                message["changed_fields"] = ""

        self.send_message(message)

        return company



class GenericTempStreamer(Streamer):

    def pipeline(self, event, model):
        model.company_number = event["resource_uri"].split("/")[2]
        model.data = event["data"]

        # Construct the base of a message for RabbitMQ
        message = self.base_message_constructor(model, model.company_number)

        if self.model == FilingsStream:
            if "description" in event["data"]:
                message["event_type"] = event["data"]["description"]
            elif "associated_filings" in event["data"] and len(event["data"]["associated_filings"]) == 0:
                message["event_type"] = event["data"]["associated_filings"][0]["description"]
            else:
                print("Unknown event type for ", str(model.id))

        self.send_message(message)

        return model

if __name__ == "__main__":
    # q = Queue()
    stream_to_launch = sys.argv[1]
    if stream_to_launch == "company":
        streamer = CompanyStreamer("https://stream.companieshouse.gov.uk/companies", CompanyProfileStream, company_event_process)
    # Temporary
    elif stream_to_launch == "filing":
        streamer = GenericTempStreamer("https://stream.companieshouse.gov.uk/filings", FilingsStream, company_event_process)
    elif stream_to_launch == "insolvency":
        streamer = GenericTempStreamer("https://stream.companieshouse.gov.uk/insolvency-cases", InsolvencyStream, company_event_process)
    elif stream_to_launch == "officer":
        streamer = GenericTempStreamer("https://stream.companieshouse.gov.uk/officers", OfficersStream, company_event_process)
    elif stream_to_launch == "charges":
        streamer = GenericTempStreamer("https://stream.companieshouse.gov.uk/charges", ChargesStream, company_event_process)
    elif stream_to_launch == "persons":
        streamer = GenericTempStreamer("https://stream.companieshouse.gov.uk/persons-with-significant-control", PersonsStream, company_event_process)

    streamer.start_stream()




