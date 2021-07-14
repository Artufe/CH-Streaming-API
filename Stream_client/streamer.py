import copy
import os
import sys
import json
import time

import pytz
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

    # model: SQLAlchemy model class appropriate to the stream type (url)
    # event_function: the function that maps the raw event JSON to model
    def __init__(self, url, model, event_function):
        self.model = model
        self.event_to_model = event_function
        # Resume from last timepoint found in db
        last_timepoint = self.session.query(model.event_timepoint).order_by(model.event_timepoint.desc()).first()[0]
        self.url = url
        print("Resuming stream from last timepoint of", last_timepoint)

        # If this is the companyprofile stream, load up latest event for each company in memory
        # Saves on querying database, which is slow and allows to keep up with realtime stream
        # This event_store is kept updated with each new event replacing old one, or if new company appending a new key
        if self.model == CompanyProfileStream:
            self.events_store = make_company_event_store(self.session)

        self.stream = Streamer.req_session.get(url + f"?timepoint={last_timepoint+1}", stream=True, auth=requests.auth.HTTPBasicAuth(Streamer.api_key, ""))

    # The event and resource keys are consistent across all streams
    # We can initialize the model with those fields populated
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
        return model

    def compare_models(self, old_event, new_event):
        fields_changed = []
        # print(old_event.company_profile_stream, new_event.company_profile_stream)
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

            # try:
            if old_event[key] != value:
                fields_changed.append(key)
            # except:
            #     print(f"------------------------------{type(old_event)} \n", old_event, "\n------------------------------------------")
            #     break

        return fields_changed


    # Any new or updated data fires a event, what is done with any of the events is up for debate
    # Currently planned is
    def fire_event(self, event_name, company=None, fields_changed:list=[]):
        if fields_changed:
            print(event_name, fields_changed, company.data_company_number)
        else:
            print(event_name, company.data_company_number)

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
                print("Stream error encountered, restarting stream. ", e)
                last_timepoint = self.session.query(self.model.event_timepoint)\
                                .order_by(self.model.event_timepoint.desc()).first()[0]
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

        if not previous_event:
            self.fire_event("companyprofile.new", company=company)

        # If a previous event was found, compare old row with new row and fire off event if any fields are changed
        if previous_event:
            changed_fields = self.compare_models(previous_event, company)
            if changed_fields:
                self.fire_event("companyprofile.update", fields_changed=changed_fields, company=company)

        return company



class GenericTempStreamer(Streamer):

    def pipeline(self, event, model):
        model.company_number = event["resource_uri"].split("/")[2]
        model.data = event["data"]

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




