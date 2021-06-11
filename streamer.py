import sys

import requests
import json
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from multiprocessing import Process, Queue
from models import CompanyProfileStream, FilingsStream, InsolvencyStream, ChargesStream, PersonsStream, OfficersStream, Company, CompanyProfileStream2, FilingsStream2, InsolvencyStream2, ChargesStream2, PersonsStream2, OfficersStream2
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.exc import MultipleResultsFound
from funcs import add_new_company, update_existing_company

# Base class that contains all the shared methods
class Streamer:
    api_key = os.environ.get("CH_API_KEY")
    req_session = requests.session()
    req_session.headers = {"User-Agent": "Stream Receiver"}

    # DB
    engine = create_engine('postgresql://postgres:BigBlueBattery123!@localhost:5500/ch')
    Session = sessionmaker(bind=engine)
    session = Session()
    source_to_model_map = {"CompanyProfileStream": CompanyProfileStream, "FilingsStream": FilingsStream,
                           "InsolvencyStream": InsolvencyStream, "ChargesStream": ChargesStream,
                           "OfficersStream": OfficersStream, "PersonsStream": PersonsStream}

    source_to_model_map2 = {"CompanyProfileStream": CompanyProfileStream2, "FilingsStream": FilingsStream2,
                           "InsolvencyStream": InsolvencyStream2, "ChargesStream": ChargesStream2,
                           "OfficersStream": OfficersStream2, "PersonsStream": PersonsStream2}

    def __init__(self, url, table_name):
        self.url = url
        self.table_name = table_name
        self.stream = self.init_stream(self.url)


    def init_stream(self, url):
        stream = Streamer.req_session.get(url, stream=True, auth=requests.auth.HTTPBasicAuth(Streamer.api_key, ""))
        return stream

    def process_line(self, line):
        decoded_line = line.decode('utf-8')
        parsed_line = json.loads(decoded_line)
        parsed_line["event_timepoint"] = parsed_line["event"]["timepoint"]
        parsed_line["event_published_at"] = parsed_line["event"]["published_at"]
        parsed_line["event_type"] = parsed_line["event"]["type"]
        del parsed_line["event"]
        return parsed_line

    def read_from_stream(self):
        print("Starting Stream for", self.table_name)
        for line in self.stream.iter_lines():
            source = self.table_name
            if line:
                line = self.process_line(line)
                print(self.table_name, "got resource id", line["resource_id"])
                model = Streamer.source_to_model_map[source]
                model2 = Streamer.source_to_model_map2[source]
                model_instance = model(**line)
                model_instance2 = model2(**line)
                # try:
                Streamer.session.add(model_instance)
                Streamer.session.add(model_instance2)
                Streamer.session.commit()
                # except:
                #     print()
                #     Streamer.session.rollback()

if __name__ == "__main__":
    # q = Queue()
    stream_to_launch = sys.argv[1]
    if stream_to_launch == "company":
        # Company stream
        streamer = Streamer("https://stream.companieshouse.gov.uk/companies", "CompanyProfileStream")
    elif stream_to_launch == "filings":
        # Filing history stream
        streamer = Streamer("https://stream.companieshouse.gov.uk/filings", "FilingsStream")
    elif stream_to_launch == 'insolvency':
        # Insolvency stream
        streamer = Streamer("https://stream.companieshouse.gov.uk/insolvency-cases", "InsolvencyStream")
    elif stream_to_launch == 'charges':
        # Charges Stream
        streamer = Streamer("https://stream.companieshouse.gov.uk/charges", "ChargesStream")
    elif stream_to_launch == 'officer':
        # Offices Stream
        streamer = Streamer("https://stream.companieshouse.gov.uk/officers", "OfficersStream")
    elif stream_to_launch == 'persons':
        # Persons with significant control stream
        streamer = Streamer("https://stream.companieshouse.gov.uk/persons-with-significant-control", "PersonsStream")

    # A list to keep all of the processes alive
    streamer.read_from_stream()

    # source_to_model_map = {"CompanyProfileStream": CompanyProfileStream, "FilingsStream": FilingsStream,
    #                        "InsolvencyStream": InsolvencyStream, "ChargesStream": ChargesStream,
    #                        "OfficersStream": OfficersStream, "PersonsStream": PersonsStream}
    #
    # while True:
    #     source, result = q.get()
    #     model = source_to_model_map[source]
    #     model_instance = model(**result)
    #     if model == CompanyProfileStream:
    #         # Different logic for companies, as they are more important and intricate than other objects.
    #         # Main difference being the resource_id is company name, and can be repeating on new data.
    #         # Other resources have unique resource_ids associated with only that resource
    #         # try:
    #         #     corresponding_company = Streamer.session.query(Company).filter(
    #         #         Company.CompanyNumber==model_instance.resource_id).one()
    #         # except NoResultFound:
    #         #     # Deal with a new company
    #         #     add_new_company(model_instance)
    #         # else:
    #         #     # Deal with a existing company
    #         #     update_existing_company(model_instance, corresponding_company)
    #     try:
    #         Streamer.session.add(model_instance)
    #         Streamer.session.commit()
    #     except:
    #         Streamer.session.rollback()
    #         # print("Unique violation, Resource ID", model_instance.resource_id, "already exists")




