import os
import sys
import json
import requests
from datetime import datetime
from pandas import json_normalize
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from multiprocessing import Process, Queue
from models import CompanyProfileStream, CompanyProfileStreamCol, Company
from funcs import company_event_process

# Base class that contains all the shared methods
class Streamer:
    api_key = os.environ.get("CH_API_KEY")
    req_session = requests.session()
    req_session.headers = {"User-Agent": "Stream Receiver"}

    # DB connection init, class wide
    engine = create_engine('postgresql://postgres:BigBlueBattery123!@localhost:5500/ch')
    Session = sessionmaker(bind=engine)
    row_session = Session()
    col_session = Session()

    # model: SQLAlchemy model class appropriate to the stream type (url)
    # event_function: the function that maps the raw event JSON to model
    def __init__(self, url, model, col_model, event_function):
        self.model = model
        self.col_model = col_model
        self.event_to_model = event_function
        # TODO add resuming from last timepoint found in DB
        self.stream = Streamer.req_session.get(url, stream=True, auth=requests.auth.HTTPBasicAuth(Streamer.api_key, ""))

    # The event and resource keys are consistent across all streams
    # We can initialize the model with those fields populated
    def populate_model(self, event, model):
        model = model()
        model.resource_id = event["resource_id"]
        model.resource_kind = event["resource_kind"]
        model.resource_uri = event["resource_uri"]

        model.event_fields_changed = event["event"].get("fields_changed", [])
        model.event_published_at = datetime.strptime(event["event"]["published_at"], "%Y-%m-%dT%H:%M:%S")
        model.event_timepoint = event["event"]["timepoint"]
        return model

    # Any new or updated data fires a event, what is done with any of the events is up for debate
    # Currently sends relevant data to targeted.ai
    def fire_event(self, event_name, company:Company=None, fields_changed:list=[]):
        pass

class CompanyStreamer(Streamer):

    def company_exists(self, company_number):
        q = Streamer.row_session.query(Company.CompanyNumber).filter(Company.CompanyNumber == company_number.upper())
        if Streamer.row_session.query(q.exists()).scalar():
            return True
        else:
            return False

    def insert_company(self, company):
        new_company = Company()
        new_company.CompanyName = company.data_company_name
        new_company.CompanyNumber = company.data_company_number

        new_company.Careof = company.data_registered_office_address_care_of
        new_company.POBox = company.data_registered_office_address_po_box
        new_company.AddressLine1 = company.data_registered_office_address_address_line_1
        new_company.AddressLine2 = company.data_registered_office_address_address_line_2
        new_company.PostTown = company.data_registered_office_address_locality
        new_company.County = company.data_registered_office_address_region
        new_company.Country = company.data_registered_office_address_country
        new_company.PostCode = company.data_registered_office_address_postal_code

        if company.data_type:
            new_company.CompanyCategory = company.data_type.replace("-", " ").title()
        else:
            new_company.CompanyCategory = company.data_type

        new_company.CompanyStatus = company.data_company_status
        new_company.CountryofOrigin = company.data_registered_office_address_country
        new_company.DissolutionDate = company.data_date_of_cessation
        new_company.IncorporationDate = company.data_date_of_creation

        new_company.AccountRefDay = company.data_accounts_accounting_reference_date_day
        new_company.AccountRefMonth = company.data_accounts_accounting_reference_date_month
        new_company.NextDueDate = company.data_accounts_next_due
        new_company.LastMadeUpDate = company.data_accounts_last_accounts_made_up_to
        new_company.AccountCategory = company.data_accounts_last_accounts_type

        try:
            new_company.SICCode1 = company.data_sic_codes.pop()
        except IndexError:
            new_company.SICCode1 = None

        try:
            new_company.SICCode2 = company.data_sic_codes.pop()
        except IndexError:
            new_company.SICCode2 = None

        try:
            new_company.SICCode3 = company.data_sic_codes.pop()
        except IndexError:
            new_company.SICCode3 = None

        try:
            new_company.SICCode4 = company.data_sic_codes.pop()
            new_company.SICCode4 = company.data_sic_codes.pop()
        except IndexError:
            new_company.SICCode4 = None

        # Dont bother assigning previous names attrib, as its a new company and will always be empty

        new_company.ConfStmtNextDueDate = company.data_confirmation_statement_next_due
        new_company.ConfStmtLastMadeUpDate = company.data_confirmation_statement_last_made_up_to
        Streamer.row_session.add(new_company)
        Streamer.row_session.commit()

    def update_company(self, company):
        return True


    def read_from_stream(self):
        counter = 0
        for line in self.stream.iter_lines():
            if line:
                event = json.loads(line.decode('utf-8'))
                # Normal companyprofile model instance
                company = self.populate_model(event.copy(), self.model)
                company = self.event_to_model(event.copy(), company)

                # Columnar companyprofile instance model instance
                company_col = self.populate_model(event.copy(), self.col_model)
                company_col = self.event_to_model(event.copy(), company_col)

                Streamer.row_session.add(company)
                Streamer.col_session.add(company_col)
                Streamer.row_session.commit()
                counter += 1
                if counter % 10000 == 0:
                    Streamer.col_session.commit()
                    Streamer.row_session.execute('''TRUNCATE TABLE public.company_profile_stream CONTINUE IDENTITY RESTRICT;''')

                # Company has been saved, now do the postprocessing

                # Check if company exists in the Company table
                if self.company_exists(company.data_company_number):
                    # Company does exist, update it
                    print("Existing company, updating", company.data_company_number)
                    fields_changed = self.update_company(company)
                    self.fire_event("companyprofile.update", fields_changed=fields_changed, company=company)
                else:
                    # Company does not yet exist in our companies table, add it
                    print("New company, inserting to companies table", company.data_company_number)
                    self.insert_company(company)
                    self.fire_event("companyprofile.new", company=company)





if __name__ == "__main__":
    # q = Queue()
    stream_to_launch = sys.argv[1]
    if stream_to_launch == "company":
        # Company profile stream
        streamer = CompanyStreamer("https://stream.companieshouse.gov.uk/companies?timepoint=29818343", CompanyProfileStream, CompanyProfileStreamCol, company_event_process)

    streamer.read_from_stream()




