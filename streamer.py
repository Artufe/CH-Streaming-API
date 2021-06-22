import os
import sys
import json
import requests
from datetime import datetime
from pandas import json_normalize
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from multiprocessing import Process, Queue
from models import CompanyProfileStream, Company, ColumnarStream
from funcs import company_event_process

# Base class that contains all the shared methods
class Streamer:
    api_key = os.environ.get("CH_API_KEY")
    req_session = requests.session()
    req_session.headers = {"User-Agent": "Stream Receiver"}

    # DB connection init, class wide
    engine = create_engine('postgresql://postgres:BigBlueBattery123!@localhost:5500/ch')
    Session = sessionmaker(bind=engine)
    session = Session()

    # model: SQLAlchemy model class appropriate to the stream type (url)
    # event_function: the function that maps the raw event JSON to model
    def __init__(self, url, model, event_function):
        self.model = model
        self.event_to_model = event_function
        # TODO add resuming from last timepoint found in DB
        self.stream = Streamer.req_session.get(url, stream=True, auth=requests.auth.HTTPBasicAuth(Streamer.api_key, ""))

    # The event and resource keys are consistent across all streams
    # We can initialize the model with those fields populated
    def populate_model(self, event):
        model = self.model()
        model.resource_id = event["resource_id"]
        model.resource_kind = event["resource_kind"]
        model.resource_uri = event["resource_uri"]

        model.event_fields_changed = event["event"].get("fields_changed", [])
        model.event_published_at = datetime.strptime(event["event"]["published_at"], "%Y-%m-%dT%H:%M:%S")
        model.event_timepoint = event["event"]["timepoint"]
        return model

    def read_from_stream(self):
        for line in self.stream.iter_lines():
            if line:
                event = json.loads(line.decode('utf-8'))
                model_instance = self.populate_model(event.copy())
                model_instance = self.event_to_model(event, model_instance)
                col_model_instance = self.event_to_model(event, model_instance)
                # try:
                Streamer.session.add(model_instance)
                Streamer.session.add(col_model_instance)
                Streamer.session.commit()
                # except:
                #     print()
                #     Streamer.session.rollback()

if __name__ == "__main__":
    # q = Queue()
    stream_to_launch = sys.argv[1]
    if stream_to_launch == "company":
        # Company profile stream
        streamer = Streamer("https://stream.companieshouse.gov.uk/companies", CompanyProfileStream, company_event_process)

    streamer.read_from_stream()




