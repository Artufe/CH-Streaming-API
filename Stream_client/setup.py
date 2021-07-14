import time
from collections import deque

from Stream_client.models import *
from Stream_client.funcs import date_str_to_datetime, company_event_process
import csv
from Stream_client.streamer import CompanyStreamer
import json
from aiohttp import ClientSession, TCPConnector, BasicAuth
import asyncio
import pypeln as pl
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pytz
from datetime import datetime

# Function to import a data snapshot from Companies House
# Into the Companies table. Should only have to be done once.
def import_snapshot(snapshot_location):
    session = Session()
    companies = []
    with open(snapshot_location, "r") as f:
        reader = csv.DictReader(f)
        for line in reader:
            companies.append(line)

    print("There are a total of", len(companies), "companies loaded from the snapshot")
    co = 0
    for comp in companies:
        comp["PreviousNames"] = []
        # Standardize the previous names into a single list
        for x in range(1, 11):
            if comp[f"PreviousName_{x}.CompanyName"]:
                comp["PreviousNames"].append("||".join([comp[f"PreviousName_{x}.CompanyName"],
                                                       comp[f"PreviousName_{x}.CONDATE"]]))
            del comp[f"PreviousName_{x}.CompanyName"]
            del comp[f"PreviousName_{x}.CONDATE"]

        # Transform sic code + text to just sic code
        for x in range(1,5):
            if comp[f"SICCode.SicText_{x}"]:
                try:
                    comp[f"SICCode{x}"] = int(comp[f"SICCode.SicText_{x}"].split(" - ")[0])
                except:
                    pass

            del comp[f"SICCode.SicText_{x}"]


        if comp.get("AccountRefDay"):
            comp["AccountRefDay"] = int(comp["AccountRefDay"])
        else:
            comp["AccountRefDay"] = None

        if comp.get("AccountRefMonth"):
            comp["AccountRefMonth"] = int(comp["AccountRefMonth"])
        else:
            comp["AccountRefMonth"] = None

        comp["NumMortCharges"] = int(comp["NumMortCharges"])
        comp["NumMortOutstanding"] = int(comp["NumMortOutstanding"])
        comp["NumMortPartSatisfied"] = int(comp["NumMortPartSatisfied"])
        comp["NumMortSatisfied"] = int(comp["NumMortSatisfied"])

        comp["NumGenPartners"] = int(comp["NumGenPartners"])
        comp["NumLimPartners"] = int(comp["NumLimPartners"])

        # Convert date string to datetime, or None
        for x in ["DissolutionDate", "IncorporationDate", "NextDueDate",
                  "LastMadeUpDate", "ConfStmtNextDueDate", "ConfStmtLastMadeUpDate"]:
            if comp.get(x):
                comp[x] = date_str_to_datetime(comp[x], '%d/%m/%Y')
            else:
                comp[x] = None

        c = Company(**comp)
        session.add(c)
        session.commit()
    session.commit()

engine = create_engine('postgresql://postgres@localhost:5500/ch')
Session = sessionmaker(bind=engine)
session = Session()


comps = [c for c in session.query(Company.CompanyNumber)]
added_comps = set([c for c in session.query(CompanyProfileStreamCol.data_company_number)])
to_do = []
for comp in comps:
    if comp not in added_comps:
        to_do.append(comp)
print("To do companies", len(to_do))

urls = (f"https://api.company-information.service.gov.uk/company/{i.CompanyNumber}" for i in to_do)
c = 0
bad_comps = []
with open("ch_api_keys.txt", "r") as f:
    api_keys = f.readlines()
    api_keys = deque([a.strip() for a in api_keys])

# Get all companyprofile resources, for use as a base reference point
# Long running, one-off function
async def main():
    streamer = CompanyStreamer("https://stream.companieshouse.gov.uk/companies", CompanyProfileStream, CompanyProfileStreamCol, company_event_process)
    tz = pytz.timezone('UTC')
    naive_time = datetime.strptime('2021-05-01 15:00', '%Y-%m-%d %H:%M')
    tz_time = tz.localize(naive_time)
    london_tz = pytz.timezone('Europe/London')
    london_time = tz_time.astimezone(london_tz)

    async with ClientSession(connector=TCPConnector(limit=0)) as session:
        c = 0
        async def fetch(url):
            global c, api_keys
            api_key = api_keys[0]
            api_keys.rotate(1)
            async with session.get(url, auth=BasicAuth(api_key, "", encoding='utf-8'), headers={"User-Agent": "Companies"}) as response:
                resp =  await response.read()
                if response.status != 200:
                    print("Bad response for comp", url.split("/")[-1], api_key)
                    await asyncio.sleep(15)
                    await fetch(url)
                    return True

                resp = json.loads(resp)

                company = CompanyProfileStreamCol(resource_id=resp["company_number"], resource_kind="company-profile",
                                                  resource_uri=f"/company/{resp['company_number']}", event_fields_changed={}, event_published_at=london_time, event_timepoint=None)
                company = streamer.event_to_model(resp, company)
                streamer.col_session.add(company)
                c += 1
                if c % 10000 == 0:
                    streamer.col_session.commit()
                if c % 500 == 0:
                    print(c)

                if int(response.headers["x-ratelimit-remain"]) <= 3:
                    time_to_sleep = int(response.headers["x-ratelimit-reset"]) - int(time.time())
                    print("Sleeping for", time_to_sleep, "seconds")
                    await asyncio.sleep(time_to_sleep+1)

        await pl.task.each(fetch, urls, workers=30)
        streamer.col_session.commit()



asyncio.run(main())


# import_snapshot("/home/arthur/BasicCompanyDataAsOneFile-2021-07-01.csv")