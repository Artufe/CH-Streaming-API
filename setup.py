from models import *
from funcs import date_str_to_datetime
import csv

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
            if comp[f"SicText_{x}"]:
                try:
                    comp[f"SICCode{x}"] = int(comp[f"SicText_{x}"].split(" - ")[0])
                except:
                    pass

            del comp[f"SicText_{x}"]


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

        if comp.get("DissolutionDate"):
            comp["DissolutionDate"] = date_str_to_datetime(comp["DissolutionDate"])

        if comp.get("IncorporationDate"):
            comp["IncorporationDate"] = date_str_to_datetime(comp["IncorporationDate"])

        if comp.get("NextDueDate"):
            comp["NextDueDate"] = date_str_to_datetime(comp["NextDueDate"])

        if comp.get("LastMadeUpDate"):
            comp["LastMadeUpDate"] = date_str_to_datetime(comp["LastMadeUpDate"])

        if comp.get("ConfStmtNextDueDate"):
            comp["ConfStmtNextDueDate"] = date_str_to_datetime(comp["ConfStmtNextDueDate"])

        if comp.get("ConfStmtLastMadeUpDate"):
            comp["ConfStmtLastMadeUpDate"] = date_str_to_datetime(comp["ConfStmtLastMadeUpDate"])

        c = Company(**comp)
        session.add(c)

        co += 1
        if co % 1000 == 0:
            try:
                session.commit()
            except:
                print("Rolled back ")
                session.rollback()



import_snapshot("/home/arthur/BasicCompanyDataAsOneFile-2021-06-01.csv")