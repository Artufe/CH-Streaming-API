from models import Company



def add_new_company(company):
    data = company["data"]
    new_company = Company(CompanyNumber=company.resource_id,
                          CompanyName=data["company_name"])


    if data.get("registered_office_address"):
        if data["registered_office_address"].get("address_line_1"):
            new_company.AddressLine1 = data["registered_office_address"].get("address_line_1")
        if data["registered_office_address"].get("address_line_2"):
            new_company.AddressLine2 = data["registered_office_address"].get("address_line_2")
        if data["registered_office_address"].get("care_of"):
            new_company.Careof = data["registered_office_address"].get("care_of")
        if data["registered_office_address"].get("po_box"):
            new_company.POBox= data["registered_office_address"].get("po_box")

        if data["registered_office_address"].get("locality"):
            new_company.PostTown = data["registered_office_address"].get("locality")
        if data["registered_office_address"].get("country"):
            new_company.Country = data["registered_office_address"].get("country")
        if data["registered_office_address"].get("postal_code"):
            new_company.PostCode = data["registered_office_address"].get("postal_code")

    new_company.CompanyStatus = data.get("company_status")
    if data.get("date_of_cessation"):
        new_company.DissolutionDate = data.get("date_of_cessation")
    if data.get("date_of_creation"):
        new_company.IncorporationDate = data.get("date_of_creation")

    if data.get("accounting_reference_date"):
        ref = data.get("accounting_reference_date")
        new_company.AccountRefDay = ref.get("day")
        new_company.AccountRefMonth = ref.get("month")

    if data.get("accounts"):
        new_company.NextDueDate = data["accounts"]["next_due"]

def update_existing_company(company, existing_company):
    pass