import uuid

from models import Company
from datetime import datetime

def date_str_to_datetime(date_str):
    if date_str:
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        return date_obj
    else:
        return None


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


def company_event_process(event, model):
    data = event["data"]
    if data.get("accounts"):
        accounts = data["accounts"]
        if accounts.get("accounting_reference_date"):
            model.data_accounts_accounting_reference_date_day = accounts["accounting_reference_date"].get("day")
            model.data_accounts_accounting_reference_date_month = accounts["accounting_reference_date"].get("month")

        if accounts.get("last_accounts"):
            model.data_accounts_last_accounts_made_up_to= date_str_to_datetime(accounts["last_accounts"].get("made_up_to"))
            model.data_accounts_last_accounts_type = accounts["last_accounts"].get("type")

        model.data_accounts_next_due = date_str_to_datetime(accounts.get("next_due"))
        model.data_accounts_next_made_up_to = date_str_to_datetime(accounts.get("next_made_up_to "))
        model.data_accounts_overdue = accounts.get("overdue")

    if data.get("annual_return"):
        annual_return = data["annual_return"]
        model.data_annual_return_last_made_up_to = date_str_to_datetime(annual_return.get("last_made_up_to"))
        model.data_annual_return_next_due = date_str_to_datetime(annual_return.get("next_due"))
        model.data_annual_return_next_made_up_to = date_str_to_datetime(annual_return.get("next_made_up_to"))
        model.data_annual_return_overdue = annual_return.get("overdue")

    if data.get("branch_company_details"):
        bcd = data["branch_company_details"]
        model.data_branch_company_details_business_activity = bcd.get("business_activity")
        model.data_branch_company_details_parent_company_name = bcd.get("parent_company_name")
        model.data_branch_company_details_parent_company_number = bcd.get("parent_company_number")

    model.data_can_file = data.get("can_file")
    model.data_company_name = data.get("company_name")
    model.data_company_number = data.get("company_number")
    model.data_company_status = data.get("company_status")
    model.data_company_status_detail = data.get("company_status_detail")

    if data.get("confirmation_statement"):
        conf = data["confirmation_statement"]
        model.data_confirmation_statement_last_made_up_to = date_str_to_datetime(conf.get("last_made_up_to"))
        model.data_confirmation_statement_next_due = date_str_to_datetime(conf.get("next_due"))
        model.data_confirmation_statement_next_made_up_to = date_str_to_datetime(conf.get("next_made_up_to"))
        model.data_confirmation_statement_overdue = conf.get("overdue")

    model.data_date_of_cessation = date_str_to_datetime(data.get("date_of_cessation"))
    model.data_date_of_creation = date_str_to_datetime(data.get("date_of_creation"))
    model.data_etag = data.get("etag")

    if data.get("foreign_company_details"):
        fcd = data["foreign_company_details"]
        if fcd.get("accounting_requirement"):
            acc_r = fcd["accounting_requirement"]
            model.data_foreign_company_details_accounting_requirement_foreign_account_type = acc_r.get("foreign_account_type")
            model.data_foreign_company_details_accounting_requirement_terms_of_account_publication = acc_r.get("terms_of_account_publication")
        if fcd.get("accounts"):
            faccounts = fcd.get("accounts")
            if faccounts.get("account_period_from"):
                faccounts_pf = faccounts["account_period_from"]
                model.data_foreign_company_details_accounts_account_period_from_day = faccounts_pf.get("day")
                model.data_foreign_company_details_accounts_account_period_from_month = faccounts_pf.get("month")

            if faccounts.get("account_period_to"):
                faccounts_pt = faccounts["account_period_to"]
                model.data_foreign_company_details_accounts_account_period_to_day = faccounts_pt.get("day")
                model.data_foreign_company_details_accounts_account_period_to_month = faccounts_pt.get("month")

            if faccounts.get("accounts_must_file_within"):
                model.data_foreign_company_details_accounts_must_file_within_months = faccounts["accounts_must_file_within"].get("months")

        model.data_foreign_company_details_business_activity = fcd.get("business_activity")
        model.data_foreign_company_details_company_type = fcd.get("company_type")
        model.data_foreign_company_details_governed_by = fcd.get("governed_by")
        model.data_foreign_company_details_is_a_credit_finance_institution = fcd.get("is_a_credit_finance_institution")
        if fcd.get("originating_registry"):
            orig_reg = fcd["originating_registry"]
            model.data_foreign_company_details_originating_registry_country = orig_reg.get("country")
            model.data_foreign_company_details_originating_registry_name = orig_reg.get("name")

        model.data_foreign_company_details_registration_number = fcd.get("registration_number")

    model.data_has_been_liquidated = data.get("has_been_liquidated")
    model.data_has_charges = data.get("has_charges")
    model.data_has_insolvency_history = data.get("has_insolvency_history")
    model.data_is_community_interest_company = data.get("is_community_interest_company")
    model.data_jurisdiction = data.get("jurisdiction")
    model.data_last_full_members_list_date = date_str_to_datetime(data.get("last_full_members_list_date"))
    if data.get("links"):
        links = data["links"]
        model.data_links_persons_with_significant_control = links.get("persons_with_significant_control")
        model.data_links_persons_with_significant_control_statements = links.get("persons_with_significant_control_statements")
        model.data_links_registers = links.get("registers")
        model.data_links_self = links.get("self")

    model.data_previous_company_names = data.get("previous_company_names", {})
    if data.get("registered_office_address"):
        addr = data["registered_office_address"]
        model.data_registered_office_address_address_line_1 = addr.get("address_line_1")
        model.data_registered_office_address_address_line_2 = addr.get("address_line_2")
        model.data_registered_office_address_care_of = addr.get("care_of")
        model.data_registered_office_address_country = addr.get("country")
        model.data_registered_office_address_locality = addr.get("locality")
        model.data_registered_office_address_po_box = addr.get("po_box")
        model.data_registered_office_address_postal_code = addr.get("postal_code")
        model.data_registered_office_address_premises = addr.get("premises")
        model.data_registered_office_address_region = addr.get("region")

    model.data_registered_office_is_in_dispute = data.get("registered_office_is_in_dispute")
    model.data_sic_codes = data.get("sic_codes", [])
    model.data_type = data.get("type")
    model.data_undeliverable_registered_office_address = data.get("undeliverable_registered_office_address")

    return model