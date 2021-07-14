import copy
from models import Company, CompanyProfileStream, CompanyProfileStreamCol
from datetime import datetime

def date_str_to_datetime(date_str, date_format='%Y-%m-%d', as_date=True):
    if date_str:
        datetime_obj = datetime.strptime(date_str, date_format)
        if as_date:
            return datetime_obj.date()
        return datetime_obj
    else:
        return None

def company_event_process(event_dict, model):
    event = copy.deepcopy(event_dict)
    data = event.get("data")
    if data.get("accounts"):
        accounts = data["accounts"]
        if accounts.get("accounting_reference_date"):
            model.data_accounts_accounting_reference_date_day = accounts["accounting_reference_date"].pop("day", None)
            model.data_accounts_accounting_reference_date_month = accounts["accounting_reference_date"].pop("month", None)

        if accounts.get("last_accounts"):
            model.data_accounts_last_accounts_made_up_to= date_str_to_datetime(accounts["last_accounts"].pop("made_up_to", None))
            model.data_accounts_last_accounts_type = accounts["last_accounts"].pop("type", None)

        model.data_accounts_next_due = date_str_to_datetime(accounts.pop("next_due", None))
        model.data_accounts_next_made_up_to = date_str_to_datetime(accounts.pop("next_made_up_to", None))
        model.data_accounts_overdue = accounts.pop("overdue", False)
    else:
        model.data_accounts_overdue = False

    if data.get("annual_return"):
        annual_return = data["annual_return"]
        model.data_annual_return_last_made_up_to = date_str_to_datetime(annual_return.pop("last_made_up_to", None))
        model.data_annual_return_next_due = date_str_to_datetime(annual_return.pop("next_due", None))
        model.data_annual_return_next_made_up_to = date_str_to_datetime(annual_return.pop("next_made_up_to", None))
        model.data_annual_return_overdue = annual_return.pop("overdue", None)

    if data.get("branch_company_details"):
        bcd = data["branch_company_details"]
        model.data_branch_company_details_business_activity = bcd.pop("business_activity", None)
        model.data_branch_company_details_parent_company_name = bcd.pop("parent_company_name", None)
        model.data_branch_company_details_parent_company_number = bcd.pop("parent_company_number", None)

    model.data_can_file = data.pop("can_file", None)
    model.data_company_name = data.pop("company_name", None)
    model.data_company_number = data.pop("company_number", None)
    model.data_company_status = data.pop("company_status", None)
    model.data_company_status_detail = data.pop("company_status_detail", None)

    if data.get("confirmation_statement"):
        conf = data["confirmation_statement"]
        model.data_confirmation_statement_last_made_up_to = date_str_to_datetime(conf.pop("last_made_up_to", None))
        model.data_confirmation_statement_next_due = date_str_to_datetime(conf.pop("next_due", None))
        model.data_confirmation_statement_next_made_up_to = date_str_to_datetime(conf.pop("next_made_up_to", None))
        model.data_confirmation_statement_overdue = conf.pop("overdue", False)
    else:
        model.data_confirmation_statement_overdue = False

    model.data_date_of_cessation = date_str_to_datetime(data.pop("date_of_cessation", None))
    model.data_date_of_creation = date_str_to_datetime(data.pop("date_of_creation", None))
    model.data_etag = data.pop("etag", None)

    if data.get("foreign_company_details"):
        fcd = data["foreign_company_details"]
        if fcd.get("accounting_requirement"):
            acc_r = fcd["accounting_requirement"]
            model.data_foreign_company_details_accounting_requirement_foreign_account_type = acc_r.pop("foreign_account_type", None)
            model.data_foreign_company_details_accounting_requirement_terms_of_account_publication = acc_r.pop("terms_of_account_publication", None)
        if fcd.get("accounts"):
            faccounts = fcd.get("accounts")
            if faccounts.get("account_period_from"):
                faccounts_pf = faccounts["account_period_from"]
                model.data_foreign_company_details_accounts_account_period_from_day = faccounts_pf.pop("day", None)
                model.data_foreign_company_details_accounts_account_period_from_month = faccounts_pf.pop("month", None)

            if faccounts.get("account_period_to"):
                faccounts_pt = faccounts["account_period_to"]
                model.data_foreign_company_details_accounts_account_period_to_day = faccounts_pt.pop("day", None)
                model.data_foreign_company_details_accounts_account_period_to_month = faccounts_pt.pop("month", None)

            if faccounts.get("accounts_must_file_within"):
                model.data_foreign_company_details_accounts_must_file_within_months = faccounts["accounts_must_file_within"].pop("months", None)

        model.data_foreign_company_details_business_activity = fcd.pop("business_activity", None)
        model.data_foreign_company_details_company_type = fcd.pop("company_type", None)
        model.data_foreign_company_details_governed_by = fcd.pop("governed_by", None)
        model.data_foreign_company_details_is_a_credit_finance_institution = fcd.pop("is_a_credit_finance_institution", None)
        if fcd.get("originating_registry"):
            orig_reg = fcd["originating_registry"]
            model.data_foreign_company_details_originating_registry_country = orig_reg.pop("country", None)
            model.data_foreign_company_details_originating_registry_name = orig_reg.pop("name", None)

        model.data_foreign_company_details_registration_number = fcd.pop("registration_number", None)

    model.data_has_been_liquidated = data.pop("has_been_liquidated", False)
    model.data_has_charges = data.pop("has_charges", False)
    model.data_has_insolvency_history = data.pop("has_insolvency_history", False)
    model.data_is_community_interest_company = data.pop("is_community_interest_company", False)
    model.data_jurisdiction = data.pop("jurisdiction", None)
    model.data_last_full_members_list_date = date_str_to_datetime(data.pop("last_full_members_list_date", None))
    if data.get("links"):
        links = data["links"]
        model.data_links_persons_with_significant_control = links.pop("persons_with_significant_control", None)
        model.data_links_persons_with_significant_control_statements = links.pop("persons_with_significant_control_statements", None)
        model.data_links_registers = links.pop("registers", None)
        model.data_links_self = links.pop("self", None)

    model.data_previous_company_names = data.pop("previous_company_names", {})
    if data.get("registered_office_address"):
        addr = data["registered_office_address"]
        model.data_registered_office_address_address_line_1 = addr.pop("address_line_1", None)
        model.data_registered_office_address_address_line_2 = addr.pop("address_line_2", None)
        model.data_registered_office_address_care_of = addr.pop("care_of", None)
        model.data_registered_office_address_country = addr.pop("country", None)
        model.data_registered_office_address_locality = addr.pop("locality", None)
        model.data_registered_office_address_po_box = addr.pop("po_box", None)
        model.data_registered_office_address_postal_code = addr.pop("postal_code", None)
        model.data_registered_office_address_premises = addr.pop("premises", None)
        model.data_registered_office_address_region = addr.pop("region", None)

    model.data_registered_office_is_in_dispute = data.pop("registered_office_is_in_dispute", False)
    model.data_sic_codes = data.pop("sic_codes", [])
    model.data_type = data.pop("type", None)
    model.data_undeliverable_registered_office_address = data.pop("undeliverable_registered_office_address", False)

    # Can print data here, to make sure that all attributes are grabbed. Any non popped attribute will show up
    # Currently not grabbed attributes:
    # accounts.last_accounts.period_end_on, accounts.last_accounts.period_start_on
    # accounts.next_accounts.due_on, accounts.next_accounts.period_end_on, accounts.next_accounts.period_start_on
    # print("\n---------------------", data, "\n---------------------")

    return model

def make_company_event_store(session):
    event_store = {}

    all_normal_rows = session.query(CompanyProfileStream).distinct(CompanyProfileStream.data_company_number) \
        .order_by(CompanyProfileStream.data_company_number, CompanyProfileStream.event_published_at.desc()).all()

    all_columnar_rows = session.query(CompanyProfileStreamCol).distinct(CompanyProfileStreamCol.data_company_number) \
        .order_by(CompanyProfileStreamCol.data_company_number, CompanyProfileStreamCol.event_published_at.desc()).all()

    # Since columnar stores older data, populate dict with these values first
    for row in all_columnar_rows:
        row_dict = copy.deepcopy(row.__dict__)
        del row_dict["_sa_instance_state"]
        event_store[row.data_company_number] = row_dict

    for row in all_normal_rows:
        row_dict = copy.deepcopy(row.__dict__)
        del row_dict["_sa_instance_state"]
        event_store[row.data_company_number] = row_dict

    print("There are", len(event_store), "rows loaded in event store. Memory usage in Mb:", getsizeof(event_store)/1000000)

    return event_store