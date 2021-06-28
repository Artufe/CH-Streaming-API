import enum
import uuid

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import ARRAY, BigInteger, Boolean, Column, Date, DateTime, Integer, JSON, SmallInteger, String, Table, Text
from sqlalchemy.dialects.postgresql import REGCLASS, UUID
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata

engine = create_engine('postgresql://postgres@localhost:5500/ch')
Session = sessionmaker(bind=engine)

# Represents a company as extracted from a snapshot
# http://download.companieshouse.gov.uk/en_output.html#:~:text=The%20Free%20Company%20Data%20Product,files%20for%20ease%20of%20downloading.
class Company(Base):
    __tablename__ = 'companies'
    CompanyNumber=Column(String, primary_key=True)
    CompanyName=Column(String(160))

    Careof=Column(String(100))
    POBox=Column(String(10))
    AddressLine1=Column(String(300))
    AddressLine2=Column(String(300))
    PostTown=Column(String(50))
    County=Column(String(50))
    Country=Column(String(50))
    PostCode=Column(String(20))

    CompanyCategory=Column(String(100))
    CompanyStatus=Column(String(70))
    CountryofOrigin=Column(String(50))
    DissolutionDate=Column(Date)
    IncorporationDate=Column(Date)

    AccountRefDay=Column(SmallInteger)
    AccountRefMonth=Column(SmallInteger)
    NextDueDate=Column(Date)
    LastMadeUpDate=Column(Date)
    AccountCategory=Column(String(30))

    NumMortCharges=Column(Integer)
    NumMortOutstanding=Column(Integer)
    NumMortPartSatisfied=Column(Integer)
    NumMortSatisfied=Column(Integer)

    SICCode1=Column(Integer)
    SICCode2=Column(Integer)
    SICCode3=Column(Integer)
    SICCode4=Column(Integer)

    NumGenPartners=Column(Integer)
    NumLimPartners=Column(Integer)
    URI=Column(String(47))

    PreviousNames = Column(ARRAY(String))

    ConfStmtNextDueDate=Column(Date)
    ConfStmtLastMadeUpDate=Column(Date)

class CompanyProfileStream(Base):
    __tablename__ = 'company_profile_stream'

    company_profile_stream = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    resource_id = Column(Text)
    resource_kind = Column(Text)
    resource_uri = Column(Text)
    data_accounts_accounting_reference_date_day = Column('data.accounts.accounting_reference_date.day', SmallInteger)
    data_accounts_accounting_reference_date_month = Column('data.accounts.accounting_reference_date.month', SmallInteger)
    data_accounts_last_accounts_made_up_to = Column('data.accounts.last_accounts.made_up_to', Date)
    data_accounts_last_accounts_type = Column('data.accounts.last_accounts.type', Text)
    data_accounts_next_due = Column('data.accounts.next_due', Date)
    data_accounts_next_made_up_to = Column('data.accounts.next_made_up_to', Date)
    data_accounts_overdue = Column('data.accounts.overdue', Boolean)
    data_annual_return_last_made_up_to = Column('data.annual_return.last_made_up_to', Date)
    data_annual_return_next_due = Column('data.annual_return.next_due', Date)
    data_annual_return_next_made_up_to = Column('data.annual_return.next_made_up_to', Date)
    data_annual_return_overdue = Column('data.annual_return.overdue', Boolean)
    data_branch_company_details_business_activity = Column('data.branch_company_details.business_activity', Text)
    data_branch_company_details_parent_company_name = Column('data.branch_company_details.parent_company_name', Text)
    data_branch_company_details_parent_company_number = Column('data.branch_company_details.parent_company_number', Text)
    data_can_file = Column('data.can_file', Boolean)
    data_company_name = Column('data.company_name', Text)
    data_company_number = Column('data.company_number', Text)
    data_company_status = Column('data.company_status', Text)
    data_company_status_detail = Column('data.company_status_detail', Text)
    data_confirmation_statement_last_made_up_to = Column('data.confirmation_statement.last_made_up_to', Date)
    data_confirmation_statement_next_due = Column('data.confirmation_statement.next_due', Date)
    data_confirmation_statement_next_made_up_to = Column('data.confirmation_statement.next_made_up_to', Date)
    data_confirmation_statement_overdue = Column('data.confirmation_statement.overdue', Boolean)
    data_date_of_cessation = Column('data.date_of_cessation', Date)
    data_date_of_creation = Column('data.date_of_creation', Date)
    data_etag = Column('data.etag', Text)
    data_foreign_company_details_accounting_requirement_foreign_account_type = Column('data.foreign_company_details.accounting_requirement.foreign_account_type', Text)
    data_foreign_company_details_accounting_requirement_terms_of_account_publication = Column('data.foreign_company_details.accounting_requirement.terms_of_account_publication', Text)
    data_foreign_company_details_accounts_account_period_from_day = Column('data.foreign_company_details.accounts.account_period_from.day', SmallInteger)
    data_foreign_company_details_accounts_account_period_from_month = Column('data.foreign_company_details.accounts.account_period_from.month', SmallInteger)
    data_foreign_company_details_accounts_account_period_to_day = Column('data.foreign_company_details.accounts.account_period_to.day', SmallInteger)
    data_foreign_company_details_accounts_account_period_to_month = Column('data.foreign_company_details.accounts.account_period_to.month', SmallInteger)
    data_foreign_company_details_accounts_must_file_within_months = Column('data.foreign_company_details.accounts.must_file_within.months', SmallInteger)
    data_foreign_company_details_business_activity = Column('data.foreign_company_details.business_activity', Text)
    data_foreign_company_details_company_type = Column('data.foreign_company_details.company_type', Text)
    data_foreign_company_details_governed_by = Column('data.foreign_company_details.governed_by', Text)
    data_foreign_company_details_is_a_credit_finance_institution = Column('data.foreign_company_details.is_a_credit_finance_institution', Boolean)
    data_foreign_company_details_originating_registry_country = Column('data.foreign_company_details.originating_registry.country', Text)
    data_foreign_company_details_originating_registry_name = Column('data.foreign_company_details.originating_registry.name', Text)
    data_foreign_company_details_registration_number = Column('data.foreign_company_details.registration_number', Text)
    data_has_been_liquidated = Column('data.has_been_liquidated', Boolean)
    data_has_charges = Column('data.has_charges', Boolean)
    data_has_insolvency_history = Column('data.has_insolvency_history', Boolean)
    data_is_community_interest_company = Column('data.is_community_interest_company', Boolean)
    data_jurisdiction = Column('data.jurisdiction', Text)
    data_last_full_members_list_date = Column('data.last_full_members_list_date', Date)
    data_links_persons_with_significant_control = Column('data.links.persons_with_significant_control', Text)
    data_links_persons_with_significant_control_statements = Column('data.links.persons_with_significant_control_statements', Text)
    data_links_registers = Column('data.links.registers', Text)
    data_links_self = Column('data.links.self', Text)
    data_previous_company_names = Column('data.previous_company_names', JSON)
    data_registered_office_address_address_line_1 = Column('data.registered_office_address.address_line_1', Text)
    data_registered_office_address_address_line_2 = Column('data.registered_office_address.address_line_2', Text)
    data_registered_office_address_care_of = Column('data.registered_office_address.care_of', Text)
    data_registered_office_address_country = Column('data.registered_office_address.country', Text)
    data_registered_office_address_locality = Column('data.registered_office_address.locality', Text)
    data_registered_office_address_po_box = Column('data.registered_office_address.po_box', Text)
    data_registered_office_address_postal_code = Column('data.registered_office_address.postal_code', Text)
    data_registered_office_address_premises = Column('data.registered_office_address.premises', Text)
    data_registered_office_address_region = Column('data.registered_office_address.region', Text)
    data_registered_office_is_in_dispute = Column('data.registered_office_is_in_dispute', Boolean)
    data_sic_codes = Column('data.sic_codes', ARRAY(Text()))
    data_type = Column('data.type', Text)
    data_undeliverable_registered_office_address = Column('data.undeliverable_registered_office_address', Boolean)
    event_fields_changed = Column('event.fields_changed', ARRAY(Text()))
    event_published_at = Column('event.published_at', DateTime(True))
    event_timepoint = Column('event.timepoint', Integer)
    event_type = Column('event.type', Text)


class CompanyProfileStreamCol(Base):
    __tablename__ = 'company_profile_stream_col'

    company_profile_stream = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    resource_id = Column(Text)
    resource_kind = Column(Text)
    resource_uri = Column(Text)
    data_accounts_accounting_reference_date_day = Column('data.accounts.accounting_reference_date.day', SmallInteger)
    data_accounts_accounting_reference_date_month = Column('data.accounts.accounting_reference_date.month', SmallInteger)
    data_accounts_last_accounts_made_up_to = Column('data.accounts.last_accounts.made_up_to', Date)
    data_accounts_last_accounts_type = Column('data.accounts.last_accounts.type', Text)
    data_accounts_next_due = Column('data.accounts.next_due', Date)
    data_accounts_next_made_up_to = Column('data.accounts.next_made_up_to', Date)
    data_accounts_overdue = Column('data.accounts.overdue', Boolean)
    data_annual_return_last_made_up_to = Column('data.annual_return.last_made_up_to', Date)
    data_annual_return_next_due = Column('data.annual_return.next_due', Date)
    data_annual_return_next_made_up_to = Column('data.annual_return.next_made_up_to', Date)
    data_annual_return_overdue = Column('data.annual_return.overdue', Boolean)
    data_branch_company_details_business_activity = Column('data.branch_company_details.business_activity', Text)
    data_branch_company_details_parent_company_name = Column('data.branch_company_details.parent_company_name', Text)
    data_branch_company_details_parent_company_number = Column('data.branch_company_details.parent_company_number', Text)
    data_can_file = Column('data.can_file', Boolean)
    data_company_name = Column('data.company_name', Text)
    data_company_number = Column('data.company_number', Text)
    data_company_status = Column('data.company_status', Text)
    data_company_status_detail = Column('data.company_status_detail', Text)
    data_confirmation_statement_last_made_up_to = Column('data.confirmation_statement.last_made_up_to', Date)
    data_confirmation_statement_next_due = Column('data.confirmation_statement.next_due', Date)
    data_confirmation_statement_next_made_up_to = Column('data.confirmation_statement.next_made_up_to', Date)
    data_confirmation_statement_overdue = Column('data.confirmation_statement.overdue', Boolean)
    data_date_of_cessation = Column('data.date_of_cessation', Date)
    data_date_of_creation = Column('data.date_of_creation', Date)
    data_etag = Column('data.etag', Text)
    data_foreign_company_details_accounting_requirement_foreign_account_type = Column('data.foreign_company_details.accounting_requirement.foreign_account_type', Text)
    data_foreign_company_details_accounting_requirement_terms_of_account_publication = Column('data.foreign_company_details.accounting_requirement.terms_of_account_publication', Text)
    data_foreign_company_details_accounts_account_period_from_day = Column('data.foreign_company_details.accounts.account_period_from.day', SmallInteger)
    data_foreign_company_details_accounts_account_period_from_month = Column('data.foreign_company_details.accounts.account_period_from.month', SmallInteger)
    data_foreign_company_details_accounts_account_period_to_day = Column('data.foreign_company_details.accounts.account_period_to.day', SmallInteger)
    data_foreign_company_details_accounts_account_period_to_month = Column('data.foreign_company_details.accounts.account_period_to.month', SmallInteger)
    data_foreign_company_details_accounts_must_file_within_months = Column('data.foreign_company_details.accounts.must_file_within.months', SmallInteger)
    data_foreign_company_details_business_activity = Column('data.foreign_company_details.business_activity', Text)
    data_foreign_company_details_company_type = Column('data.foreign_company_details.company_type', Text)
    data_foreign_company_details_governed_by = Column('data.foreign_company_details.governed_by', Text)
    data_foreign_company_details_is_a_credit_finance_institution = Column('data.foreign_company_details.is_a_credit_finance_institution', Boolean)
    data_foreign_company_details_originating_registry_country = Column('data.foreign_company_details.originating_registry.country', Text)
    data_foreign_company_details_originating_registry_name = Column('data.foreign_company_details.originating_registry.name', Text)
    data_foreign_company_details_registration_number = Column('data.foreign_company_details.registration_number', Text)
    data_has_been_liquidated = Column('data.has_been_liquidated', Boolean)
    data_has_charges = Column('data.has_charges', Boolean)
    data_has_insolvency_history = Column('data.has_insolvency_history', Boolean)
    data_is_community_interest_company = Column('data.is_community_interest_company', Boolean)
    data_jurisdiction = Column('data.jurisdiction', Text)
    data_last_full_members_list_date = Column('data.last_full_members_list_date', Date)
    data_links_persons_with_significant_control = Column('data.links.persons_with_significant_control', Text)
    data_links_persons_with_significant_control_statements = Column('data.links.persons_with_significant_control_statements', Text)
    data_links_registers = Column('data.links.registers', Text)
    data_links_self = Column('data.links.self', Text)
    data_previous_company_names = Column('data.previous_company_names', JSON)
    data_registered_office_address_address_line_1 = Column('data.registered_office_address.address_line_1', Text)
    data_registered_office_address_address_line_2 = Column('data.registered_office_address.address_line_2', Text)
    data_registered_office_address_care_of = Column('data.registered_office_address.care_of', Text)
    data_registered_office_address_country = Column('data.registered_office_address.country', Text)
    data_registered_office_address_locality = Column('data.registered_office_address.locality', Text)
    data_registered_office_address_po_box = Column('data.registered_office_address.po_box', Text)
    data_registered_office_address_postal_code = Column('data.registered_office_address.postal_code', Text)
    data_registered_office_address_premises = Column('data.registered_office_address.premises', Text)
    data_registered_office_address_region = Column('data.registered_office_address.region', Text)
    data_registered_office_is_in_dispute = Column('data.registered_office_is_in_dispute', Boolean)
    data_sic_codes = Column('data.sic_codes', ARRAY(Text()))
    data_type = Column('data.type', Text)
    data_undeliverable_registered_office_address = Column('data.undeliverable_registered_office_address', Boolean)
    event_fields_changed = Column('event.fields_changed', ARRAY(Text()))
    event_published_at = Column('event.published_at', DateTime(True))
    event_timepoint = Column('event.timepoint', Integer)
    event_type = Column('event.type', Text)

# class CompanyProfileStream(CompanyProfileStream):
#     __tablename__ = 'company_profile_stream_col'

# class BaseStream(object):
#     id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
#     resource_id = Column(String(67))
#     resource_kind = Column(String(25))
#     resource_uri = Column(String(100))
#     data = Column(JSON)
#     event_timepoint = Column(BigInteger)
#     event_published_at = Column(String(23))
#     event_type = Column(String(15))
#
#
# class CompanyProfileStream(BaseStream, Base):
#     __tablename__ = "company_profile_stream"
#
# class FilingsStream(BaseStream, Base):
#     __tablename__ = "filings_stream"
#
# class InsolvencyStream(BaseStream, Base):
#     __tablename__ = "insolvency_stream"
#
# class ChargesStream(BaseStream, Base):
#     __tablename__ = "charges_stream"
#
# class OfficersStream(BaseStream, Base):
#     __tablename__ = "officers_stream"
#
# class PersonsStream(BaseStream, Base):
#     __tablename__ = "persons_stream"


# class CompanyProfileStream2(BaseStream, Base):
#     __tablename__ = "company_profile_stream_col"
#
# class FilingsStream2(BaseStream, Base):
#     __tablename__ = "filings_stream_col"
#
# class InsolvencyStream2(BaseStream, Base):
#     __tablename__ = "insolvency_stream_col"
#
# class ChargesStream2(BaseStream, Base):
#     __tablename__ = "charges_stream_col"
#
# class OfficersStream2(BaseStream, Base):
#     __tablename__ = "officers_stream_col"
#
# class PersonsStream2(BaseStream, Base):
#     __tablename__ = "persons_stream_col"

# If running this script directly, the intended behaviour is to create all of the tables.
# Safe operation, with existence check for all tables.
if __name__ == "__main__":
    Base.metadata.create_all(engine)