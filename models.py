import enum
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Integer, Column, JSON, String, SmallInteger, BigInteger
from sqlalchemy.dialects.postgresql import ARRAY, UUID
import uuid

engine = create_engine('postgresql://postgres@localhost:5500/ch')
Session = sessionmaker(bind=engine)

Base = declarative_base()

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
    DissolutionDate=Column(String(10))
    IncorporationDate=Column(String(10))

    AccountRefDay=Column(SmallInteger)
    AccountRefMonth=Column(SmallInteger)
    NextDueDate=Column(String(10))
    LastMadeUpDate=Column(String(10))
    AccountCategory=Column(String(30))

    NextDueDate=Column(String(10))
    LastMadeUpDate=Column(String(10))

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

    ConfStmtNextDueDate=Column(String(10))
    ConfStmtLastMadeUpDate=Column(String(10))

class BaseStream(object):
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    resource_id = Column(String(67))
    resource_kind = Column(String(25))
    resource_uri = Column(String(100))
    data = Column(JSON)
    event_timepoint = Column(BigInteger)
    event_published_at = Column(String(23))
    event_type = Column(String(15))


class CompanyProfileStream(BaseStream, Base):
    __tablename__ = "company_profile_stream"

class FilingsStream(BaseStream, Base):
    __tablename__ = "filings_stream"

class InsolvencyStream(BaseStream, Base):
    __tablename__ = "insolvency_stream"

class ChargesStream(BaseStream, Base):
    __tablename__ = "charges_stream"

class OfficersStream(BaseStream, Base):
    __tablename__ = "officers_stream"

class PersonsStream(BaseStream, Base):
    __tablename__ = "persons_stream"




class CompanyProfileStream2(BaseStream, Base):
    __tablename__ = "company_profile_stream_col"

class FilingsStream2(BaseStream, Base):
    __tablename__ = "filings_stream_col"

class InsolvencyStream2(BaseStream, Base):
    __tablename__ = "insolvency_stream_col"

class ChargesStream2(BaseStream, Base):
    __tablename__ = "charges_stream_col"

class OfficersStream2(BaseStream, Base):
    __tablename__ = "officers_stream_col"

class PersonsStream2(BaseStream, Base):
    __tablename__ = "persons_stream_col"

# If running this script directly, the intended behaviour is to create all of the tables.
# Safe operation, with existence check for all tables.
if __name__ == "__main__":
    Base.metadata.create_all(engine)