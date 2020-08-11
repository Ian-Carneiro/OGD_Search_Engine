from sqlalchemy import Column, String, create_engine, select, and_, ForeignKey, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

Base = declarative_base()
engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5433/data_processor_db')


class MetadataResources(Base):
    __tablename__ = 'metadata_resources'
    id = Column(String, primary_key=True)
    url = Column(String)
    name = Column(String)
    description = Column(String)
    package_id = Column(String, ForeignKey('metadata_dataset.id'))
    format = Column(String)
    created = Column(String)
    # package = relationship('MetadataDataset', lazy='joined')


class MetadataDataset(Base):
    __tablename__ = 'metadata_dataset'
    id = Column(String, primary_key=True)
    author = Column(String)
    title = Column(String)
    metadata_created = Column(String)
    metadata_modified = Column(String)
    organization_name = Column(String)
    organization_id = Column(String)
    tags = Column(String)
    notes = Column(String)
    quant_resources = Column(Integer)
    resources: list = relationship('MetadataResources', lazy='joined',
                                   primaryjoin="""and_(MetadataDataset.id == MetadataResources.package_id,
                                            MetadataResources.format.ilike('csv'),
                                            MetadataResources.url.ilike('http%://%'),
                                            MetadataResources.url.notilike('%.zip'))""")


Session = sessionmaker(bind=engine)
session = Session()


def get_resources():
    return session.query(MetadataResources).filter(and_(MetadataResources.format.ilike('csv'),
                                                        MetadataResources.url.ilike('http%://%'),
                                                        MetadataResources.url.notilike('%.zip'))) \
        .order_by(MetadataResources.created).all()


def get_dataset(offset, limit=1):
    # resource_csv = session.query(MetadataResources).filter(MetadataResources.format.ilike('csv')).subquery()
    return session.query(MetadataDataset) \
        .order_by(MetadataDataset.metadata_created).limit(limit).offset(offset).all()
