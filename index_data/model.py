from sqlalchemy import Column, String, Boolean, create_engine, and_, ForeignKey, Integer, update
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, make_transient


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
    spatial_indexing = Column(Boolean)
    temporal_indexing = Column(Boolean)
    thematic_indexing = Column(Boolean)
    excluded = Column(Boolean)
    updated = Column(Boolean)


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
    num_resources = Column(Integer)
    resources: list = relationship('MetadataResources', lazy='joined',
                                   primaryjoin="""and_(MetadataDataset.id == MetadataResources.package_id,
                                            MetadataResources.format.ilike('csv'),
                                            MetadataResources.url.ilike('http%://%'),
                                            MetadataResources.url.notilike('%.zip'))""")


Session = sessionmaker(bind=engine)


def get_resources():
    session = Session()
    resources = session.query(MetadataResources).filter(and_(MetadataResources.format.ilike('csv'),
                                                        MetadataResources.url.ilike('http%://%'),
                                                        MetadataResources.url.notilike('%.zip'))) \
        .order_by(MetadataResources.created).all()
    session.close()
    return resources


def get_dataset(offset, limit=1):
    session = Session()
    dataset = session.query(MetadataDataset) \
        .order_by(MetadataDataset.metadata_created).limit(limit).offset(offset).all()
    session.close()
    return dataset


def set_as_indexed(resource_id, indexing_done):
    stm = update(MetadataResources).where(MetadataResources.id == resource_id).values(**{indexing_done: True})
    engine.execute(stm)


def spatial_indexing_done(resource_id_list: list):
    for id_ in resource_id_list:
        set_as_indexed(id_, "spatial_indexing")


def temporal_indexing_done(resource_id_list: list):
    for id_ in resource_id_list:
        set_as_indexed(id_, "temporal_indexing")


def thematic_indexing_done(resource_id_list: list):
    for id_ in resource_id_list:
        set_as_indexed(id_, "thematic_indexing")


def get_deleted_resources(limit=1):
    session = Session()
    packages = session.query(MetadataDataset)\
        .filter(and_(MetadataResources.format.ilike('csv'),
                     MetadataResources.excluded.is_(True),
                     MetadataResources.package_id.like(MetadataDataset.id))).limit(limit).all()
    for package in packages:
        for resource in package.resources:
            session.delete(resource)
        make_transient(package)
    session.commit()
    session.close()
    return packages
