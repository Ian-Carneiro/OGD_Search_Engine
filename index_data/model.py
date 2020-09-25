from sqlalchemy import Column, String, Boolean, create_engine, and_, ForeignKey, Integer, update
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, make_transient
from config import config

Base = declarative_base()
engine = create_engine(config.db_connection)


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
    temporal_indexing = Column(Boolean)
    thematic_indexing = Column(Boolean)
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


def get_dataset(offset, limit=config.num_dataset_per_iteration):
    session = Session()
    dataset = session.query(MetadataDataset) \
        .order_by(MetadataDataset.metadata_created).limit(limit).offset(offset).all()
    session.close()
    return dataset


def set_resource_as_indexed(resource_id, indexing_done):
    stm = update(MetadataResources).where(MetadataResources.id == resource_id).values(**{indexing_done: True})
    engine.execute(stm)


def set_dataset_as_indexed(dataset_id, indexing_done):
    stm = update(MetadataDataset).where(MetadataDataset.id == dataset_id).values(**{indexing_done: True})
    engine.execute(stm)


def spatial_indexing_done(resource_id_list: list):
    for id_ in resource_id_list:
        set_resource_as_indexed(id_, "spatial_indexing")


def temporal_indexing_done(resource_id_list: list, dataset_id=None):
    for id_ in resource_id_list:
        set_resource_as_indexed(id_, "temporal_indexing")
    if dataset_id:
        set_dataset_as_indexed(dataset_id, "temporal_indexing")


def thematic_indexing_done(resource_id_list: list, dataset_id=None):
    for id_ in resource_id_list:
        set_resource_as_indexed(id_, "thematic_indexing")
    if dataset_id:
        set_dataset_as_indexed(dataset_id, "thematic_indexing")


def get_deleted_resources(limit=10):
    session = Session()
    results = session.query(MetadataDataset.num_resources, MetadataResources)\
        .filter(MetadataResources.excluded, MetadataDataset.id == MetadataResources.package_id)\
        .limit(limit).all()
    for result in results:
        session.delete(result[1])
    session.commit()
    session.close()
    return results
