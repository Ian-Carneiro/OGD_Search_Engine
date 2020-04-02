from sqlalchemy import Column, String, create_engine, select, and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()
engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5433/data_processor_db')


class MetadataResources(Base):
    __tablename__ = 'metadata_resources'
    id = Column(String, primary_key=True)
    url = Column(String)
    name = Column(String)
    description = Column(String)
    package_id = Column(String)
    format = Column(String)
    created = Column(String)

    def __repr__(self):
        return "<Resource(id={}, url={}, name={}, description={}, package_id={})>" \
            .format(self.id, self.url, self.name, self.description, self.package_id)


Session = sessionmaker(bind=engine)


def get_resources():
    session = Session()
    return session.query(MetadataResources).filter(and_(MetadataResources.format.ilike('csv'),
                                                        MetadataResources.url.ilike('http%://%'),
                                                        MetadataResources.url.notilike('%.zip'))) \
        .order_by(MetadataResources.created)
