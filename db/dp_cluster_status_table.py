from sqlalchemy import Column, Integer, String, ForeignKey, create_engine
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

class ClusterStatus(Base):
    __tablename__ = 'dp_cluster_status_table'

    primary_id = Column(Integer, primary_key=True)
    name = Column(String)
    state = Column(String)
    ip = Column(String(255))
    user_name = Column(Integer)
    password = Column(String(255))
