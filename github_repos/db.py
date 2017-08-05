from sqlalchemy import Table, Column, ForeignKey
from sqlalchemy import Integer, SmallInteger, String, Boolean
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship, sessionmaker

import github_repos.config as g

def get_engine(url=g.db_url):
    #return psycopg2.connect(dbname=DBNAME, host=HOST, port=PORT)
    return create_engine(url, client_encoding='utf8')

engine = get_engine()
Session = sessionmaker(engine)

Base = declarative_base()

class Repo(Base):
    __tablename__ = 'repositories'

    id = Column('id', Integer, primary_key=True)
    # github_id = Column('github_id', String, unique=True)
    name = Column('name', String, index=True)
    description = Column('description', String)
    owner_id = Column('owner_id', Integer, ForeignKey('owners.id'), index=True)
    disk_usage = Column('disk_usage', Integer)
    url = Column('url', String)
    is_fork = Column('is_fork', Boolean)
    is_mirror = Column('is_mirror', Boolean)

    owner = relationship('Owner', uselist=False)
    languages = relationship('Language')

class OwnerType(Base):
    __tablename__ = 'owner_types'

    id = Column('id', SmallInteger, primary_key=True)
    typename = Column('type', String, unique=True)

    def __init__(self, typename):
        self.typename = typename

class Owner(Base):
    __tablename__ = 'owners'

    id = Column('id', Integer, primary_key=True)
    login = Column('login', String, index=True, unique=True)
    type_id = Column('type_id', SmallInteger, ForeignKey('owner_types.id'))

    owner_type = relationship('OwnerType', uselist=False)
    owner_typename = association_proxy('owner_type', 'typename')

class NewRepo(Base):
    __tablename__ = 'new_repos'

    id = Column('id', Integer, primary_key=True)
    owner_id = Column('owner_id', Integer, ForeignKey('owners.id'))
    name = Column('name', String)

    owner = relationship('Owner', uselist=False)

class ReposTodo(Base):
    __tablename__ = 'repos_todo'

    id = Column('id', Integer, primary_key=True)
    repo_id = Column('repo_id', Integer, ForeignKey('repositories.id'), unique=True)
    repo = relationship('Repo', uselist=False)

class Language(Base):
    __tablename__ = 'languages'

    id = Column('id', Integer, primary_key=True)
    name = Column('name', String)
    color = Column('color', String)

class QueryCost(Base):
    __tablename__ = 'query_costs'

    id = Column('id', Integer, primary_key=True)
    guess = Column('guess', Integer)
    normalized_actual = Column('normalized_actual', Integer)

repo_languages = Table('repo_languages', Base.metadata,
                       Column('repo_id', Integer, ForeignKey('repositories.id'), index=True),
                       Column('lang_id', Integer, ForeignKey('languages.id'), index=True),
                       Column('rank', Integer))

Base.metadata.create_all(engine)
