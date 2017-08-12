from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, SmallInteger, BigInteger, String, Boolean
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy import func, desc, asc, exists

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
    name = Column('name', String, index=True, nullable=False)
    description = Column('description', String)
    owner_id = Column('owner_id', Integer, ForeignKey('owners.id'), index=True, nullable=False)
    disk_usage = Column('disk_usage', Integer)
    url = Column('url', String)
    is_fork = Column('is_fork', Boolean)
    is_mirror = Column('is_mirror', Boolean)

    owner = relationship('Owner', uselist=False)
    languages = relationship('RepoLanguages', back_populates='repo')

class OwnerType(Base):
    __tablename__ = 'owner_types'

    id = Column('id', SmallInteger, primary_key=True)
    typename = Column('type', String, unique=True)

    def __init__(self, typename):
        self.typename = typename

class Owner(Base):
    __tablename__ = 'owners'

    id = Column('id', Integer, primary_key=True)
    login = Column('login', String, index=True, unique=True, nullable=False)
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

class RepoError(Base):
    __tablename__ = 'repo_errors'

    id = Column('id', Integer, primary_key=True)
    repo_id = Column('repo_id', Integer, ForeignKey('repositories.id'), unique=True)
    error_text = Column('error_text', String, nullable=True)

    repo = relationship('Repo', uselist=False)

class Language(Base):
    __tablename__ = 'languages'

    id = Column('id', Integer, primary_key=True)
    name = Column('name', String, unique=True)
    color = Column('color', String)

    repos = relationship('RepoLanguages', back_populates='language')

class QueryCost(Base):
    __tablename__ = 'query_costs'

    id = Column('id', Integer, primary_key=True)
    guess = Column('guess', Integer)
    normalized_actual = Column('normalized_actual', Integer)

class RepoLanguages(Base):
    __tablename__ = 'repo_languages'

    id = Column('id', BigInteger, primary_key=True)
    repo_id = Column('repo_id', Integer, ForeignKey('repositories.id'), index=True)
    lang_id = Column('lang_id', Integer, ForeignKey('languages.id'), index=True)
    bytes_used = Column('bytes_used', BigInteger)

    repo = relationship('Repo', back_populates='languages')
    language = relationship('Language', back_populates='repos')

Base.metadata.create_all(engine)


def get_popular_languages(limit=None, headers=False, reverse=False):
    s = Session()

    order = asc if reverse else desc

    repo_count = s.query(Repo).count()
    table = s.query(Language.name, func.count(RepoLanguages.repo_id)) \
             .join(RepoLanguages) \
             .group_by(RepoLanguages.lang_id, Language.name) \
             .order_by(order(func.count(RepoLanguages.repo_id))) \
             .limit(limit) \
             .all()

    table = tuple(row + ('{:3f}%'.format(row[1] / repo_count * 100),) for row in table)

    if headers:
        table = (('name', '"top 10 langs" count', '% share of repos'),) + table

    return table

def get_average_repos_per_owner():
    s = Session()
    return s.query(Repo).count() / s.query(Owner).count()

def get_expanded_repo_count():
    return Session().query(Repo) \
                    .filter(~exists().where(Repo.id == RepoError.repo_id)) \
                    .filter(~exists().where(Repo.id == ReposTodo.repo_id)) \
                    .count()
