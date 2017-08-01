from sqlalchemy import Table, Column, ForeignKey
from sqlalchemy import Integer, String, Boolean
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
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
    gtihub_id = Column('github_id', String, unique=True)
    name = Column('name', String)
    description = Column('description', String)
    owner_id = Column('owner_id', Integer, ForeignKey('users.id'))
    disk_usage = Column('disk_usage', Integer)
    url = Column('url', String)
    is_fork = Column('is_fork', Boolean)
    is_mirror = Column('is_mirror', Boolean)

class User(Base):
    __tablename__ = 'users'

    id = Column('id', Integer, primary_key=True)
    login = Column('login', String, unique=True)
    name = Column('name', String)

class UsersTodo(Base):
    __tablename__ = 'users_todo'

    id = Column('id', Integer, primary_key=True)
    user_id = Column('user_id', Integer, ForeignKey('users.id'), unique=True)
    user = relationship('User', uselist=False)

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

associations = {
    'repo_languages':
    Table('repo_languages', Base.metadata,
          Column('repo_id', Integer, ForeignKey('users.id')),
          Column('lang_id', Integer, ForeignKey('repositories.id')),
          Column('rank', Integer)),

    'contributed':
    Table('contributed', Base.metadata,
          Column('user_id', Integer, ForeignKey('users.id')),
          Column('repo_id', Integer, ForeignKey('repositories.id'))),

    'submitted_issue_pullrequest':
    Table('submitted_issue_pullrequest', Base.metadata,
          Column('user_id', Integer, ForeignKey('users.id')),
          Column('repo_id', Integer, ForeignKey('repositories.id'))),

    'starred':
    Table('starred', Base.metadata,
          Column('user_id', Integer, ForeignKey('users.id')),
          Column('repo_id', Integer, ForeignKey('repositories.id'))),

    'watching':
    Table('watching', Base.metadata,
          Column('user_id', Integer, ForeignKey('users.id')),
          Column('repo_id', Integer, ForeignKey('repositories.id')))}

Base.metadata.create_all(engine)
