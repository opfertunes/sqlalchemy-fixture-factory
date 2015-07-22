# -*- coding: utf-8 -*-

"""

"""

from __future__ import absolute_import
from sqlalchemy import Table, Column, Integer, ForeignKey, Unicode, create_engine
import sqlalchemy
#from sqlalchemy.ext.declarative import declarative_base
#from sqlalchemy.orm import sessionmaker  relationship, 
from sqlalchemy_fixture_factory.sqla_fix_fact import SqlaFixFact
from sqlalchemy import MetaData, mapper, relation, create_session

class BaseSAObject(object):
    def __init__(self, *args, **kwargs):
        for k in kwargs:
            self.__setattr__(k, kwargs[k])

class TestCase(object):
    Base = None

    # tables
    Role = None
    Account = None
    Person = None
    Country = None

    engine = None
    connection = None
    db_session = None
    fix_fact = None

    def setup(self):
        self.Base = BaseSAObject #declarative_base()

        self.engine = create_engine('sqlite:///')
        self.metadata = MetaData()
        self.metadata.bind = self.engine

        # self.engine.echo = True
        self.create_models()

        #sqlalchemy.orm.configure_mappers()
        self.connection = self.engine.connect()

        self.create_tables()

        #SessionPool = sessionmaker(bind=self.engine)
        #self.db_session = SessionPool()
        self.db_session = create_session()

        # initialize SQLAlchemy Fixture Factory with the DB session
        self.fix_fact = SqlaFixFact(self.db_session)


    def create_tables(self):
        self.metadata.create_all(self.connection)

    def drop_tables(self):
        self.metadata.drop_all(self.connection)

    def teardown(self):
        #self.db_session.close_all()
        #self.db_session.expunge_all()
        self.drop_tables()
        self.engine.dispose()
        self.connection.close()

    def create_models(self):

        # association table, only required once
        account_role = Table('account_role', self.metadata,
                             Column('id_account', Integer, ForeignKey('account.id')),
                             Column('id_role', Integer, ForeignKey('role.id')))

        role_table = Table('role', self.metadata,
                            Column('id', Integer, primary_key=True),
                            Column('name', Unicode))

        account_table = Table('account', self.metadata,
                            Column('id', Integer, primary_key=True),
                            Column('name', Unicode))

        country_table = Table('country', self.metadata,
                            Column('id', Integer, primary_key=True),
                            Column('code', Unicode))

        person_table = Table('person', self.metadata,
                            Column('id', Integer, primary_key=True),
                            Column('first_name', Unicode),
                            Column('account_id', Integer, ForeignKey('account.id')),
                            Column('country_id', Integer, ForeignKey('country.id'))
                            )
        class Role(self.Base):
            pass

        class Account(self.Base):
            pass

            #__tablename__ = 'account'

            #id = Column(Integer, primary_key=True)
            #name = Column('name', Unicode)

            #roles = relationship(Role, secondary=account_role)

        class Person(self.Base):
            pass

            #__tablename__ = 'person'

            #id = Column(Integer, primary_key=True)
            #first_name = Column('first_name', Unicode)
            #account_id = Column(Integer, ForeignKey('account.id'))
            #account = relationship(Account)
        
        class Country(self.Base):
            pass
        
        mapper(Country, country_table)

        mapper(Role, role_table) 

        mapper(Account, account_table, properties={
            "roles": relation(Role, secondary=account_role),
        })

        mapper(Person, person_table, properties={
            "account": relation(Account),
            "country": relation(Country),
        })

        self.Role = Role
        self.Account = Account
        self.Person = Person
        self.Country = Country
