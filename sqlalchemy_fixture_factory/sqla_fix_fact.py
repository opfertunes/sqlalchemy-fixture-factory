# -*- coding: utf-8 -*-
# Copyright (c) 2015, Michael Pickelbauer
# License: MIT (see LICENSE for details)

"""
Fixture Factory for SQLAlchemy
"""
#from sqlalchemy import inspect
#from sqlalchemy.ext import hybrid
from sqlalchemy.orm import class_mapper
from pprint import pprint

METHOD_MODEL = 'model'
METHOD_CREATE = 'create'
METHOD_GET = 'get'

class SqlaFixFact():
    """
    Fixture factory manager
    """
    db_session = None
    instances = None

    def __init__(self, db_session):
        assert db_session, 'Passed in DB session is None!'
        self.db_session = db_session
        self.instances = {}

    def get_db_session(self):
        return self.db_session

    def merge(self, instance, Fixture, kwargs):
        assert self.db_session, 'DB session not initialized yet'
        
        try:
            self.db_session.expunge(instance)
        except:
            pass
                
        inst = self.db_session.merge(instance)
        
        self.db_session.flush()

        self.instances[(Fixture.__name__, str(kwargs))] = inst
    
        return inst

    def get(self, Fixture, **kwargs):
        
        inst = self.instances.get((Fixture.__name__, str(kwargs)))

        if not inst:
            inst = Fixture(self, **kwargs).model()

        return self.merge(inst, Fixture, kwargs)

####
# sub factory things
######
def subFactoryGet(fixture, **kwargs):
    """
    To be used in fixture definition (or in the kwargs of the fixture constructor) to reference a other
    fixture using the :meth:`.BaseFix.get` method.

    :param fixture: Desired fixture
    :param kwargs: *Optional:* key words to overwrite properties of this fixture
    :return: Proxy object for the desired fixture including the altered properties
    """
    return SubFactory(fixture, METHOD_GET, **kwargs)

def subFactoryCreate(fixture, **kwargs):
    """
    To be used in fixture definition (or in the kwargs of the fixture constructor) to reference a other
    fixture using the :meth:`.BaseFix.create` method.

    :param fixture: Desired fixture
    :param kwargs: *Optional:* key words to overwrite properties of this fixture
    :return: Proxy object for the desired fixture including the altered properties
    """
    return SubFactory(fixture, METHOD_CREATE, **kwargs)

def subFactoryModel(fixture, **kwargs):
    """
    To be used in fixture definition (or in the kwargs of the fixture constructor) to reference a other
    fixture using the :meth:`.BaseFix.model` method.

    :param fixture: Desired fixture
    :param kwargs: *Optional:* key words to overwrite properties of this fixture
    :return: Proxy object for the desired fixture including the altered properties
    """
    return SubFactory(fixture, METHOD_MODEL, **kwargs)


class SubFactory():
    fixture = None
    kwargs = None

    def __init__(self, fixture, method, **kwargs):
        self.fixture = fixture
        self.method = method
        self.kwargs = kwargs


class BaseFix(object):
    """
    Base class for each fixture
    """
    MODEL = None
    _fix_fact = None
    _kwargs = None

    def __init__(self, fix_fact, **kwargs):
        """

        :param fix_fact: instance of :class:`.SqlaFixFact`
        :param kwargs: *Optional:* key words to overwrite properties of this fixture
        """
        if not self.MODEL:
            raise AttributeError('self.MODEL is not defined')

        self._fix_fact = fix_fact
        self._kwargs = kwargs
        
        for propname, rel in class_mapper(self.MODEL).properties.iteritems():
        #for rel in self.MODEL._sa_class_manager.mapper.relationships:
            attr = getattr(self, propname, None)
            if attr:
                list_type_error = False
                try:
                    if False in [isinstance(a, SubFactory) for a in attr]:
                        raise AttributeError('References in fixtures must be declared with "SubFactory": ' + propname)
                except TypeError, e:
                    # ok, attr is not iterable, maybe its directly a SubFactory
                    if not isinstance(attr, SubFactory):
                        raise AttributeError('References in fixtures must be declared with "SubFactory": ' + propname)

    def model(self):
        """
        Returns a model instance of this fixture which is ready to be added. The model itself is not added to the DB
        but all dependencies are.

        :return: SQLAlchemy Model instance
        """
        # INFOs
        # attr + relations:
        # [f.key for f in Group._sa_class_manager.attributes]
        #
        # Attributes
        # [(a.key, getattr(self, a.key)) for a in self.MODEL._sa_class_manager.mapper.column_attrs]
        #
        # relations
        # [a.key for a in Group._sa_class_manager.mapper.relationships]

        attributes = self.getAttributes()
        #print "attributes (after) %s => %s" % (self.MODEL, attributes)
        
        attributes["_sa_session"] = None  #so mapper doesn't add this instance to the session!
        #if hasattr(self.MODEL(), 'update'):
        #    model = self.MODEL()
        #    model.update(**attributes)
        #else:
        model = self.MODEL(**attributes)

        return model

    def create(self):
        """
        Adds this model to the session. This instance is not registered and thus can never be
        referred to via get

        :return: SQLAlchemy Model instance
        """

        model = self.model()
        self._fix_fact.get_db_session().save(model) #.add()
        self._fix_fact.get_db_session().flush()
        self._fix_fact.get_db_session().expunge(model)

        # in order to have the right values for all fields updated directly by the DB, we have to load the model again
        
        #print "pk for model %s is %s  (%s)" % (self.MODEL, class_mapper(self.MODEL).primary_key, class_mapper(self.MODEL).primary_key.__class__.__name__)
        pk = class_mapper(self.MODEL).primary_key
        if len(pk) == 1:
            pk0 = class_mapper(self.MODEL).primary_key[0]
            id_attr = pk0.key or pk0.name
            id = getattr(model, id_attr)
            return self._fix_fact.get_db_session().query(self.MODEL).get(id)
        else:
            pkFilter = []
            for eachPk in pk:
                id_attr = eachPk.key or eachPk.name
                #print "%s pk attr %s => %s" % (self.MODEL, id_attr, getattr(model, id_attr)) 
                pkFilter.append(getattr(model, id_attr))
                
            result = self._fix_fact.get_db_session().query(self.MODEL).get(pkFilter)     
            #print "seek %s with pk %s => %s" % (self.MODEL, pkFilter, result)
            return result                   

    def get(self):
        """
        returns an already existing model instance or creates one, registers it to be able to
        find it later and then returns the instance

        :return: SQLAlchemy Model instance
        """
        return self._fix_fact.get(self.__class__, **self._kwargs)

    def getAttributes(self):
        def getAttr(key):
            if key in self._kwargs:
                return self._kwargs[key]
            else:
                return getattr(self, key, None)
 
        #attrs = dict([(a.key, getAttr(a.key)) for a in self.MODEL._sa_class_manager.mapper.attrs if (getAttr(a.key) is not None)])
        attrs = dict([(a.key, getAttr(a.key)) for a in class_mapper(self.MODEL).iterate_properties if (getAttr(a.key) is not None)])
        #print "%s => attrs(1) %s" % (self.MODEL, attrs)
        # add hybrids
        #for a in inspect(self.MODEL).all_orm_descriptors.keys():
        #    if (getAttr(a) is not None) and a != '__mapper__':
        #        attrs[a] = getAttr(a)

        def resolveSubFactory(attr):
            if isinstance(attr, SubFactory):
                #print "call %s on SubFactory: %s " % (attr.method, attr)
                if attr.method == METHOD_GET:
                    return attr.fixture(self._fix_fact, **attr.kwargs).get()
                elif attr.method == METHOD_CREATE:
                    return attr.fixture(self._fix_fact, **attr.kwargs).create()
                elif attr.method == METHOD_MODEL:
                    return attr.fixture(self._fix_fact, **attr.kwargs).model()
            #else:
            #    print "attr is no SubFactory: %s" % attr
            return None
           
        #need to iterate all properties as SQ 0.3 is not returning backref properties from class_mapper(self.MODEL).properties.iteritems()   
        for propname, rel in attrs.iteritems(): 
            
            #print "propname: %s  rel: %s" % (propname, rel)

            attr = attrs.get(propname, None) 
            if isinstance(attr, basestring):
                continue

            try:
                converted = []
                for a in attr:
                    #print "list like: %s" % a
                    a = resolveSubFactory(a)
                    if a:
                        converted.append(a)

            except TypeError, e:
                # seems not be a list, try it as an attribute
                #print "%s seems not be a list, try it as an attribute" % attr
                converted = resolveSubFactory(attr)
                #print "%s converted = %s" % (attr, converted)

            if converted:
                attrs[propname] = converted   #rel.key

        # also add hybrid properties
        #for hyb in inspect(self.MODEL).all_orm_descriptors.keys():
        #    if inspect(self.MODEL).all_orm_descriptors[hyb].extension_type is hybrid.HYBRID_PROPERTY:
        #        attr = attrs.get(hyb, None)
        #
        #        try:
        #            converted = []
        #            for a in attr:
        #                a = resolveSubFactory(a)
        #                if a:
        #                    converted.append(a)
        #                #                converted = [a for resolveSubFactory(a) in attr if a]
        #
        #        except TypeError, e:
        #            # seems not be a list, try it as an attribute
        #            converted = resolveSubFactory(attr)
        #
        #        if converted:
        #            attrs[hyb] = converted

        return attrs
