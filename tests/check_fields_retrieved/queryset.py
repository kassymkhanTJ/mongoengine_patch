# -*- coding: utf-8 -*-

import datetime
import unittest
import uuid
from decimal import Decimal

from bson import DBRef, ObjectId
import pymongo
from pymongo.read_preferences import ReadPreference
from pymongo.results import UpdateResult
import six
from six import iteritems

from mongoengine import *
from mongoengine.connection import get_connection, get_db
from mongoengine.context_managers import query_counter, switch_db
from mongoengine.errors import InvalidQueryError
from mongoengine.mongodb_support import MONGODB_36, get_mongodb_version
from mongoengine.queryset import (
    DoesNotExist,
    MultipleObjectsReturned,
    QuerySet,
    QuerySetManager,
    queryset_manager,
)

Document._meta['check_fields_retrieved'] = True

class db_ops_tracker(query_counter):
    def get_ops(self):
        ignore_query = dict(self._ignored_query)
        ignore_query["command.count"] = {
            "$ne": "system.profile"
        }  # Ignore the query issued by query_counter
        return list(self.db.system.profile.find(ignore_query))


def get_key_compat(mongo_ver):
    ORDER_BY_KEY = "sort"
    CMD_QUERY_KEY = "command" if mongo_ver >= MONGODB_36 else "query"
    return ORDER_BY_KEY, CMD_QUERY_KEY


class QuerySetTest(unittest.TestCase):
    def setUp(self):
        connect(db="mongoenginetest")
        connect(db="mongoenginetest2", alias="test2")

        class PersonMeta(EmbeddedDocument):
            weight = IntField()

        class Person(Document):
            name = StringField()
            age = IntField()
            person_meta = EmbeddedDocumentField(PersonMeta)
            meta = {"allow_inheritance": True}

        Person.drop_collection()
        self.PersonMeta = PersonMeta
        self.Person = Person

        self.mongodb_version = get_mongodb_version()

    def test_limit(self):
        """WILL FAIL. Ensure that QuerySet.limit works as expected."""
        user_a = self.Person.objects.create(name="User A", age=20)
        user_b = self.Person.objects.create(name="User B", age=30)

        # Test limit on a new queryset
        people = list(self.Person.objects.limit(1))
        self.assertEqual(len(people), 1)
        self.assertEqual(people[0], user_a)

        # Test limit on an existing queryset
        people = self.Person.objects
        self.assertEqual(len(people), 2)
        people2 = people.limit(1)
        self.assertEqual(len(people), 2)
        self.assertEqual(len(people2), 1)
        self.assertEqual(people2[0], user_a)

        # Test limit with 0 as parameter
        people = self.Person.objects.limit(0)
        self.assertEqual(people.count(with_limit_and_skip=True), 2)
        self.assertEqual(len(people), 2)

        # Test chaining of only after limit
        person = self.Person.objects().limit(1).only("name").first()
        self.assertEqual(person, user_a)
        self.assertEqual(person.name, "User A")
        self.assertEqual(person.age, None)

    def test_skip(self):
        """WILL FAIL. Ensure that QuerySet.skip works as expected."""
        user_a = self.Person.objects.create(name="User A", age=20)
        user_b = self.Person.objects.create(name="User B", age=30)

        # Test skip on a new queryset
        people = list(self.Person.objects.skip(1))
        self.assertEqual(len(people), 1)
        self.assertEqual(people[0], user_b)

        # Test skip on an existing queryset
        people = self.Person.objects
        self.assertEqual(len(people), 2)
        people2 = people.skip(1)
        self.assertEqual(len(people), 2)
        self.assertEqual(len(people2), 1)
        self.assertEqual(people2[0], user_b)

        # Test chaining of only after skip
        person = self.Person.objects().skip(1).only("name").first()
        self.assertEqual(person, user_b)
        self.assertEqual(person.name, "User B")
        self.assertEqual(person.age, None)

        self.assertEqual(result, 1)
        result = self.Person.objects.update_one(
            set__name="Test User", write_concern={"w": 0}
        )
        self.assertEqual(result, None)
    
    def test_read_preference(self):
        """WILL FAIL"""
        class Bar(Document):
            txt = StringField()

            meta = {"indexes": ["txt"]}

        Bar.drop_collection()
        bar = Bar.objects.create(txt="xyz")

        bars = list(Bar.objects.read_preference(ReadPreference.PRIMARY))
        self.assertEqual(bars, [bar])

        bars = Bar.objects.read_preference(ReadPreference.SECONDARY_PREFERRED)
        self.assertEqual(bars._read_preference, ReadPreference.SECONDARY_PREFERRED)
        self.assertEqual(
            bars._cursor._Cursor__read_preference, ReadPreference.SECONDARY_PREFERRED
        )

        # Make sure that `.read_preference(...)` does accept string values.
        self.assertRaises(TypeError, Bar.objects.read_preference, "Primary")

        # Make sure read preference is respected after a `.skip(...)`.
        bars = Bar.objects.skip(1).read_preference(ReadPreference.SECONDARY_PREFERRED)
        self.assertEqual(bars._read_preference, ReadPreference.SECONDARY_PREFERRED)
        self.assertEqual(
            bars._cursor._Cursor__read_preference, ReadPreference.SECONDARY_PREFERRED
        )

        # Make sure read preference is respected after a `.limit(...)`.
        bars = Bar.objects.limit(1).read_preference(ReadPreference.SECONDARY_PREFERRED)
        self.assertEqual(bars._read_preference, ReadPreference.SECONDARY_PREFERRED)
        self.assertEqual(
            bars._cursor._Cursor__read_preference, ReadPreference.SECONDARY_PREFERRED
        )

        # Make sure read preference is respected after an `.order_by(...)`.
        bars = Bar.objects.order_by("txt").read_preference(
            ReadPreference.SECONDARY_PREFERRED
        )
        self.assertEqual(bars._read_preference, ReadPreference.SECONDARY_PREFERRED)
        self.assertEqual(
            bars._cursor._Cursor__read_preference, ReadPreference.SECONDARY_PREFERRED
        )

        # Make sure read preference is respected after a `.hint(...)`.
        bars = Bar.objects.hint([("txt", 1)]).read_preference(
            ReadPreference.SECONDARY_PREFERRED
        )
        self.assertEqual(bars._read_preference, ReadPreference.SECONDARY_PREFERRED)
        self.assertEqual(
            bars._cursor._Cursor__read_preference, ReadPreference.SECONDARY_PREFERRED
        )

    def test_order_by_chaining(self):
        """WILL FAIL. Ensure that an order_by query chains properly and allows .only()
        """
        self.Person(name="User B", age=40).save()
        self.Person(name="User A", age=20).save()
        self.Person(name="User C", age=30).save()

        only_age = self.Person.objects.order_by("-age").only("age")

        names = [p.name for p in only_age]
        ages = [p.age for p in only_age]

        # The .only('age') clause should mean that all names are None
        self.assertEqual(names, [None, None, None])
        self.assertEqual(ages, [40, 30, 20])

        qs = self.Person.objects.all().order_by("-age")
        qs = qs.limit(10)
        ages = [p.age for p in qs]
        self.assertEqual(ages, [40, 30, 20])

        qs = self.Person.objects.all().limit(10)
        qs = qs.order_by("-age")

        ages = [p.age for p in qs]
        self.assertEqual(ages, [40, 30, 20])

        qs = self.Person.objects.all().skip(0)
        qs = qs.order_by("-age")
        ages = [p.age for p in qs]
        self.assertEqual(ages, [40, 30, 20])

    def test_embedded_only(self):
        import random

        class Person(EmbeddedDocument):
            name = StringField()
            age = DecimalField()

        class Class(EmbeddedDocument):
            number = DecimalField()
            literal = StringField()

        class School(EmbeddedDocument):
            name = StringField()
            classes = ListField(EmbeddedDocumentField(Class))
            director = EmbeddedDocumentField(Person)

        class City(Document):
            schools = ListField(EmbeddedDocumentField(School))
            name = StringField()

        City.drop_collection()

        literals = 'ABCDEF'

        schools = [
            {
                'name': 'School number {}'.format(i), 
                'director': Person(name='Director {}'.format(i)),
                'classes': [
                    {
                        'number': j,
                        'literal': random.choice(literals),
                    } for j in range(random.randint(1, 12))
                ]
            } for i in range(10)
        ]

        City(schools=schools, name='Moscow').save()

        city = City.objects.only('schools.classes.number', 'schools.director.name').first()

if __name__ == "__main__":
    unittest.main()
