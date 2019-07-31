# -*- coding: utf-8 -*-
from mongoengine import (
    Document,
    StringField,
    ValidationError,
    EmbeddedDocument,
    EmbeddedDocumentField,
    InvalidQueryError,
    LookUpError,
    IntField,
    GenericEmbeddedDocumentField,
    ListField,
    EmbeddedDocumentListField,
    ReferenceField,
)

from tests.utils import MongoDBTestCase


class TestEmbeddedDocumentField(MongoDBTestCase):

    def test_query_embedded_document_attribute(self):
        class AdminSettings(EmbeddedDocument):
            foo1 = StringField()
            foo2 = StringField()

        class Person(Document):
            settings = EmbeddedDocumentField(AdminSettings)
            name = StringField()

        Person.drop_collection()

        p = Person(settings=AdminSettings(foo1="bar1", foo2="bar2"), name="John").save()

        # Test non exiting attribute
        with self.assertRaises(InvalidQueryError) as ctx_err:
            Person.objects(settings__notexist="bar").first()
        self.assertEqual(unicode(ctx_err.exception), u'Cannot resolve field "notexist"')

        with self.assertRaises(LookUpError):
            Person.objects.only("settings.notexist")

        # Test existing attribute
        self.assertEqual(Person.objects(settings__foo1="bar1").first().id, p.id)
        only_p = Person.objects.only("settings.foo1").first()
        self.assertEqual(only_p.settings.foo1, p.settings.foo1)
        self.assertIsNone(only_p.settings.foo2)
        self.assertIsNone(only_p.name)

        exclude_p = Person.objects.exclude("settings.foo1").first()
        self.assertIsNone(exclude_p.settings.foo1)
        self.assertEqual(exclude_p.settings.foo2, p.settings.foo2)
        self.assertEqual(exclude_p.name, p.name)

    def test_query_embedded_document_attribute_with_inheritance(self):
        class BaseSettings(EmbeddedDocument):
            meta = {"allow_inheritance": True}
            base_foo = StringField()

        class AdminSettings(BaseSettings):
            sub_foo = StringField()

        class Person(Document):
            settings = EmbeddedDocumentField(BaseSettings)

        Person.drop_collection()

        p = Person(settings=AdminSettings(base_foo="basefoo", sub_foo="subfoo"))
        p.save()

        # Test non exiting attribute
        with self.assertRaises(InvalidQueryError) as ctx_err:
            self.assertEqual(Person.objects(settings__notexist="bar").first().id, p.id)
        self.assertEqual(unicode(ctx_err.exception), u'Cannot resolve field "notexist"')

        # Test existing attribute
        self.assertEqual(Person.objects(settings__base_foo="basefoo").first().id, p.id)
        self.assertEqual(Person.objects(settings__sub_foo="subfoo").first().id, p.id)

        only_p = Person.objects.only("settings.base_foo", "settings._cls").first()
        self.assertEqual(only_p.settings.base_foo, "basefoo")
        self.assertIsNone(only_p.settings.sub_foo)