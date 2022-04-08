import pickle
import pytz
from datetime import datetime
from unittest.mock import patch

from django.test import TestCase

from kafka_consumer.consumer import BaseForeignMessage
from kafka_consumer.messages import MessageBatchProcessor
from kafka_consumer.models import JunkMessage
from kafka_consumer.tests.helpers import (
    FaddySubscriber,
    ClumsySubscriber,
    TestableBaseSubscriber,
)
from kafka_consumer.utils import get_class_path


EVENT_NAME_1 = 1
EVENT_NAME_2 = 2


class DummyMessage(BaseForeignMessage):
    def __init__(self, message):
        self.__message = message

    @property
    def offset(self):
        return self.__message['offset']

    @property
    def raw_data(self):
        return self.__message['value']


class DummyMessageBatchIter(object):
    def __init__(self, batch_iter):
        self.__batch_iter = batch_iter

    def __iter__(self):
        return self

    def next(self):
        return DummyMessage(next(self.__batch_iter))

    def __next__(self):
        return self.next()


class DummyMessageBatch(object):
    def __init__(self, batch):
        self.__batch = batch

    def __iter__(self):
        return DummyMessageBatchIter(iter(self.__batch))


class TestBatchProcessor(TestCase):
    @classmethod
    def get_message1(cls):
        return {
            'value': pickle.dumps({
                'name': EVENT_NAME_1,
                'time': datetime(2017, 3, 20, 0, 0, 0, tzinfo=pytz.UTC),
                'uid': '1234',
                'data': {
                    'key1': 'value',
                    'key2': 'value',
                },
            }),
            'offset': 234,
        }

    @classmethod
    def get_message2(cls):
        return {
            'value': pickle.dumps({
                'name': EVENT_NAME_2,
                'time': datetime(2017, 3, 20, 0, 0, 1, tzinfo=pytz.UTC),
                'uid': '1235',
                'data': {
                    'key1': 'value',
                    'key2': 'value',
                },
            }),
            'offset': 345,
        }
        return fake_message

    @classmethod
    def get_message3(cls):
        return {
            'value': pickle.dumps({
                'name': EVENT_NAME_1,
                'time': datetime(2017, 3, 20, 0, 0, 2, tzinfo=pytz.UTC),
                'uid': '1236',
                'data': {
                    'key1': 'value',
                    'key2': 'value',
                },
            }),
            'offset': 456,
        }
        return fake_message

    @classmethod
    def get_message4(cls):
        return {
            'value': pickle.dumps({
                'name': EVENT_NAME_1,
                'time': datetime(2017, 3, 20, 0, 0, 2, tzinfo=pytz.UTC),
                'uid': '1236',
                'data': {
                    'key1': 'value',
                    'key2': 'value',
                },
            }),
            'offset': 457,
        }

    @patch('kafka_consumer.tests.helpers.TestableBaseSubscriber._handle')
    @patch('kafka_consumer.tests.helpers.FaddySubscriber._handle')
    def test_process_message_batch(self, faddy_handle, test_handle):
        topics = {
            'test_topic': {
                'topic': 'test_topic',
                'group': 'sok_bpo_core_group',
                'subscribers': (
                    get_class_path(TestableBaseSubscriber),
                    get_class_path(FaddySubscriber),
                ),
                'message_processor': 'plain_message_processor',
            },
        }
        topic_message_processors = {
            'plain_message_processor': {
                'class': 'kafka_consumer.tests.helpers.PickleMessageProcessor',
            },
        }
        with patch('kafka_consumer.settings.KAFKA_CONSUMER_TOPICS', topics), \
             patch('kafka_consumer.settings.KAFKA_CONSUMERS_MESSAGE_PROCESSORS', topic_message_processors):
            message_batch_processor = MessageBatchProcessor('test_topic')

            messages = [self.get_message1(), self.get_message2(), self.get_message3(), self.get_message4()]
            message_batch_processor.process_message_batch(DummyMessageBatch(messages))

        self.assertEqual(test_handle.call_count, 4)
        faddy_handle.assert_not_called()

    def test_process_message_batch_with_decode_error(self):
        topics = {
            'test_topic': {
                'topic': 'test_topic',
                'group': 'sok_bpo_core_group',
                'subscribers': (
                    get_class_path(TestableBaseSubscriber),
                    get_class_path(FaddySubscriber),
                ),
                'message_processor': 'plain_message_processor',
            },
        }
        topic_message_processors = {
            'plain_message_processor': {
                'class': 'kafka_consumer.tests.helpers.ClumsyMessageProcessor',
            },
        }
        with patch('kafka_consumer.settings.KAFKA_CONSUMER_TOPICS', topics), \
             patch('kafka_consumer.settings.KAFKA_CONSUMERS_MESSAGE_PROCESSORS', topic_message_processors):
            message_batch_processor = MessageBatchProcessor('test_topic')

            messages = [self.get_message1()]
            message_batch_processor.process_message_batch(DummyMessageBatch(messages))

        self.assertSetEqual(
            set([jm.subscriber for jm in JunkMessage.objects.all()]),
            set([get_class_path(TestableBaseSubscriber), get_class_path(FaddySubscriber)])
        )

    def test_process_message_batch_with_loser_subscriber(self):
        topics = {
            'unlucky_topic': {
                'topic': 'unlucky_topic',
                'group': 'sok_bpo_core_group',
                'subscribers': (
                    get_class_path(TestableBaseSubscriber),
                    get_class_path(FaddySubscriber),
                    get_class_path(ClumsySubscriber),
                ),
                'message_processor': 'plain_message_processor',
            },
        }
        topic_message_processors = {
            'plain_message_processor': {
                'class': 'kafka_consumer.tests.helpers.PickleMessageProcessor',
            },
        }
        with patch('kafka_consumer.settings.KAFKA_CONSUMER_TOPICS', topics), \
             patch('kafka_consumer.settings.KAFKA_CONSUMERS_MESSAGE_PROCESSORS', topic_message_processors):
            message_batch_processor = MessageBatchProcessor('unlucky_topic')
            message = self.get_message1()
            messages = [message]
            message_batch_processor.process_message_batch(DummyMessageBatch(messages))

        self.assertSetEqual(
            set(list(JunkMessage.objects.all().values_list('subscriber', flat=True))),
            set([get_class_path(ClumsySubscriber)])
        )

        jm = JunkMessage.objects.first()
        self.assertEqual(jm.subscriber, get_class_path(ClumsySubscriber))
        self.assertEqual(jm.raw_data, message['value'])
        self.assertEqual(jm.error_message, 'Ups, I did it again')
        self.assertEqual(jm.topic_key, 'unlucky_topic')
        self.assertEqual(jm.offset, 234)
