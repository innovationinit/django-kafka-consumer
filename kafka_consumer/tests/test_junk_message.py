from datetime import (
    datetime,
    timedelta,
    timezone,
)
from unittest.mock import patch
import os
import pickle
import pytz

from django.core.management import call_command
from django.test import TestCase

from kafka_consumer.messages import MessageBatchProcessor
from kafka_consumer.models import JunkMessage
from kafka_consumer.tests.helpers import (
    ClumsySubscriber,
    TestableBaseSubscriber,
)
from kafka_consumer.utils import get_class_path


EVENT_NAME_1 = 1
EVENT_NAME_2 = 2


class TestBatchProcessor(TestCase):
    @classmethod
    def get_message1(cls):
        fake_message = {
            'value': {
                'name': EVENT_NAME_1,
                'time': datetime(2017, 3, 20, 0, 0, 0, tzinfo=pytz.UTC),
                'uid': '1234',
                'data': {
                    'key1': 'value',
                    'key2': 'value',
                },
            },
            'offset': 234,
        }
        return fake_message

    @patch('kafka_consumer.tests.helpers.TestableBaseSubscriber._handle')
    def test_retry_process_message_with_success(self, test_handle):
        message = self.get_message1()
        jm = JunkMessage.objects.create(
            subscriber=get_class_path(TestableBaseSubscriber),
            raw_data=pickle.dumps(message['value']),
            error_message='Ups',
            offset=1,
            topic_key='unlucky_topic',
        )

        jm.refresh_from_db()

        topics = {
            'unlucky_topic': {
                'topic': 'unlucky_topic',
                'group': 'sok_bpo_core_group',
                'subscribers': (
                    get_class_path(TestableBaseSubscriber),
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
            message_batch_processor.process_message(jm, True)

        test_handle.assert_called_once()
        self.assertFalse(JunkMessage.objects.filter(pk=jm.pk).exists())

    @patch('kafka_consumer.tests.helpers.ClumsySubscriber._handle')
    def test_retry_process_message_with_fail_again(self, loser_handle):
        message = self.get_message1()
        jm = JunkMessage.objects.create(
            subscriber=get_class_path(ClumsySubscriber),
            raw_data=pickle.dumps(message['value']),
            error_message='Ups',
            offset=1,
            topic_key='unlucky_topic',
        )

        jm.refresh_from_db()

        topics = {
            'unlucky_topic': {
                'topic': 'unlucky_topic',
                'group': 'sok_bpo_core_group',
                'subscribers': (
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
            message_batch_processor.process_message(jm, True)

        self.assertTrue(JunkMessage.objects.filter(pk=jm.pk).exists())
        loser_handle.assert_not_called()

    @patch('kafka_consumer.tests.helpers.TestableBaseSubscriber._handle')
    def test_retry_manage_command(self, test_handle):
        message = self.get_message1()
        jm = JunkMessage.objects.create(
            subscriber=get_class_path(TestableBaseSubscriber),
            raw_data=pickle.dumps(message['value']),
            error_message='Ups',
            offset=1,
            topic_key='unlucky_topic',
        )

        jm.refresh_from_db()

        topics = {
            'unlucky_topic': {
                'topic': 'unlucky_topic',
                'group': 'sok_bpo_core_group',
                'subscribers': (
                    get_class_path(TestableBaseSubscriber),
                ),
                'message_processor': 'plain_message_processor',
            },
        }
        topic_message_processors = {
            'plain_message_processor': {
                'class': 'kafka_consumer.tests.helpers.PickleMessageProcessor',
            },
        }

        with open(os.devnull, 'w') as devnull:
            with patch('kafka_consumer.settings.KAFKA_CONSUMER_TOPICS', topics), \
                 patch('kafka_consumer.settings.KAFKA_CONSUMERS_MESSAGE_PROCESSORS', topic_message_processors):
                call_command('consume_junk_messages', stdout=devnull)

        test_handle.assert_called_once()
        self.assertFalse(JunkMessage.objects.filter(pk=jm.pk).exists())


class ConsumeJunkMessagesCommandTestCase(TestCase):

    def test_prepare_messages_filters(self):
        tests = {
            'default_tz': [
                ('2020-09-01 12:00', datetime(2020, 9, 1, 12, tzinfo=pytz.utc)),
                ('2020-09-02 12:00', datetime(2020, 9, 2, 12, tzinfo=pytz.utc))
            ],
            'passed_tz': [
                ('2020-09-01 12:00+02:00', datetime(2020, 9, 1, 12, tzinfo=timezone(timedelta(0, 7200), '+0200'))),
                ('2020-09-02 12:00+02:00', datetime(2020, 9, 2, 12, tzinfo=timezone(timedelta(0, 7200), '+0200'))),
            ],
        }
        for name, (start_date, end_date) in tests.items():
            with self.subTest(name=name):
                with open(os.devnull, 'w') as devnull, \
                        patch('kafka_consumer.management.commands.consume_junk_messages.Command.prepare_messages_filters') as mock:
                    call_command(
                        'consume_junk_messages',
                        '--start_date={}'.format(start_date[0]),
                        '--end_date={}'.format(end_date[0]),
                        stdout=devnull,
                    )

                mock.assert_called_once_with(
                    {
                        'end_date': end_date[1],
                        'end_offset': None,
                        'ids': None,
                        'start_date': start_date[1],
                        'start_offset': None,
                        'subscriber': None,
                    },
                    {
                        'subscriber': None,
                        'ids': 'pk__in',
                        'start_offset': 'offset__gte',
                        'end_offset': 'offset__lte',
                        'start_date': 'created_at__gte',
                        'end_date': 'created_at__lte',
                    },
                )
