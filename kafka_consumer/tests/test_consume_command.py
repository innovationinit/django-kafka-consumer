import os
import random
from functools import partial
from unittest import TestCase as LogTestCase
from unittest.mock import patch

from kafka import (
    KafkaClient,
    TopicPartition,
)
from kafka.consumer.fetcher import ConsumerRecord

from django.core.management import call_command
from django.test.testcases import TestCase

from kafka_consumer.consumer import KafkaConsumerContext
from kafka_consumer.tests.helpers import (
    JsonMessageProcessor,
    TestableBaseSubscriber,
    create_kafka_message,
    fake_message_value,
    kafka_client_init,
    kafka_client_close,
    kafka_consumer_close,
    kafka_consumer_seek,
    kafka_consumer_end_offsets,
    kafka_consumer_partitions_for_topic,
    kafka_consumer_poll,
    mock_kafka_server,
    stringify_message_value,
    enabled_logging,
)
from kafka_consumer.utils import get_class_path

EVENT_NAME = 1
EVENT_DATA = {
    'key1': 'value',
    'key2': 'value',
}


def fake_messages_stringified():
    for message in [
        fake_message_value(),
        fake_message_value(name=EVENT_NAME, data=EVENT_DATA),
    ]:
        yield stringify_message_value(message)


class TestConsumeCommand(TestCase, LogTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestConsumeCommand, cls).setUpClass()
        cls.topics = {
            'test_topic': {
                'topic': 'test_topic',
                'group': 'sok_bpo_core_group',
                'client': 'bpo_stats',
                'subscribers': (
                    get_class_path(TestableBaseSubscriber),
                ),
                'message_processor': 'bpo_events_message',
                'max_number_of_messages_in_batch': 10,
            },
        }
        cls.topic_message_processors = {
            'bpo_events_message': {
                'class': get_class_path(JsonMessageProcessor),
            },
        }

    @mock_kafka_server(messages_values=fake_messages_stringified())
    @patch('{}._handle'.format(get_class_path(TestableBaseSubscriber)))
    def test_command(self, mock):

        with open(os.devnull, 'w') as devnull:
            with patch('kafka_consumer.settings.KAFKA_CONSUMER_TOPICS', self.topics), \
                    patch('kafka.consumer.group.KafkaConsumer', autospec=True) as MockedConsumer, \
                    patch('kafka_consumer.settings.KAFKA_CONSUMERS_MESSAGE_PROCESSORS', self.topic_message_processors):

                MockedConsumer.messages = [
                    ConsumerRecord("test_topic", 0, i + 16123456, None, None, None, create_kafka_message(value), None, None, None, None, None)
                    for i, value in enumerate(fake_messages_stringified() or [])
                ]
                MockedConsumer.poll.side_effect = partial(kafka_consumer_poll, MockedConsumer)

                def mocked_enter(self):
                    self.kafka = MockedConsumer
                    return MockedConsumer

                with patch.object(KafkaConsumerContext, '__enter__', mocked_enter):
                    call_command('consume', topic='test_topic', stdout=devnull)

            self.assertEqual(mock.call_count, 2)

    def test_first_db_is_working(self):

        with open(os.devnull, 'w') as devnull:
            with patch('kafka_consumer.settings.KAFKA_CONSUMER_TOPICS', self.topics), \
                    patch('kafka_consumer.settings.KAFKA_CONSUMERS_MESSAGE_PROCESSORS', self.topic_message_processors), \
                    patch.object(KafkaClient, '__init__', kafka_client_init), \
                    patch.object(KafkaClient, 'close', kafka_client_close), \
                    patch('kafka.consumer.group.KafkaConsumer', autospec=True) as MockedConsumer, \
                    patch('kafka_consumer.management.commands.consume.Command._db_is_working') as check_mock, \
                    patch('kafka_consumer.messages.MessageBatchProcessor.process_message_batch') as msg_batch_mock:

                check_mock.side_effect = [False, True, True]

                MockedConsumer.messages = [
                    ConsumerRecord("test_topic", 0, i + 16123456, None, None, None, create_kafka_message(value), None, None, None, None, None)
                    for i, value in enumerate(fake_messages_stringified() or [])
                ]
                MockedConsumer.offsets = {TopicPartition("test_topic", i): random.randint(10, 1200) for i in range(5)}
                MockedConsumer.poll.side_effect = partial(kafka_consumer_poll, MockedConsumer)
                MockedConsumer.partitions_for_topic.side_effect = partial(kafka_consumer_partitions_for_topic, MockedConsumer)
                MockedConsumer.end_offsets.side_effect = partial(kafka_consumer_end_offsets, MockedConsumer)
                MockedConsumer.seek.side_effect = partial(kafka_consumer_seek, MockedConsumer)
                MockedConsumer.close.side_effect = partial(kafka_consumer_close, MockedConsumer)

                def mocked_enter(self):
                    self.kafka = MockedConsumer
                    return MockedConsumer

                with patch.object(KafkaConsumerContext, '__enter__', mocked_enter):
                    call_command('consume', topic='test_topic', stdout=devnull)

            MockedConsumer.poll.assert_called_once()
            msg_batch_mock.assert_called_once()

    def test_second_db_is_working(self):

        with open(os.devnull, 'w') as devnull:
            with patch('kafka_consumer.settings.KAFKA_CONSUMER_TOPICS', self.topics), \
                    patch('kafka_consumer.settings.KAFKA_CONSUMERS_MESSAGE_PROCESSORS', self.topic_message_processors), \
                    patch.object(KafkaClient, '__init__', kafka_client_init), \
                    patch.object(KafkaClient, 'close', kafka_client_close), \
                    patch('kafka.consumer.group.KafkaConsumer', autospec=True) as MockedConsumer, \
                    patch('kafka_consumer.management.commands.consume.Command._db_is_working') as check_mock, \
                    patch('kafka_consumer.management.commands.consume.Command.reset_offsets') as reset_mock, \
                    patch('kafka_consumer.messages.MessageBatchProcessor.process_message_batch') as msg_batch_mock:

                check_mock.side_effect = [True, False, True, True]

                MockedConsumer.messages = [
                    ConsumerRecord("test_topic", 0, i + 16123456, None, None, None, create_kafka_message(value), None, None, None, None, None)
                    for i, value in enumerate(fake_messages_stringified() or [])
                ]
                MockedConsumer.offsets = {TopicPartition("test_topic", i): random.randint(10, 1200) for i in range(5)}
                MockedConsumer.poll.side_effect = partial(kafka_consumer_poll, MockedConsumer)
                MockedConsumer.partitions_for_topic.side_effect = partial(kafka_consumer_partitions_for_topic, MockedConsumer)
                MockedConsumer.end_offsets.side_effect = partial(kafka_consumer_end_offsets, MockedConsumer)
                MockedConsumer.seek.side_effect = partial(kafka_consumer_seek, MockedConsumer)
                MockedConsumer.close.side_effect = partial(kafka_consumer_close, MockedConsumer)

                def mocked_enter(self_):
                    self_.kafka = MockedConsumer
                    return MockedConsumer

                with patch.object(KafkaConsumerContext, '__enter__', mocked_enter):
                    call_command('consume', topic='test_topic', stdout=devnull)

            self.assertEqual(2, MockedConsumer.poll.call_count)
            reset_mock.assert_called_once()
            msg_batch_mock.assert_called_once()

    @mock_kafka_server(messages_values=fake_messages_stringified())
    def test_logging_when_junk_message_fails(self):
        with enabled_logging(), \
                open(os.devnull, 'w') as devnull:
            with patch('kafka_consumer.settings.KAFKA_CONSUMER_TOPICS', self.topics), \
                    patch('kafka_consumer.settings.KAFKA_CONSUMERS_MESSAGE_PROCESSORS', self.topic_message_processors), \
                    patch('kafka_consumer.messages.MessageDispatcher.process_message', return_value={'consumed': False, 'exception': KeyError()}), \
                    patch('kafka_consumer.managers.JunkMessageManager.handle_junk_message', side_effect=ValueError), \
                    patch('kafka.consumer.group.KafkaConsumer', autospec=True) as MockedConsumer, \
                    self.assertLogs('kafka_consumer', level='ERROR') as kl:

                MockedConsumer.messages = [
                    ConsumerRecord("test_topic", 0, i + 16123456, None, None, None, create_kafka_message(value), None, None, None, None, None)
                    for i, value in enumerate(fake_messages_stringified() or [])
                ]
                MockedConsumer.poll.side_effect = partial(kafka_consumer_poll, MockedConsumer)

                def mocked_enter(self):
                    self.kafka = MockedConsumer
                    return MockedConsumer

                with patch.object(KafkaConsumerContext, '__enter__', mocked_enter):
                    call_command('consume', topic='test_topic', stdout=devnull)

                self.assertNotEqual(kl.output, [])

                expected_fragment = "ERROR:kafka_consumer.messages.dispatcher:There was an error during creation of JunkMessage"
                self.assertIn(expected_fragment, kl.output[0])
                self.assertIn(expected_fragment, kl.output[1])
