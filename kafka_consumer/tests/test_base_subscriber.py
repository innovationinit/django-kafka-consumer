from unittest.mock import patch

from django.test.testcases import TestCase

from kafka_consumer.messages import Message
from kafka_consumer.tests.helpers import (
    JsonMessageProcessor,
    TestableBaseSubscriber,
    fake_message_value,
    fake_raw_message,
)


class TestBaseSubscriber(TestCase):
    @classmethod
    def setUpClass(cls):
        super(TestBaseSubscriber, cls).setUpClass()
        cls.subscriber = TestableBaseSubscriber()

    @patch('kafka_consumer.tests.helpers.TestableBaseSubscriber._handle')
    def test_process_message_processing(self, mock):
        value = fake_message_value()
        msg = Message(JsonMessageProcessor(fake_raw_message(value)))
        self.subscriber.process_message(msg)
        mock.assert_called_once()

    @patch('kafka_consumer.tests.helpers.TestableBaseSubscriber._should_process_message')
    @patch('kafka_consumer.tests.helpers.TestableBaseSubscriber._handle')
    def test_process_message_not_processing(self, mock_handle, mock_should_process_message):
        mock_should_process_message.return_value = False
        value = fake_message_value()
        msg = Message(JsonMessageProcessor(fake_raw_message(value)))
        self.subscriber.process_message(msg)
        mock_handle.assert_not_called()
