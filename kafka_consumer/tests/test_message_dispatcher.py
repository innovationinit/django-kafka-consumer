from unittest.mock import patch

from django.test.testcases import TestCase

from kafka_consumer.messages import (
    Message,
    MessageDispatcher,
)
from kafka_consumer.tests.helpers import (
    JsonMessageProcessor,
    TestableBaseSubscriber,
    fake_message_value,
    fake_raw_message,
)
from kafka_consumer.utils import get_class_path


class TestMessageDispatcher(TestCase):
    def setUp(self):
        subscribers = [get_class_path(TestableBaseSubscriber)]
        self.dispatcher = MessageDispatcher(subscribers, 'test_topic')

    def test_subscribers_instantiation(self):
        self.assertEqual(len(self.dispatcher._subscribers), 1)
        self.assertIsInstance(self.dispatcher._subscribers[0], TestableBaseSubscriber)

    @patch('kafka_consumer.tests.helpers.TestableBaseSubscriber._handle')
    def test_send_message(self, mock):
        raw_msg = fake_raw_message(fake_message_value())
        msg = Message(JsonMessageProcessor(raw_msg))
        self.dispatcher.send_message(msg, raw_msg)
        self.assertEqual(mock.call_count, 1)
