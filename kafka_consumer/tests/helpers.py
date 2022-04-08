import logging
from contextlib import contextmanager

import json
import pickle
import random
from functools import wraps
from unittest.mock import patch

from kafka.consumer.fetcher import ConsumerRecord
from kafka import (
    KafkaClient,
    KafkaConsumer,
)
from kafka.protocol.message import Message
from kafka.structs import TopicPartition

from django.utils.functional import cached_property
from django.utils.timezone import (
    datetime,
    utc,
)
from kafka_consumer.subscribers.base import BaseSubscriber
from kafka_consumer.messages import BaseMessageProcessor
from kafka_consumer.exceptions import ErrorMessageReceiverProcess


def fake_generic_message(dt=datetime.now(tz=utc), name=0, data=None):
    """Generate fake message value"""
    timestamp = int((dt - datetime(1970, 1, 1, tzinfo=utc)).total_seconds() * 1000)
    return {
        "name": name,
        "time": timestamp,
        "data": data,
    }


def fake_message_value(dt=datetime.now(tz=utc), name=0, data={}):
    """Generate fake message value, params see fake_generic_message"""
    return fake_generic_message(dt, name, data)


def stringify_message_value(value):
    """Stringify message value"""
    return json.dumps(value).encode('utf-8')


def fake_raw_message(value):
    """Make fake raw message"""
    return stringify_message_value(value)


def create_kafka_message(value):
    """Create message with packed value"""
    return Message(value)


def kafka_client_init(self, *args, **kwargs):  # pylint: disable=unused-argument
    """Mocked KafkaClient __init__ method

    :param args: positional arguments of the functions that is decorated
    :type args: list
    :param kwargs: keyword arguments of the functions that is decorated
    :type kwargs: dict
    """
    pass


def kafka_client_close(self, *args, **kwargs):  # pylint: disable=unused-argument
    """Mocked KafkaClient close method

    :param args: positional arguments of the functions that is decorated
    :type args: list
    :param kwargs: keyword arguments of the functions that is decorated
    :type kwargs: dict
    """
    pass


def kafka_consumer_poll(self, max_records=None, *args, **kwargs):  # pylint: disable=unused-argument
    """Mocked KafkaConsumer kafka_consumer_poll method

    :param self: KafkaConsumer instance
    :param max_records: maximal numbers of records in batch.
    :param args: positional arguments of the functions that is decorated
    :type args: list
    :param kwargs: keyword arguments of the functions that is decorated
    :type kwargs: dict
    :return: dict of messages
    """
    if max_records is None:
        result = self.messages
        self.messages = []
    else:
        result = self.messages[:max_records]
        self.messages = self.messages[max_records:]
    result = {(TopicPartition("test_topic", 0)): result}
    return result


def kafka_consumer_close(*args):
    """Mocked KafkaConsumer kafka_consumer_close method

    :param self: KafkaConsumer instance
    """
    pass


def kafka_consumer_partitions_for_topic(self, topic):
    """Mock KafkaConsumer partitions_for_topic method

    Arguments:
        topic (str): Topic to check.

    Returns:
        set: Partition ids
    """
    return {0, 1, 2, 3, 4}


def kafka_consumer_end_offsets(self, partitions):
    """Mock KafkaConsumer end_offsets method

    Arguments:
        partitions (list): List of TopicPartition instances to fetch
            offsets for.

    Returns:
        ``{TopicPartition: int}``: The end offsets for the given partitions.

    Raises:
        UnsupportedVersionError: If the broker does not support looking
            up the offsets by timestamp.
        KafkaTimeoutError: If fetch failed in request_timeout_ms
    """

    return self.offsets


def kafka_consumer_seek(self, partition, offset):
    """Mock KafkaConsumer seek method

    Arguments:
        partition (TopicPartition): Partition for seek operation
        offset (int): Message offset in partition

     Raises:
        AssertionError: If offset is not an int >= 0; or if partition is not
            currently assigned.
    """
    try:
        self.offsets[partition] = offset
    except Exception:
        raise AssertionError


def mock_kafka_server(messages_values=None):
    """A decorator that mock kafka server

    :param messages_values: list of message values to be returned by mocked server
    :type messages_values: list
    :return: the decorator
    :rtype: 'types.FunctionType'
    """
    def decorator(func=None):
        """The decorated function to be returned

        :param func: the function that is decorated
        :type func: `types.FunctionType`
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            """Patch KafkaClient and SimpleConsumer methods before executing the function

            :param args: positional arguments of the functions that is decorated
            :type args: list
            :param kwargs: keyword arguments of the functions that is decorated
            :type kwargs: dict
            """

            def kafka_consumer_init(self, *args, **kwargs):  # pylint: disable=unused-argument
                """Mocked KafkaConsumer __init__ method

                Create sample messages based on given messages values that will be return in kafka_get_messages

                :param args: positional arguments of the functions that is decorated
                :type args: list
                :param kwargs: keyword arguments of the functions that is decorated
                :type kwargs: dict
                """
                self.messages = [ConsumerRecord("test_topic", 0, i + 16123456, None, None, None, create_kafka_message(value), None, None, None)
                                 for i, value in enumerate(messages_values or [])]
                self.offsets = {TopicPartition("test_topic", i): random.randint(10, 1200) for i in range(5)}

            with patch.object(KafkaClient, '__init__', kafka_client_init),\
                    patch.object(KafkaClient, 'close', kafka_client_close),\
                    patch.object(KafkaConsumer, '__init__', kafka_consumer_init),\
                    patch.object(KafkaConsumer, 'poll', kafka_consumer_poll),\
                    patch.object(KafkaConsumer, 'partitions_for_topic', kafka_consumer_partitions_for_topic),\
                    patch.object(KafkaConsumer, 'end_offsets', kafka_consumer_end_offsets),\
                    patch.object(KafkaConsumer, 'seek', kafka_consumer_seek),\
                    patch.object(KafkaConsumer, 'close', kafka_consumer_close):
                return func(*args, **kwargs)
        return wrapper
    return decorator


@contextmanager
def enabled_logging():
    """Ensure logging is enabled."""

    initial_level = logging.root.manager.disable
    logging.disable(logging.NOTSET)
    try:
        yield
    finally:
        logging.disable(initial_level)


class TestableBaseSubscriber(BaseSubscriber):
    def _handle(self, message):
        pass

    def _should_process_message(self, message):
        return True


class ClumsySubscriber(BaseSubscriber):
    def _handle(self, message):
        pass

    def _should_process_message(self, message):
        raise ErrorMessageReceiverProcess('Ups, I did it again')


class FaddySubscriber(BaseSubscriber):  # Wybredny, kapryśny
    def _handle(self, message):
        pass

    def _should_process_message(self, message):
        return False


class JsonMessageProcessor(BaseMessageProcessor):
    """Simple processor for purely JSON messages."""

    @cached_property
    def _parsed_kafka_message(self) -> dict:
        """Method to parse raw data from kafka message
        :rtype: dict
        :raises: BadMessageValue
        """
        return json.loads(self._kafka_message)

    def get_type(self):
        return self._parsed_kafka_message['name']

    def get_time(self):
        return self._parsed_kafka_message['time']

    def get_data(self):
        return self._parsed_kafka_message['data']


class PickleMessageProcessor(BaseMessageProcessor):
    """Processor for plain message.

    Raw data is given as dict of fields, which directly maps to Message
    without any decompression, deserialization, decryption stuff
    This class is provided for testing purpose
    """
    def __init__(self, src_kafka_message, config_key=None):
        kafka_message = pickle.loads(src_kafka_message)
        super(PickleMessageProcessor, self).__init__(kafka_message, config_key)

    def get_type(self):
        return self._kafka_message['name']

    def get_time(self):
        return self._kafka_message['time']

    def get_data(self):
        return self._kafka_message['data']


class ClumsyMessageProcessor(PickleMessageProcessor):  # Clumsy Smurf - Ciamajda
    def __init__(self, *args, **kwargs):
        raise Exception('Ups, I did it again')
