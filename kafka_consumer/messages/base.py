import logging
from datetime import datetime
from typing import (
    Any,
    Type,
    Union,
)

from django.utils.functional import cached_property

from kafka_consumer import settings


logger = logging.getLogger(__name__)


class Message:
    """Envelope for data

    Includes meta data, and actual data, which is accessed by property data.
    It should be transport independent.

    In contrast to consumer.kafka_consumer.BaseForeignMessage it represents unserialized, decrypted, message, ready to send to subscribers
    Note: It can still have data in serialized form, but this is the matter of subscriber and further stuff, f.e. CoreLead, which store
    data in serialized form and deserialized by "serek" in the fly.
    """

    def __init__(self, message_processor: 'BaseMessageProcessor', offset: int = 0):
        """BaseMessage init method"""
        self.message_processor = message_processor
        self.offset = offset

    @cached_property
    def type(self) -> Union[str, int]:
        """Type of message."""
        return self.message_processor.get_type()

    @cached_property
    def time(self) -> datetime:
        return self.message_processor.get_time()

    @cached_property
    def data(self) -> Any:
        return self.message_processor.get_data()

    def __str__(self) -> str:
        return f"Message(type={self.type}, data={self.data}, time={self.time})"


class BaseMessageProcessor:

    MESSAGE_CLASS: Type[Message] = Message

    def __init__(self, kafka_message, config_key=None):
        """BaseMessageProcessor init method

        :param kafka_message: kafka message to be looked in KAFKA_CONSUMERS_MESSAGE_PROCESSORS
        :type kafka_message: unicode or str
        :param config_key: kafka message
        :type config_key: unicode or str
        """

        if config_key:
            self.config_entry = settings.KAFKA_CONSUMERS_MESSAGE_PROCESSORS[config_key]
        else:
            self.config_entry = None

        self._kafka_message = kafka_message

    def get_data(self):
        """Return data field value from message
        :raises: BadEventData
        """
        raise NotImplementedError()

    def get_type(self):
        """Return type of message.
        :return: message name
        :rtype: unicode or str
        """
        raise NotImplementedError()

    def get_time(self):
        """Return time field value from message.
        :return: message datetime or None if value is too short
        :rtype: datetime
        :raises: BadEventTimestampValue
        """
        raise NotImplementedError()

    def get_processed_message(self, offset: int = 0) -> Message:
        return self.MESSAGE_CLASS(self, offset)
