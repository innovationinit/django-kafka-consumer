import logging
from typing import (
    List,
    Type,
)

from django.utils.module_loading import import_string

from kafka_consumer import settings
from kafka_consumer.connection_cleaning import cleanup_db_connections
from kafka_consumer.consumer import BaseForeignMessage

from .base import BaseMessageProcessor
from .dispatcher import MessageDispatcher


logger = logging.getLogger(__name__)


class MessageBatchProcessor:

    """Processes kafka message batch"""

    topic_entry: dict
    message_processor_key: str
    message_processor_class: Type[BaseMessageProcessor]

    def __init__(self, topic_key: str):
        """MessageBatchProcessor init method

        :param topic_key: topic name key to be looked in settings.KAFKA_CONSUMER_TOPICS
        :type topic: str
        """
        self.topic_entry = settings.KAFKA_CONSUMER_TOPICS[topic_key]

        subscribers = self.topic_entry['subscribers']
        self._msg_dispatcher = MessageDispatcher(subscribers, topic_key)

        self.message_processor_key = self.topic_entry['message_processor']
        message_processor_class = settings.KAFKA_CONSUMERS_MESSAGE_PROCESSORS[self.message_processor_key]['class']
        if isinstance(message_processor_class, str):
            self.message_processor_class = import_string(message_processor_class)
        else:
            self.message_processor_class = message_processor_class

    def process_message(self, raw_message: BaseForeignMessage, retry: bool = False):
        """Parse given message

        :param raw_message: Raw message from kafka or JunkMessage
        :type raw_message: BaseForeignMessage

        :param retry: Parameter passed directly to send_message method.
        :type retry: bool

        """

        message = None
        exception = None

        try:
            message_processor = self.message_processor_class(
                raw_message.raw_data,
                self.message_processor_key
            )
            message = message_processor.get_processed_message(raw_message.offset)
        except Exception as e:
            logger.exception("Exception occurred during processing message: ex debug: offset: %d", raw_message.offset)
            exception = e

        self._msg_dispatcher.send_message(
            message=message,
            raw_message=raw_message,
            exception=exception,
            retry=retry,
        )

    def process_message_batch(self, message_batch: List[BaseForeignMessage]):
        """Parse given message batch

        :param message_batch: list of kafka messages
        :type message_batch: List[BaseForeignMessage]
        """
        with cleanup_db_connections():
            for kafka_message in message_batch:
                try:
                    self.process_message(kafka_message)
                except Exception as e:
                    logger.critical(
                        'There was an unexpected error during batch processing!',
                        extra={'kafka_message': kafka_message},
                        exc_info=e,
                    )

    def notify_alive(self):
        """Hook executed periodically by consume command. Can be used to implement health checks."""
