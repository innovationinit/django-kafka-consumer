import abc
import logging

from django.db import transaction


logger = logging.getLogger(__name__)


class BaseSubscriber(object):
    """Base class for subscribers."""

    __metaclass__ = abc.ABCMeta

    def process_message(self, message):
        """Process message
        :param message: message
        :type message: consumer.messages.Message
        """
        if self._should_process_message(message):
            logger.info("START processing message with %s", self.__class__.__name__, extra={'kafka_message': message})
            with transaction.atomic():
                self._handle(message)
            logger.info("STOP processing message with %s", self.__class__.__name__, extra={'kafka_message': message})

    @abc.abstractmethod
    def _handle(self, message):
        """Handle message
        :param message: message
        :type message: consumer.messages.Message
        """
        pass

    @abc.abstractmethod
    def _should_process_message(self, message):
        """Check if message should be processed
        :param message: message
        :type message: consumer.messages.Message
        :return: True if message should be processed, False if it shouldn't be processed
        :rtype: bool
        """
        return False
