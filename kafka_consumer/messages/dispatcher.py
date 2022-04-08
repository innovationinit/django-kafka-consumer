import logging

from django.utils.module_loading import import_string
from kafka_consumer.exceptions import ErrorMessageReceiverProcess
from kafka_consumer.models import JunkMessage


logger = logging.getLogger(__name__)


class MessageDispatcher:

    """Sends message to each subscriber"""

    def __init__(self, subscribers_paths, topic_key):
        """MessageDispatcher init method

        :param subscribers_paths: list or tuple of dotted paths to subscribers classes
        :type subscribers_paths: list or tuple

        :param topic_key: Topic entry key, to be lookup in config at retry time. Used to instantiate message processors
        :type topic_key: str
        """
        self._subscribers_paths = subscribers_paths
        self._subscribers_instances = {}
        self._register_subscribers()
        self._topic_key = topic_key

    def process_message(self, subscriber, message):
        """Process single message by single subscriber

        :param subscriber: Message subscriber, processes given message
        :type subscriber: BaseSubscriber

        :param message: Message to process by subscribers, may be null, what means parse error
        :type message: consumer.messages.Message

        :return: process result, needed for JunkMessages, contains information about result and eventually exception
        :rtype: {'consumed': bool, exception: Exception}
        """
        consumed = False
        exception = None

        if message:
            try:
                subscriber.process_message(message)
            except ErrorMessageReceiverProcess as e:
                logger.exception(
                    "Expected exception occurred during processing message by %s: %s. ex debug: etype: %s",
                    subscriber.__class__.__name__, e,
                    str(message.type),
                    extra={'data': message.data}
                )
                exception = e
            except Exception as e:
                logger.exception(
                    "Unexpected exception occurred during processing message by %s: %s.",
                    subscriber.__class__.__name__, e
                )
                exception = e
            else:
                consumed = True

        return {'consumed': consumed, 'exception': exception}

    def send_message(self, message, raw_message, exception=None, retry=False):
        """Send message to all subscribers

        :param message: Message to process by subscribers, may be null, what means parse error
        :type message: consumer.messages.Message

        :param raw_message: Original message, to allow store as JunkMessage on failure
        :type raw_message: consumer.kafka_consumer.BaseForeignMessage

        :param exception: Exception, that occurred in parsing or processing time
        :type exception: Exception

        :param retry: Indicate, that this is JunkMessage, so it should handle JunkMessage in different way,
        In normal situation, JunkMessage is created if process fail, in retry situation is should delete JunkMessage if success and
        update if fail
        :type retry: bool
        """
        if not retry:
            subscribers = self._subscribers
        else:
            subscribers = [self._subscribers_instances[raw_message.subscriber]]

        for subscriber in subscribers:
            consumed = False
            if not exception:
                process_result = self.process_message(subscriber, message)
                consumed = process_result['consumed']
                process_exception = process_result.get('exception')

            if not retry and not consumed:
                try:
                    JunkMessage.objects.handle_junk_message(
                        subscriber=subscriber,
                        raw_message=raw_message,
                        exception=exception or process_exception,
                        topic_key=self._topic_key,
                    )
                except Exception as e:
                    logger.error(
                        'There was an error during creation of JunkMessage for topic %s in subscriber %s',
                        self._topic_key,
                        subscriber,
                        extra={'raw_message': raw_message},
                        exc_info=e,
                    )

            elif retry and not consumed:
                raw_message.update_message(
                    exception=exception or process_exception,
                )
            elif retry and consumed:
                raw_message.delete()

    def _register_subscribers(self):
        """Instantiate all subscribers"""
        self._subscribers = []
        for path in self._subscribers_paths:
            subscriber = import_string(path)()
            self._subscribers_instances[path] = subscriber
            self._subscribers.append(subscriber)
