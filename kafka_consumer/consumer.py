import logging

from kafka import KafkaConsumer

from .settings import *
from .utils import get_class_path

logger = logging.getLogger(__name__)


class BaseForeignMessage(object):
    """Represents message in serialized form, type of raw_data should be bytes

    Origin of this message is either Kafka or JunkMessage
    Assumes that both sources store data in serialized form, which fit to field Base64TextField on failure
    without any conversion

    This message is processed by MessageBatchProcessor.process_message
    """
    @property
    def offset(self):
        raise NotImplementedError()

    @property
    def raw_data(self):
        """Raw data"""
        raise NotImplementedError()


class KafkaMessage(BaseForeignMessage):
    """Wrapper over kafka message providing DAO for needed fields"""

    def __init__(self, kafka_message):
        self.__kafka_message = kafka_message

    @property
    def offset(self):
        return self.__kafka_message.offset

    @property
    def raw_data(self):
        return self.__kafka_message.value

    def __str__(self):
        return str(self.__kafka_message)


class KafkaMessageBatchIter(object):
    def __init__(self, message_batch_iter):
        self.__message_batch_iter = message_batch_iter

    def __iter__(self):
        return self

    def next(self):
        return KafkaMessage(next(self.__message_batch_iter))

    def __next__(self):
        return KafkaMessage(next(self.__message_batch_iter))


class KafkaMessageBatch(object):
    """Wrapper over kafka get_messages

    For separation between messages.MessageBatchProcessor and Kafka (management/commands/consume)
    to allow support Junk Message
    and simplify MessageBatchProcessor testing
    """

    def __init__(self, kafka_batch):
        self.__kafka_batch = kafka_batch

    def __iter__(self):
        return KafkaMessageBatchIter(iter(self.__kafka_batch))

    def __str__(self):
        return str(self.__kafka_batch)


class KafkaConsumerContext(object):
    """Context manager that start connection to kafka server."""

    def __init__(
        self,
        topic,
        group=None,
        client=None,
        blocking=False,
        buffer_size=4096,
        max_buffer_size=32768,
        timeout=None,
        consumer_options=None,
    ):  # pylint: disable=too-many-arguments
        """KafkaConsumerContext init method.

        :param group: consumer group name
        :type group: str
        :param client: client name
        :type client: str
        :param topic: topic name
        :type topic: str
        :param blocking: if True wait for next message infinity, otherwise read only currently available messages
        :type blocking: bool
        :param buffer_size:  initial number of bytes to tell kafka we have available
        :type buffer_size: int
        :param max_buffer_size: max number of bytes to tell kafka we have available. None means no limit.
        :type max_buffer_size: int or None
        :param timeout: number of second to wait for buffer filling up, None means wait forever, 0 no wait, +int wait n seconds.
        used only if blocking
        :type timeout: int or None
        :param consumer_options: options for kafka.consumer.KafkaConsumer initialization
        :type consumer_options: dict or None
        """
        self.group = group if group else str(get_class_path(self.__class__))
        self.client = client
        self.topic = topic
        self.blocking = blocking
        self.max_buffer_size = max_buffer_size
        self.buffer_size = buffer_size
        self.kafka = None
        self.timeout = timeout
        self.consumer_options = consumer_options or {}

    def calculate_timeout(self):
        if not self.blocking:
            return 0

        return self.timeout

    def __enter__(self):
        """Start connection to kafka server.

        :return: kafka consumer
        :rtype: kafka.consumer.KafkaConsumer
        """
        # Complete consumer configuration kwargs in 3 steps
        # 1. Get configuration from settings
        options = {
            'bootstrap_servers': KAFKA_HOSTS,
            'security_protocol': settings.KAFKA_CONSUMER_SSL_SETTINGS.get('security_protocol'),
            'ssl_cafile': settings.KAFKA_CONSUMER_SSL_SETTINGS.get('ssl_cafile'),
            'ssl_certfile': settings.KAFKA_CONSUMER_SSL_SETTINGS.get('ssl_certfile'),
            'ssl_keyfile': settings.KAFKA_CONSUMER_SSL_SETTINGS.get('ssl_keyfile'),
            'ssl_check_hostname': False,
        }
        # 2. Get configuration from consumer options set in topic config - can override earlier data
        options.update(self.consumer_options)
        # 3. Get configuration from context - override earlier data
        options.update(
            client_id=self.client,
            group_id=self.group,
            fetch_max_bytes=self.max_buffer_size,
        )
        self.kafka = KafkaConsumer(
            self.topic,
            **options
        )
        return self.kafka

    def __exit__(self, exc_type, exc_val, exc_tb):
        """End connection to kafka server."""
        if self.kafka:
            self.kafka.close()
            self.kafka = None
