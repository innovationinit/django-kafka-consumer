# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import logging
from itertools import chain
from time import sleep
from typing import (  # noqa
    Dict,
    List,
)

from kafka import (  # noqa
    KafkaConsumer,
    TopicPartition,
)

from django.core.management.base import (
    BaseCommand,
    CommandError,
)
from django.db import (
    OperationalError,
    transaction,
)

from kafka_consumer import settings
from kafka.consumer.fetcher import ConsumerRecord
from kafka_consumer.consumer import (
    KafkaConsumerContext,
    KafkaMessageBatch,
)
from kafka_consumer.utils import get_message_batch_processor_class


logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """A management command that runs kafka consumer"""

    help = 'Consume kafka messages'

    def add_arguments(self, parser):
        """Add the arguments to the parser instance
        :param parser: the parser instance
        :type parser: `django.core.management.base.CommandParser`
        """
        parser.add_argument('--supervised', action='store_true', dest='blocking', default=False,
                            help='Wait for next message infinity')
        parser.add_argument('--topic', type=str, help='The kafka topic settings key to consume')

    def handle(self, *args, **options):
        """Set up the Consumer and run it.
        :param args: command arguments
        :type args: list
        :param options: parsed options
        :type options: Dict[str, Any]
        """
        topic_key = options.get('topic')
        if topic_key not in settings.KAFKA_CONSUMER_TOPICS:
            raise CommandError("Topic `{0}` DOES NOT EXIST in settings!".format(topic_key or ''))
        self._consume(topic_key, options['blocking'])

    def _get_offsets(self, kafka_consumer, topic):  # type: (KafkaConsumer, str) -> Dict[TopicPartition, int]
        """Returns topic offsets
        """
        offsets = {}
        for partition in kafka_consumer.partitions_for_topic(topic):
            topic_part = TopicPartition(topic, partition)
            offsets[topic_part] = (
                kafka_consumer.position(topic_part) if topic_part in kafka_consumer.assignment() else kafka_consumer.committed(topic_part)
            )

        return offsets

    def _consume(self, topic_key, blocking):
        """Connect to kafka server, read and save messages in database.
        :param topic_key: topic name key to be looked in settings.KAFKA_CONSUMER_TOPICS
        :type topic_key: str
        :param blocking: if True wait for next message infinity, otherwise read only currently available messages
        :type blocking: bool
        """
        topic_entry = settings.KAFKA_CONSUMER_TOPICS[topic_key]
        topic = topic_entry['topic']
        group = topic_entry['group']
        client = topic_entry['client']
        consumer_options = topic_entry.get('consumer_options', {})
        self.stdout.write("Start reading from kafka topic {} as {} group as client {}".format(topic, group, client))

        msg_batch_processor = get_message_batch_processor_class()(topic_key)
        timeout = topic_entry.get('wait', 0)

        with KafkaConsumerContext(
            topic=topic,
            group=group,
            blocking=blocking,
            client=client,
            max_buffer_size=int(settings.CONSUMER_MESSAGE_BATCH_SIZE / 1000 * 2 * 1024 * 1024),
            timeout=timeout,
            consumer_options=consumer_options,
        ) as kafka_consumer:  # type: KafkaConsumer

            initial_offsets = {}

            while True:

                # First we make a crude check whether db connection is fine
                if not self._db_is_working():
                    continue

                initial_offsets.update(self._get_offsets(kafka_consumer, topic))

                result = kafka_consumer.poll(  # type: Dict[TopicPartition, List[ConsumerRecord]]
                    timeout_ms=(timeout or settings.KAFKA_CONSUMER_DEFAULT_TIMEOUT) * 1000,
                    max_records=topic_entry['max_number_of_messages_in_batch'],
                )

                messages = list(chain.from_iterable(result.values()))
                kafka_message_batch = KafkaMessageBatch(messages)
                if kafka_message_batch:
                    if not self._db_is_working():
                        logger.error(
                            'Operational error after message consumption! Resetting offsets...',
                            extra={'batch': kafka_message_batch}
                        )
                        self.reset_offsets(kafka_consumer, initial_offsets)
                        continue
                    msg_batch_processor.process_message_batch(kafka_message_batch)
                else:
                    msg_batch_processor.notify_alive()

                if not blocking:
                    break

    @staticmethod
    def _db_is_working():
        """Check if there is a problem with db connection."""
        try:
            with transaction.atomic():
                return True
        except OperationalError:
            sleep(1)
            return False

    @staticmethod
    def reset_offsets(consumer, initial_offsets):
        """Set offsets on the consumer's partitions.

        :param consumer: Kafka consumer instance
        :type consumer: kafka.consumer.KafkaConsumer
        :param initial_offsets: offsets to reset partitions to
        :type initial_offsets: Dict[kafka.TopicPartition, int]
        """
        for partition, offset in initial_offsets.items():
            try:
                consumer.seek(partition, offset)
            except AssertionError as e:
                logger.warning('Assertion error when seeking partition %s', partition, exc_info=e)
