# -*- coding: utf-8 -*-

"""A management command that runs the subscriber to consume junk messages"""

from __future__ import unicode_literals

from django.core.management.base import (
    BaseCommand,
    CommandError,
)
from django.utils import timezone
from django.utils.dateparse import parse_datetime
from django.utils.module_loading import import_string

from kafka_consumer import settings
from kafka_consumer.models import JunkMessage
from kafka_consumer.utils import get_message_batch_processor_class


def parsed_datetime(value):
    """format value to datetime"""
    dt = parse_datetime(value)
    if dt is None:
        raise CommandError('Given datetime has wrong format')
    if dt.tzinfo is None:
        dt = timezone.make_aware(dt)
    return dt


class Command(BaseCommand):

    """A management command that runs the subscriber for given subscriber class"""

    help = 'Consume junk messages'
    subscribers = {}

    def add_arguments(self, parser):
        """Add the subscriber, identifiers, offset start_date and end_date arguments to the parser instance

        :param parser: the parser instance
        :type parser: `django.core.management.base.CommandParser`
        """
        parser.add_argument('--subscriber', nargs='?', type=str, help='The path to subscriber class')
        parser.add_argument('--ids', nargs='*', type=int, help='Identifiers of JunkMessages')
        parser.add_argument('--start_offset', type=int, help='Consume junk messages with offset greater than this value')
        parser.add_argument('--end_offset', type=int, help='Consume junk messages with offset lower than this value')
        parser.add_argument('--start_date', type=parsed_datetime, help='Consume junk message last consume after this date')
        parser.add_argument('--end_date', type=parsed_datetime, help='Consume junk message last consume before this date')

    def prepare_messages_filters(self, options, filter_config):
        """Prepare filter config from options

        :param options: dict of command options
        :type options: dict
        :param filter_config: configuration of filters
        :type filter_config: dict
        """
        return {
            filter_name or option_name: options.get(option_name)
            for option_name, filter_name in filter_config.items() if options.get(option_name)
        }

    def get_subscriber(self, subscriber_class_path):
        """Return subscriber object for given class path

        :param subscriber_class_path: subscriber class path
        :type subscriber_class_path: str or unicode
        :return: subscriber object
        :type: Class
        """
        if not self.subscribers.get(subscriber_class_path):
            Subscriber = import_string(subscriber_class_path)
            subscriber = Subscriber()
            self.subscribers[subscriber_class_path] = subscriber
        return self.subscribers[subscriber_class_path]

    def handle(self, *args, **options):
        """Set up the subscriber and run it

        :param args: command arguments
        :type args: list
        :param options: parsed options
        :type options: dict
        """
        filters_config = {
            'subscriber': None,
            'ids': 'pk__in',
            'start_offset': 'offset__gte',
            'end_offset': 'offset__lte',
            'start_date': 'created_at__gte',
            'end_date': 'created_at__lte',
        }
        filters = self.prepare_messages_filters({k: v for k, v in options.items() if k in filters_config}, filters_config)
        messages = JunkMessage.objects.filter(**filters).order_by('offset')

        topics_message_batch_processors = {}

        for junk_message in messages:
            topic_key = junk_message.topic_key
            if topic_key not in settings.KAFKA_CONSUMER_TOPICS:
                raise CommandError("Topic `{0}` DOES NOT EXIST in settings!".format(topic_key))

            if topic_key not in topics_message_batch_processors:
                topics_message_batch_processors[topic_key] = get_message_batch_processor_class()(topic_key)
            message_batch_processor = topics_message_batch_processors[topic_key]
            message_batch_processor.process_message(junk_message, retry=True)
