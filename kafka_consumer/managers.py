from django.db.models import Manager

from .utils import get_class_path


class JunkMessageManager(Manager):
    def handle_junk_message(self, subscriber, raw_message, exception, topic_key):
        """Store kafka message in original form for further process

        :param subscriber: Message subscriber, processes given message
        :type subscriber: BaseSubscriber

        :param raw_message: Original message
        :type raw_message: consumer.kafka_consumer.BaseForeignMessage

        :param exception: Exception, that occurred in parsing or processing time
        :type exception: Exception

        :param topic_key: Topic entry key, to be lookup in config at retry time. Used to instantiate message processors
        :type topic_key: str
        """
        max_length = self.model._meta.get_field('error_message').max_length
        self.create(
            subscriber=get_class_path(subscriber.__class__),
            raw_data=raw_message.raw_data,
            offset=raw_message.offset,
            error_message=str(exception)[:max_length],
            topic_key=topic_key,
        )
