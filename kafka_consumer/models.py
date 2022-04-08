# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals

from django.db import models
from django.utils.translation import ugettext_lazy as _

from .fields import Base64TextField
from .managers import JunkMessageManager


class TimeStampedModel(models.Model):

    """An abstract model with creation and modification dates."""

    created_at = models.DateTimeField(
        _('kafka-consumer.models.created-at'),
        auto_now_add=True,
        db_index=True
    )
    updated_at = models.DateTimeField(
        _('kafka-consumer.models.updated-at'),
        auto_now=True,
        db_index=True
    )

    class Meta:
        abstract = True


class ConsumerState(models.Model):

    """Holds Consumer state for each topic separately"""

    offset = models.BigIntegerField(
        _('kafka-consumer.models.consumer-state.offset'),
        null=True
    )
    topic = models.CharField(
        _('kafka-consumer.models.consumer-state.topic'),
        max_length=255,
        unique=True
    )


class JunkMessage(TimeStampedModel, models.Model):

    """Junk message class for store messages which failed.

    Note, that it should acts like BaseForeignMessage as it comes from external source.
    It should implement consumer.kafka_consumer.BaseForeignMessage interface,
    but it doesn't work with django.Model, so doesn't based on it.
    """

    subscriber = models.CharField(
        _('kafka-consumer.models.junk-message.subscriber'),
        max_length=100
    )
    raw_data = Base64TextField(
        _('kafka-consumer.models.junk-message.raw-data'),
    )
    error_message = models.CharField(
        _('kafka-consumer.models.junk-message.error-message'),
        max_length=1000,
        null=False,
        blank=False
    )
    offset = models.BigIntegerField(
        _('kafka-consumer.models.junk-message.offset'),
    )
    topic_key = models.CharField(
        _('kafka-consumer.models.junk-message.topick-key'),
        max_length=100
    )

    objects = JunkMessageManager()

    def update_message(self, exception):
        """Update exception message, when retry fail again

        :param exception: Exception, that occurred in parsing or processing time
        :type exception: Exception
        """

        max_length = self._meta.get_field('error_message').max_length
        self.error_message = str(exception)[:max_length]
        self.save()
