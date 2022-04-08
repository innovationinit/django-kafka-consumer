from django.conf import settings

KAFKA_HOSTS = getattr(settings, 'KAFKA_HOSTS')

KAFKA_CONSUMER_MESSAGE_BATCH_PROCESSOR_CLASS = getattr(
    settings,
    'KAFKA_CONSUMER_MESSAGE_BATCH_PROCESSOR_CLASS',
    'kafka_consumer.messages.MessageBatchProcessor'
)

"""Topics configuration
{
    'key': {
        'topic': 'topic name',
        'group': 'topic group',
        'subscribers': (
            'path.to.subscriber.Class',
        ),
        message_processor': 'processor key',  # lookup in KAFKA_CONSUMERS_MESSAGE_PROCESSORS
        'wait': 0,  # optional, indicates how many seconds Kafka will wait to fillup buffer, None or ommited means wait forever
        'consumer_options': {},  # optional, overrides options used to create KafkaConsumer
    },
}
"""
KAFKA_CONSUMER_TOPICS = getattr(settings, 'KAFKA_CONSUMER_TOPICS')

"""Message processors configuration
{
    'key': {
        'class': 'path.to.messageprocessor.Class',
        # Processors init arguments, e.g.
        'rsa_private_key_path': 'path/to/private/key'
    },
}
"""
KAFKA_CONSUMERS_MESSAGE_PROCESSORS = getattr(settings, 'KAFKA_CONSUMERS_MESSAGE_PROCESSORS')

CONSUMER_MESSAGE_BATCH_SIZE = getattr(settings, 'CONSUMER_MESSAGE_BATCH_SIZE', 1000)

KAFKA_CONSUMER_DEFAULT_TIMEOUT = getattr(settings, 'KAFKA_CONSUMER_DEFAULT_TIMEOUT', 10)

KAFKA_CONSUMER_SSL_SETTINGS = getattr(settings, 'KAFKA_CONSUMER_SSL_SETTINGS', {'security_protocol': 'PLAINTEXT'})
