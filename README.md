Django Kafka Consumer
=====================

Purpose
-------

**Django Kafka Consumer** is an utility for consume events from
[Kafka](https://kafka.apache.org/)

Quick start
-----------

1.  Add `kafka_consumer` to your `INSTALLED_APPS` setting like this:

        INSTALLED_APPS = [
            # ...
            'kafka_consumer',
        ]

2.  Run `python manage.py migrate` to create the `kafka_consumer`
    models.
3.  Add custom subscribers as classes derived from
    `kafka_consumer.subscribers.base.BaseSubscriber`
4.  Prepare settings:

        KAFKA_HOSTS = ['kafka-host.com:9092']

        KAFKA_CONSUMER_TOPICS = {
            'topic_key': {
                'topic': 'topic name',  # no spaces allowed!
                'group': 'topic group',
                'client': 'client ID',
                'subscribers': (
                    'path.to.subscriber.Class',
                ),
                'message_processor': 'processor key',  # lookup in KAFKA_CONSUMERS_MESSAGE_PROCESSORS
                'wait': 0,  # optional, indicates how many seconds Kafka will wait to fillup buffer, None or ommited means wait forever
                'max_number_of_messages_in_batch': 200,
                'consumer_options': {  # Overrides options used to create KafkaConsumer
                    'auto_offset_reset': 'latest',
                }
            },
        }
        KAFKA_CONSUMERS_MESSAGE_PROCESSORS = {
          'processor key': {
            'class': 'path.to.messageprocessor.Class',
            # Processors init arguments, e.g.
            'rsa_private_key_path': 'path/to/private/key'
          },
        }

        KAFKA_CONSUMER_SSL_SETTINGS = {
            'security_protocol': 'SSL',
            'ssl_cafile': '/path/to/file/ca.crt',
            'ssl_certfile': '/path/to/file/signed.request.crt',
            'ssl_keyfile': '/path/to/some/keyfile.key',
        }

5.  To continuously consume events from kafka run:

        python manage.py consume --supervised --topic topic_key

License
-------

The Django Kafka Consumer package is licensed under the [FreeBSD
License](https://opensource.org/licenses/BSD-2-Clause).
