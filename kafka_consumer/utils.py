import struct

from django.utils.module_loading import import_string

from kafka_consumer import settings


def get_class_path(cls):
    """Return class path

    :param cls: class object
    :return: class path
    :rtype: str
    """
    return '{cls.__module__}.{cls.__name__}'.format(cls=cls)


def cascade_unpack(cascade_formats, seralized_data):
    """Example:
    {'fmt': 'HHI', 'export': ('proto_version', 'data_type_version', 'msg2_str_len',)},
    {'fmt': '{msg2_str_len}sQqI', 'export': ('msg2_str', 'msg0', 'msg1', 'aes_key_len',)},
    {'fmt': '{aes_key_len}sII', 'export': ('aes_key', 'crc32', 'ciphertext_len',)},
    {'fmt': '{ciphertext_len}s', 'export': ('ciphertext',)},
    """
    result = {}

    rest = seralized_data
    for fmt_chunk in cascade_formats:
        fmt = '!' + fmt_chunk['fmt'].format(**result)
        size = struct.calcsize(fmt)
        values = struct.unpack(fmt, rest[:size])
        fields = dict(zip(fmt_chunk['export'], values))
        result.update(fields)
        rest = rest[size:]
    return result


def get_message_batch_processor_class():
    return import_string(settings.KAFKA_CONSUMER_MESSAGE_BATCH_PROCESSOR_CLASS)
