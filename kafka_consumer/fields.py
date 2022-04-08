"""Custom field for django models"""
from base64 import encodebytes as b64encode
from base64 import decodebytes as b64decode

from django.db import models
from django.utils.encoding import force_text


class Base64TextField(models.TextField):

    """A field that will accept python object and store it in the database as base64."""

    def to_python(self, value):
        """Decode value with base64

        :param value: value to decode
        :type value: str or unicode
        :return: decoded value
        :rtype: str or unicode
        """
        if value is not None:
            value = b64decode(value)
        return value

    def get_db_prep_value(self, value, connection=None, prepared=False):
        """Encode value with base64

        :param value: value to encode
        :type value: str or unicode
        :return: encoded value
        :rtype: str or unicode
        """
        if value is not None:
            value = force_text(b64encode(value))
        return value

    def from_db_value(self, value, expression, connection, context):  # pylint: disable=unused-argument
        """Convert value from database to python object

        :param value: value from database
        :type value: str or unicode
        :return: value convert to python
        :rtype: str or unicode
        """
        return self.to_python(bytes(value.encode()))
