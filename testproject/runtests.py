# -*- coding: utf-8 -*-
import logging
import os
import sys

test_dir = os.path.dirname(__file__)
sys.path.insert(0, test_dir)


def run_tests(*args):
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "testproject.settings")
    try:
        from django.core.management import execute_from_command_line
    except ImportError:
        # The above import may fail for some other reason. Ensure that the
        # issue is really that Django is missing to avoid masking other
        # exceptions on Python 2.
        try:
            import django
        except ImportError:
            raise ImportError(
                "Couldn't import Django. Are you sure it's installed and "
                "available on your PYTHONPATH environment variable? Did you "
                "forget to activate a virtual environment?"
            )
        raise
    test_args = list(args) or ['kafka_consumer']
    logging.disable(logging.CRITICAL)  # mute logger during tests
    sys.exit(execute_from_command_line(['manage.py', 'test'] + test_args))
    logging.disable(logging.NOTSET)


if __name__ == '__main__':
    args = sys.argv[1:]
    run_tests(*args)
