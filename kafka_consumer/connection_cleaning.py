from contextlib import contextmanager

from django.db import (
    close_old_connections,
    connection,
)


@contextmanager
def cleanup_db_connections():
    try:
        yield
    finally:
        connection.close()
        close_old_connections()
