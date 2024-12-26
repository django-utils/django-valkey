from django.core.exceptions import ImproperlyConfigured


class ConnectionInterrupted(Exception):
    def __init__(self, connection, parent=None):
        self.connection = connection

    def __str__(self) -> str:
        error_type = type(self.__cause__).__name__
        error_msg = str(self.__cause__)
        return f"Valkey {error_type}: {error_msg}"


class CompressorError(Exception):
    pass


class InvalidValkeyBackendError(ImproperlyConfigured):
    pass
