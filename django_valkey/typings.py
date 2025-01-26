from valkey._parsers import (
    _RESP2Parser,
    _RESP3Parser,
    _LibvalkeyParser,
    _AsyncRESP2Parser,
    _AsyncRESP3Parser,
    _AsyncLibvalkeyParser,
)

KeyT = int | float | str | bytes | memoryview


DefaultParserT = type[_RESP2Parser | _RESP3Parser | _LibvalkeyParser]
AsyncDefaultParserT = type[
    _AsyncRESP2Parser | _AsyncRESP3Parser | _AsyncLibvalkeyParser
]
