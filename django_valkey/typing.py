from valkey._parsers import _RESP2Parser, _RESP3Parser, _LibvalkeyParser

KeyT = int | float | str | bytes | memoryview


DefaultParserT = type[_RESP2Parser | _RESP3Parser | _LibvalkeyParser]
