# Compressor support

the default compressor class is `django_valkey.compressors.identity.IdentityCompressor`

this class doesn't compress the data, it only returns the data as is, but it works as a swappable placeholder if we want to compress data.

compressors have two common global settings and some of them have a number of their own.
the common settings are:
`CACHE_COMPRESS_LEVEL` which tells the compression tool how much compression to perform.
`CACHE_COMPRESS_MIN_LENGTH` which tells django-valkey how big a data should be to be compressed.

the library specific settings are listed in their respective sections, you should look at their documentations for more info.

### Brotli compression

to use the brotli compressor you need to install the `brotli` package first
you can do that with:

```shell
pip install django-valkey[brotli]
```

or simply

```shell
pip install brotli
```

to configure the compressor you should edit you settings files to look like this:

```python
CACHES = {
    "default": {
        # ...
        "OPTIONS": {
            "COMPRESSOR": "django_valkey.compressors.brotli.BrotliCompressor",
        }
    }
}
# optionally you can set compression parameters:
CACHE_COMPRESS_LEVEL = 11  # defaults to 11
CACHE_COMPRESS_MIN_LENGTH = 15  # defaults to 15
COMPRESS_BROTLI_LGWIN = 22  # defaults to 22
COMPRESS_BROTLI_LGBLOCK = 0  # defaults to 0
COMPRESS_BROTLI_MODE = "GENERIC"  # defaults to "GENERIC" other options are: ("GENERIC", "TEXT", "FONT")
```

*NOTE* the values shown here are only examples and *not* best practice or anything.

you can read more about this compressor at their [Documentations](https://pypi.org/project/Brotli/) (and source code, for this matter)

### Bz2 compression

the bz2 compression library comes in python's standard library, so if you have a normal python installation just configure it and it's done:

```python
CACHES = {
    "default": {
        # ...
        "OPTIONS": {
            "COMPRESSOR": "django_valkey.compressors.bz2.Bz2Compressor",
        }
    }
}
# optionally you can set compression parameters:
CACHE_COMPRESS_LEVEL = 9  # defaults to 9
CACHE_COMPRESS_MIN_LEVEL = 15  # defaults to 15
```

*NOTE* the values shown here are only examples and *not* best practice or anything.

### Gzip compression

the gzip compression library also comes with python's standard library, so configure it like this and you are done:

```python
CACHES = {
    "default": {
        # ...
        "OPTIONS": {
            "COMPRESSOR": "django_valkey.compressors.gzip.GzipCompressor",
        }
    }
}
# optionally you can set compression parameters:
CACHE_COMPRESS_LEVEL = 9  # defaults to 9
CACHE_COMPRESS_MIN_LEVEL = 15  # defaults to 15
```

*NOTE* the values shown here are only examples and *not* best practice or anything.


### Lz4 compression

to use the lz4 compression you need to install the lz4 package first:

```shell
pip install django-valkey[lz4]
```

or simply

```shell
pip install lz4
```

then you can configure it like this:

```python
CACHES = {
    "default": {
        # ...
        "OPTIONS": {
            "COMPRESSOR": "django_valkey.compressors.lz4.Lz4Compressor",
        }
    }
}
# optionally you can set compression parameters:
CACHE_COMPRESS_LEVEL = 0  # defaults to 0
CACHE_COMPRESS_MIN_LEVEL = 15  # defaults to 15

COMPRESS_LZ4_BLOCK_SIZE = 0  # defaults to 0
COMPRESS_LZ4_CONTENT_CHECKSUM = 0  # defaults to 0
COMPRESS_LZ4_BLOCK_LINKED = True  # defaults to True
COMPRESS_LZ4_STORE_SIZE = True  # defaults to True
```

*NOTE* the values shown here are only examples and *not* best practice or anything.


### Lzma compression

lzma compression library also comes with python's standard library

```python
CACHES = {
    "default": {
        # ...
        "OPTIONS": {
            "COMPRESSOR": "django_valkey.compressors.lzma.LzmaCompressor",
        }
    }
}
# optionally you can set compression parameters:
CACHE_COMPRESS_LEVEL = 9  # defaults to 4
CACHE_COMPRESS_MIN_LEVEL = 15  # defaults to 15

COMPRESS_LZMA_FORMAT = 1  # defaults to 1
COMPRESS_LZMA_CHECK = -1  # defaults to -1
COMPRESS_LZMA_FILTERS = None  # defaults to None

# optional decompression parameters
DECOMPRESS_LZMA_MEMLIMIT = None  # defaults to None (if you want to change this, make sure you read lzma docs about it's dangers)
DECOMPRESS_LZMA_FORMAT = 0  # defaults to 4
DECOMPERSS_LZMA_FILTERS = None  # defaults to None
```

*NOTE* the values shown here are only examples and *not* best practice or anything.

### Zlib compression

zlib compression library also comes with python's standard library

```python
CACHES = {
    "default": {
        # ...
        "OPTIONS": {
            "COMPRESSOR": "django_valkey.compressors.zlib.ZlibCompressor",
        }
    }
}
# optionally you can set compression parameters:
CACHE_COMPRESS_LEVEL = 9  # defaults to 6
CACHE_COMPRESS_MIN_LEVEL = 15  # defaults to 15

compress_zlib_wbits = 15  # defaults to 15  (NOTE: only available in python 3.11 and newer
```    

*NOTE* the values shown here are only examples and *not* best practice or anything.


### Zstd compression

to use zstd compression you need to have the pyzstd library installed

```shell
pip install django-valkey[pyzstd]
```

or simply

```shell
pip install pyzstd
```

then you can configure it as such:

```python
CACHES = {
    "default": {
        # ...
        "OPTIONS": {
            "COMPRESSOR": "django_valkey.compressors.zstd.ZStdCompressor",
        }
    }
}
# optionally you can set compression parameters:
CACHE_COMPRESS_LEVEL = 1  # defaults to 1
CACHE_COMPRESS_MIN_LEVEL = 15  # defaults to 15

# the below settings are all defaulted to None
COMPRESS_ZSTD_OPTIONS = {...}  # if you set this, `CACHE_COMPRESS_LEVEL` will be ignored.
DECOMPRESS_ZSTD_OPTIONS = {...}  # note: if you don't set this, the above one will be used.
COMPRESS_ZSTD_DICT = {...}
DECOMPRESS_ZSTD_DICT = {...}  # note: if you don't set this, the above one will be used.
```

*NOTE* the values shown here are only examples and *not* best practice or anything.