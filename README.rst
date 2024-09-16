===============================
Valkey cache backend for Django
===============================


Introduction
------------

django-valkey is a BSD licensed, full featured Valkey cache and session backend
for Django.

this project is a fork of the wonderful `django-redis <https://github.com/jazzband/django-redis>`_ project.
they wrote all the good codes.

Why use django-valkey?
~~~~~~~~~~~~~~~~~~~~~~

- Valkey is a free licenced and well maintained key/value database
- Uses native valkey-py url notation connection strings
- Pluggable clients
- Pluggable parsers
- Pluggable serializers
- Primary/secondary support in the default client
- Comprehensive test suite
- Used in production in several projects as cache and session storage
- Supports infinite timeouts
- Facilities for raw access to Valkey client/connection pool
- Highly configurable (can emulate memcached exception behavior, for example)
- Unix sockets supported by default

Requirements
~~~~~~~~~~~~

- `Python`_ 3.10+
- `Django`_ 3.2.9+
- `valkey-py`_ 6.0.0+ (probably works on older versions too)
- `Valkey server`_ 7.2.6+ (probably works with older versions too)

.. _Python: https://www.python.org/downloads/
.. _Django: https://www.djangoproject.com/download/
.. _valkey-py: https://pypi.org/project/valkey/
.. _Valkey server: https://valkey.io/download

User guide
----------

Documentation
~~~~~~~~~~~~~
check out our `Docs <https://django-valkey.readthedocs.io/en/latest/>`_ for a complete explanation

Installation
~~~~~~~~~~~~

Install with pip:

.. code-block:: console

    python -m pip install django-valkey

Install with c bindings:

.. code-block:: console

    python -m pip install django-valkey[libvalkey]

Install 3rd party compression

.. code-block:: console

    python -m pip install django-valkey[lz4]

.. code-block:: console

    python -m pip install django-valkey[pyzstd]

.. code-block:: console

    python -m pip install django-valkey[brotli]





Contribution
~~~~~~~~~~~~
contribution rules are like other projects,being respectful and keeping the ethics.
also make an issue before going through troubles of coding, someone might already be doing what you want to do.


Todo
~~~~
1. check type hints to be accurate
2. refactor spaghetti codes
3. review the documentations
4. work on cluster client
5. for more async client

License
-------

.. code-block:: text

    Copyright (v) 2024 Amirreza Sohrabi far
    Copyright (c) 2011-2016 Andrey Antukh <niwi@niwi.nz>
    Copyright (c) 2011 Sean Bleier

    All rights reserved.

    Redistribution and use in source and binary forms, with or without
    modification, are permitted provided that the following conditions
    are met:
    1. Redistributions of source code must retain the above copyright
       notice, this list of conditions and the following disclaimer.
    2. Redistributions in binary form must reproduce the above copyright
       notice, this list of conditions and the following disclaimer in the
       documentation and/or other materials provided with the distribution.
    3. The name of the author may not be used to endorse or promote products
       derived from this software without specific prior written permission.

    THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS`` AND ANY EXPRESS OR
    IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
    OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
    IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
    INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
    NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
    DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
    THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
    (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
    THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
