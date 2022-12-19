Arcana Extension - TODO
=======================
.. .. image:: https://github.com/arcanaframework/arcana-TODO/actions/workflows/tests.yml/badge.svg
..    :target: https://github.com/arcanaframework/arcana-TODO/actions/workflows/tests.yml
.. .. image:: https://codecov.io/gh/arcanaframework/arcana-TODO/branch/main/graph/badge.svg?token=UIS0OGPST7
..    :target: https://codecov.io/gh/arcanaframework/arcana-TODO
.. image:: https://readthedocs.org/projects/arcana/badge/?version=latest
  :target: http://arcana.readthedocs.io/en/latest/?badge=latest
  :alt: Documentation Status


This is a template repository for extensions to the Arcana_ framework. Please adapt it
to provide your own extension to the Arcana_ framework.

After creating a new extension repository from this template, firstly do a global
search for "TODO" and replace it with the name of your package to update the package
settings. Also, update the author and maintainer tags in the "[project]" Section of the
the ``pyproject.toml``.

The extension defines 5 optional sub-packages that should be renamed from *todo* to the
name of your extension package:

* arcana.analysis.*todo*
* arcana.cli.*todo*
* arcana.data.*todo*
* arcana.deploy.*todo*
* arcana.utils.*todo*

Typically an extension package will either implement new analysis classes under
``arcana.analysis.<your-extension>``, or a classes and commands required to connect
and/or deploy pipelines to a new type of data store (e.g. XNAT) under
``arcana.cli.<your-extension>``, ``arcana.data.<your-extension>`` and
``arcana.deploy.<your-extension>``, respectively. ``arcana.utils.<your-extension>``
can be used to put any utility functions, noting that the version of the extension
will be written to ``arcana.utils.<your-extension>._version.py``.

The extension is configured in the ``pyproject.toml`` file. If you omit one of the
potential extension points (i.e. "analysis", "data", "deploy" or "utils"), you should
remove it from the ``namespace_packages`` setting in the "[tool.flit.metadata] Section".


Quick Installation
------------------

This extension can be installed for Python 3 using *pip*::

    $ pip3 install arcana-TODO

This will also install the core Arcana_ package and any required dependencies.

License
-------

This work is licensed under a
`Creative Commons Attribution 4.0 International License <http://creativecommons.org/licenses/by/4.0/>`_

.. image:: https://i.creativecommons.org/l/by/4.0/88x31.png
  :target: http://creativecommons.org/licenses/by/4.0/
  :alt: Creative Commons Attribution 4.0 International License



.. _Arcana: http://arcana.readthedocs.io
