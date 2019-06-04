..
.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at
..
..   http://www.apache.org/licenses/LICENSE-2.0
..
.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.
..

.. _contribute-datasource:

Didn't Find the Data Source You Want? Contribute!
==================================================

With the interfaces ThirdEye provide, it is not too difficult to add a new data source.
Please refer to SQL and Pinot data source code ``sql/`` and ``pinot/`` under ``thirdeye-pinot/config/data-sources``.
Contributions are highly welcomed. If you have any question, feel free to contact us.

If it is a SQL based data source and can be connected via JDBC
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Under ``thirdeye-pinot/src/main/java/org/apache/pinot/thirdeye/datasource/sql/``:

In ``SqlResponseCacheLoader.java``, refer to Presto and MySQL code and add relevent code for the loader.
In ``SqlUtils.java``, you may need to change some SQL queries according to your data source.

That's it! Then you can add corresponding database config to ``thirdeye-pinot/config/data-sources/data-sources-config.yml``, and see your database showing up
on :ref:`import-sql-metric` page and import it the same way.

If it is not SQL based
~~~~~~~~~~~~~~~~~~~~~~~~~~

One interface is required to be implemented: ``ThirdEyeDataSource``:

For ``ThirdEyeDataSource``, ``execute`` is the most important function to implement, which returns a ThirdEyeResponse.
The ThirdEyeResponse can be built using RelationalThirdEyeResponse.


``CacheLoader`` is highly recommended to be used by ``ThirdEyeDataSource`` to improve performance. To learn more, please refer to our existing code for Pinot and SQL,
or learn more at `CacheLoader (Google Core Libraries for Java) <https://google.github.io/guava/releases/15.0/api/docs/com/google/common/cache/CacheLoader.html>`_.

Again, Please refer to SQL and Pinot data source code ``sql/`` and ``pinot/`` under ``thirdeye-pinot/config/data-sources``.