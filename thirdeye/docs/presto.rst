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

.. _presto:

Presto
======

**0: Prerequisites**

Run through the **Quick Start** guide and shut down the frontend server process.


**1: Update the data sources configuration**

Add your MySQL database URL and credentials in `thirdeye-pinot/config/datasources/data-sources-config.yml`. You will be able to add multiple databases with multiple credentials, as follows:

.. code-block:: yaml

    dataSourceConfigs:
      - className: org.apache.pinot.thirdeye.datasource.sql.SqlThirdEyeDataSource
        properties:
          Presto:
            - db:
                <dbname1>: jdbc:presto://<db url1>
                <dbname2>: jdbc:presto://<db url2>
              user: <username>
              password: <password>
             - db:
                <dbname3>: jdbc:presto://<db url3>
                <dbname4>: jdbc:presto://<db url4>
              user: <username2>
              password: <password2>

Note: the `dbname` here is an arbitrary name that you want to name it. 
In `dburl`, you still need to include the specific database you are using.

**2: Run ThirdEye frontend**

.. code-block:: bash

    ./run-frontend.sh

**3: Import metric from Presto**

See :ref:`import-sql-metric`.

**4: Start an analysis**

Point your favorite browser to

``http://localhost:1426/app/#/rootcause``

and type any data set or metric name (fragment) in the search box. Auto-complete will now list the names of matching metrics. Select any metric to start an investigation.