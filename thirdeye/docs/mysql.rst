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

.. _mysql:

****************************
MySQL
****************************

This doc will help you understand how to add data sources to ThirdEye.
Please run through the **Quick Start** guide and shut down the frontend server process.

Prerequisites
######################

An accessible MySQL server containing data.


Data sources configuration
#####################################################

Add your MySQL database URL and credentials in `thirdeye-pinot/config/datasources/data-sources-config.yml`. You will be able to add multiple databases with multiple credentials, as follows:

.. code-block:: yaml

    dataSourceConfigs:
      - className: org.apache.pinot.thirdeye.datasource.sql.SqlThirdEyeDataSource
        properties:
          MySQL:
            - db:
                <dbname1>: jdbc:mysql://<db url1>
                <dbname2>: jdbc:mysql://<db url2>
              user: <username>
              password: <password>
             - db:
                <dbname3>: jdbc:mysql://<db url3>
                <dbname4>: jdbc:mysql://<db url4>
              user: <username2>
              password: <password2>

Note: the `dbname` here is an arbitrary name that you want to name it. 
In `dburl`, you still need to include the specific database you are using.

Here's an example below.

.. code-block:: yaml

  dataSourceConfigs:
    - className: org.apache.pinot.thirdeye.datasource.sql.SqlThirdEyeDataSource
      properties:
        MySQL:
          - db:
              dataset_pageviews: jdbc:mysql://localhost/dataset
            user: uthirdeye
            password: pass


Run ThirdEye frontend
####################################

Start the ThirdEye server.

.. code-block:: bash

    ./run-frontend.sh

Import metric from MySQL
####################################

The next step is to import metrics from your dataset into the ThirdEye system. Please see :ref:`import-sql-metric`.

Start an analysis
####################################

Point your favorite browser to


``http://localhost:1426/app/#/rootcause``


and type any data set or metric name (fragment) in the search box. Auto-complete will now list the names of matching metrics. Select any metric to start an investigation.