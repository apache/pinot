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

.. _bigquery:

****************************
BigQuery
****************************

The BigQuery connectors uses the JDBC connector for BigQuery.
See https://cloud.google.com/bigquery/providers/simba-drivers.
The connector was tested with the JDBC driver `1.2.4`.

WARNING: Using ThirdEye with BigQuery can incur some cost.
TIPS:
1. Use partitioned table, and use the _partitiontime as the time column in the ThirdEye metric.
See https://cloud.google.com/bigquery/docs/partitioned-tables.
2. Reserve slots to avoid on-demand query pricing.
See https://cloud.google.com/bigquery/docs/reservations-intro.

**0: Prerequisites**
Rebuild ThirdEye with the specific resources for bigquery:

.. code-block:: bash

    ./install.sh bigquery


**1: Update the data sources configuration**

Add your BigQuery database URL and credentials in `thirdeye-pinot/config/datasources/data-sources-config.yml`. You will be able to add multiple databases with multiple credentials, as follows:

.. code-block:: yaml

    dataSourceConfigs:
      - className: org.apache.pinot.thirdeye.datasource.sql.SqlThirdEyeDataSource
        properties:
          BigQuery:
            - db:
                dbname1: jdbc:bigquery://https://www.googleapis.com/bigquery/v2;ProjectId=PROJECT_ID1;OAuthType=1;
                dbname2: jdbc:bigquery://https://www.googleapis.com/bigquery/v2;ProjectId=PROJECT_ID2;OAuthType=0;OAuthServiceAcctEmail=thirdeye-sa@project.iam.gserviceaccount.com;OAuthPvtKeyPath=/path/to/thirdeye-sa.json;
              driver: com.simba.googlebigquery.jdbc42.Driver

See how to build the BigQuery url here:
https://www.simba.com/products/BigQuery/doc/JDBC_InstallGuide/content/jdbc/bq/using/connectionurl.htm
You can use different authentication methods ``OAuthType``, see:
https://www.simba.com/products/BigQuery/doc/JDBC_InstallGuide/content/jdbc/bq/options/oauthtype.htm

Note: the `dbname` here is an arbitrary name. For BigQuery you will most likely use something that will recall the project running the BigQuery jobs.


**2: Run ThirdEye frontend**

.. code-block:: bash

    ./run-frontend.sh

**3: Import metric from BigQuery**

See :ref:`import-sql-metric`.

Specificities of BigQuery:
``Table Name``: Use back tick, eg: ``\`project_id.dataset_id.table_name\```
You can also unnest in the table name, eg: ``\`project_id.dataset_id.table_name\`,  unnest(repeated_field) as r``

Example of time configuration:
``Time column``:  ``UNIX_SECONDS(TIMESTAMP(DATETIME(_PARTITIONTIME)))``
``Timezone``: ``UTC``
``Time Format``: ``EPOCH``
``Time Granularity``: ``1 SECONDS``


**4: Start an analysis**

Point your favorite browser to

``http://localhost:1426/app/#/rootcause``

and type any data set or metric name (fragment) in the search box. Auto-complete will now list the names of matching metrics. Select any metric to start an investigation.