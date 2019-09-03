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

.. _import-sql-metric:

Import metric from Presto/MySQL
==================================

**0: Prerequisites**

Run through step 1-2 in :ref:`presto`. or :ref:`mysql`.

**1: Import Metric on Front End**

Click on this link to import: ``http://localhost:1426/app/#/self-serve/import-sql-metric``

Once the UI is fixed, this link should appear in the create alert page.

Fill in the form which includes the following fields, and click Import Metrics.
 
``Table Name``: For Presto, it is the Presto table name, including all schema prefixes. For MySQL it is just the table name.

``Time column``: Column name that contains the time.

``Timezone``: Timezone of the time column.

``Time Format``: Format of the time column.

``Time Granularity``: The granularity of your metric. For example, daily data should choose 1DAYS. 
Hourly data should choose 1HOURS.

``Dimensions``: Add dimensions and fill in the name of the dimension

``Metrics``: Add metrics and fill in the name and the aggregation method on the dimension when it is being aggregated by time.

For example:

.. image:: https://user-images.githubusercontent.com/11586489/56252038-cb974880-606a-11e9-9213-a06bfa533826.png

**2: Done!**

The data set name will be ``[source name].[db name].[table name]``. For example, a ThirdEye monitoring metric data set may be named ``MySQL.te.merged_anomaly_index_result``.
And the metric name is just the metric name you set.

Note that this page does not validate that your entry is correct. Try on the Root Cause Analysis page whether you can see the
metric showing up. If not, please retry entering the form again and the previous entry will be overwritten.
