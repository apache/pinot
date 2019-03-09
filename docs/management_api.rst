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

Managing Pinot
==============

There are two ways to manage Pinot cluster, i.e. using Pinot management console and ``pinot-admin.sh`` script.

Pinot Management Console
------------------------

There is a REST API which allows management of tables, tenants, segments and schemas. It can be accessed by going to
``http://[controller host]/help`` which offers a web UI to do these tasks, as well as document the REST API.

For example, list all the schema within Pinot cluster:

  .. figure:: img/list-schemas.png

Upload a pinot segment:

  .. figure:: img/upload-segment.png


Pinot-admin.sh
--------------

It can be used instead of the ``pinot-admin.sh`` commands to automate the creation of tables and tenants.

For example, create a pinot segment:

  .. figure:: img/generate-segment.png

Query a table:

  .. figure:: img/query-table.png
