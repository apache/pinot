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

Managing Pinot via REST API on the Controller
=============================================

*TODO* : Remove this section altogether and find a place somewhere for a pointer to the management API. Maybe in the 'Running pinot in production' section?

There is a REST API which allows management of tables, tenants, segments and schemas. It can be accessed by going to ``http://[controller host]/help`` which offers a web UI to do these tasks, as well as document the REST API.

It can be used instead of the ``pinot-admin.sh`` commands to automate the creation of tables and tenants.
