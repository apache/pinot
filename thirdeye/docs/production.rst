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

.. _production:

Production Settings
====================

ThirdEye relies on a central meta data store to coordinate its workers and frontend processes. The first step towards moving ThirdEye into production should therefore be the setup of a dedicated (MySQL) database instance. You can use the ``thirdeye-pinot/src/main/resources/schema/create-schema.sql`` script to create your tables. Then, update the ``thirdeye-pinot/config/persistence.yml`` file with path and credentials. Once you have a dedicated database instance, you can run backend and frontend servers in parallel. You can do it by running ``./run-frontend.sh`` and ``./run-backend.sh`` together, or use your favorite IDE to run them.

The next step could be the configuration of the holiday auto-loader. The holiday auto loader connects to the Google Calendar API. Once you obtain an API token, place it in ``thirdeye-pinot/config/holiday-loader-key.json`` and in ``thirdeye-pinot/config/detector.yml`` set holidayEventsLoader: true. Once the backend worker is restarted, it will periodically update the local cache of holiday events for ThirdEye's detection and Root-Cause Analysis components.
