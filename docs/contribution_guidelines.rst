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

Contribution Guidelines
=======================

Before you begin to contribute, make sure you have reviewed :ref:`dev-setup` and :ref:`code-modules` sections.

Create an issue for the change
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Create a Pinot issue for the change you would like to make. Provide information on why the change is needed and how you
plan to address it. Use the conversations on the issue as a way to validate assumptions and the right way to proceed.

Once you have a good about what you want to do, proceed with the next steps.

Create a branch for your change
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Creating a PR
^^^^^^^^^^^^^

* Verifying code-style


Run the following command to verify the code-style before posting a PR

.. code-block:: none

    mvn checkstyle:check

* License Headers for newly added files
All source code files should have license headers. To automatically add the header for any new file you plan to checkin,
 run:

.. code-block:: none

    mvn license:format

.. note::

If you checkin third-party code or files, do not add license headers for them. Follow these instructions to ensure we
are compliant with Apache Licensing process. <TBD>

* Run tests

* Push changes and create a PR for review

* Addressing comments and Rebasing

* Once your change is merged, check to see if any documentation needs to be updated. If so, create a PR for documentation.
