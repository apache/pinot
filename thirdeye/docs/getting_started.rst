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

.. _quick-start:

****************************
Getting Started
****************************
This document will help you set up ThirdEye(abbreviated as 'TE') with an external MySQL persistence layer.
It will also demonstrate how to plugin your own datasets from custom datasources.

Prerequisites
######################

You'll need Java 8+, Maven 3.6+, and NPM 3.10+

.. warning:: On MacOS, Java 8 is the recommended version. Higher versions of JDK including Java 9 and Java 14 are known to have issues.


Building ThirdEye from source
##########################################
Simply clone the repo and build using the set of commands below.

.. code-block:: bash

    git clone https://github.com/apache/incubator-pinot.git
    cd incubator-pinot/thirdeye
    chmod +x install.sh run-frontend.sh run-backend.sh reset.sh
    ./install.sh

.. note:: The build of thirdeye-frontend may take several minutes


Configuration
##########################################
ThirdEye is extremely flexible in terms of storage and working with different persistence layers and
data sources. In this document, we'll set it up using MySQL as the main persistence layer.

By default, ThirdEye assumes ``./config`` as the main config directory relative to the current
working dir. The configurations use the ``YAML`` file format.

MySQL Persistence
***********************

.. note:: This section assumes that you have a running MySQL server available with admin privileges.

Step 1. Create a Database for ThirdEye
===========================================

.. code-block:: sql

  -- Create DB
  CREATE DATABASE thirdeye_test
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_unicode_ci;

Step 2. Setup users with appropriate privileges.
=======================================================

This assumes that you have an admin user and an
app user. The app user is more restrictive in terms of its privileges. You can also create
a single user to do all operations.

.. code-block:: sql

  -- Create admin user
  CREATE USER 'uthirdeyeadmin'@'%' IDENTIFIED BY 'pass';
  GRANT ALL PRIVILEGES ON thirdeye_test.* TO 'uthirdeyeadmin'@'%' WITH GRANT OPTION;

  -- Create app user
  CREATE USER 'uthirdeye'@'%' IDENTIFIED BY 'pass';
  GRANT SELECT, INSERT, UPDATE, DELETE, EXECUTE ON thirdeye_test.* TO 'uthirdeye'@'%';


Step 3. Create the tables in the ThirdEye Database
=======================================================

Login to your MySQL database and run the script below.

.. code-block:: bash

  mysql -h localhost -u uthirdeye thirdeye_test -p < thirdeye-pinot/src/main/resources/schema/create-schema.sql


Step 4. Update the persistence config file
=======================================================
ThirdEye stores it's persistence config in the file below.

.. code-block:: bash

    ./config/persistence.yml

For demo purposes, TE uses an in memory H2 db by default. To use MySQL, change the file contents
with the one shown below.

.. code-block:: yaml

  databaseConfiguration:
    # Assuming a local MySQL server running on the default port 3306
    url: jdbc:mysql://localhost/thirdeye_test?autoReconnect=true
    user: uthirdeye
    password: pass
    driver: com.mysql.jdbc.Driver

All set! ThirdEye is now configured to use MySQL as the persistence layer.

Running ThirdEye
##########################################

You can use the command below to run ThirdEye assuming your working dir to be ``./thirdeye``

.. code-block:: bash

    ./run-frontend.sh

.. note:: You can stop the ThirdEye dashboard server anytime by pressing **Ctrl+C** in the terminal


Creating an Application
##########################################

See :ref:`application`. We'll be using this application when creating alerts.

Setting up Alerts
##########################################

You can set up alerts and do root cause analysis on this application. See more at :ref:`alert-setup`.