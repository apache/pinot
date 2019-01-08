Table configuration format
==========================

Tables are defined in Pinot using a HOCON-based file format.
Here is an example of a table configuration in Pinot:

.. code-block:: javascript

  table {
    name: "myTable"
    types: [OFFLINE]
    replication: 3
    retention: "30 DAYS"
    schema {
      dimensions {
        myColumn {
          dataType=INT
          singleValue=true
        }
      }
    }
  }

This defines a simple table that has a single dimension column called ``myColumn``.

A sample table configuration that contains all the possible configuration options can be found in the Pinot distribution (TODO).

Useful HOCON features
---------------------

HOCON supports comments, file inclusion, variable expansion and inheritance, which can be helpful to manage table configurations across multiple
Pinot deployments.

Here is an example of these features:

``default-table.conf``

.. code-block:: javascript

  table {
    replication: 3
    retention: "30 DAYS"
  }
  config.kafkaHost="kafka-host-1234.us-west-4.cloudprovider.com:12345"

``my-table.conf``

.. code-block:: javascript

  #include "default-table.conf"
  table {
    name: "myTable"
    retention: "60 days" // Retention is overridden from the default 30 days
    // Replication is inherited to be 3
    schema {
      // ...
    }
    "streamConfigs.stream.kafka.broker.list": "${config.kafkaHost}"
  }

Configuration profiles
----------------------

Configuration profiles can also be used to simplify configuration management across different Pinot deployments. The Pinot
table configuration deployment tools optionally support passing in a list of configuration profiles, which are included at the top of the
table definition file. For example, passing in the ``prod`` and ``prod-us-west-4`` profiles is equivalent to
including ``profiles/prod.conf`` and ``profiles/prod-us-west-4.conf`` at the top of the table configuration.

Table definitions can also contain an optional ``profiles`` section which can be used to control automated table deployments.

``profiles/prod-us-west-4.conf``

.. code-block:: javascript

  config.kafkaHost="kafka-host-prod-1234.us-west-4.cloudprovider.com:12345"

``my-table.conf``

.. code-block:: javascript

  profiles=[prod, prod-us-west-4, test, test-us-east-2]
  table {
    // ...
    "streamConfigs.stream.kafka.broker.list": "${config.kafkaHost}"
  }

If the Pinot tools are used to deploy this table configuration with the profiles ``prod`` and ``prod-us-west-4``, this configuration
will get deployed to that particular Pinot deployment. However, the table would be skipped if the configuration profiles ``prod``
and ``prod-tokyo-1`` are passed, due to the ``prod-tokyo-1`` configuration profile not being part of the ``profiles`` section.

Profile conditionals
--------------------

Configuration values can be set based on which profiles are enabled. For example:

.. code-block:: javascript

  table {
    //...
    replication___prod = 3
    replication___testing = 1
  }

In the example above, the replication for the table will be 3 if the ``prod`` profile is enabled, 1 if the ``testing``
profile is enabled, and unset otherwise.

There is no conflict resolution between conflicting conditional values, so a configuration containing simultaneous
different values for profile conditionals is invalid.

.. code-block:: javascript

  table {
    // Invalid value for replication, it simultaneously has a value of 3 and 4
    // if the prod and prod-us-west-4 profiles are enabled at the same time
    replication___prod = 3
    replication___prod-us-west-4 = 4
  }

Different settings for realtime and offline
-------------------------------------------

Different settings for realtime and offline tables can be set by adding a ``offline`` or ``realtime`` suffix to the
property value. If not specified, the property value applies to both the offline and realtime counterparts of a table.

.. code-block:: javascript

  table {
    // ...
    "retention.offline": "90 days"
    "retention.realtime": "5 days"
  }

This can be combined with profile conditionals as follows:

.. code-block:: javascript

  table {
    // ...
    "retention.offline___testing": "14 days"
    "retention.offline___prod": "90 days"
    "retention.realtime": "5 days"
  }

Operations
==========

Deploying configurations
------------------------

Configurations can be deployed using the Pinot admin tools, as follows:

.. code-block:: bash

  pinot-admin.sh ApplyTableConfig -tableConfigFile my-table.conf -profile prod,prod-us-west-4 \
      -controllerUrl http://pinot-controller-proxy.us-west-4.cloudprovider.com:23456/

For batch automatic deployments, the table configuration file parameter can be a directory, which will recursively process all the table
definition files in that directory.

We recommend managing table configurations using a source control system and automating table deployments by running the
``ApplyTableConfig`` command on source control changes or periodically using a scheduler.

Backing up table configurations
-------------------------------

Table configurations can be backed up using the ``BackupTableConfigs`` command:

.. code-block:: bash

  pinot/bin/pinot-admin.sh BackupTableConfigs -tableName myTable \
      -controllerUrl http://pinot-controller-proxy.us-west-4.cloudprovider.com:23456/

Migrating from the legacy configuration format
----------------------------------------------

For single cluster Pinot deployments, migration is done by backing up all of the table configurations as shown above.

For multiple cluster Pinot deployments, an additional merge step needs to be done to minimize the number of table files.

1. Write all of the desired configuration properties in the ``profiles`` directory. For example, one might want to
   create ``profiles/prod-us-west-4.conf`` with Kafka broker addresses.

2. Back up all of the table configurations in a folder structure that contains the desired tags. For example, a table
   configuration in ``configBackup/prod/prod-us-west-4/myTable.conf`` will assume that the ``prod`` and
   ``prod-us-west-4`` tags apply to that configuration file during the merge step.

3. Merge all of the table configurations by using the ``MergeConfigs`` command, for example,
   ``pinot-admin.sh MergeConfigs -inputDir configBackup -outputDir mergedConfigs`` After the merge step, tables
   configurations for tables that have the same name will be merged together, suitable variable expansions will be
   substituted for configuration variables present in the configuration profiles and identical values will be collapsed
   across clusters.

Deploying a new table
---------------------

1. Create a table in the testing environment, as follows:

.. code-block:: javascript

  profiles = [testing, testing-us-central-1]
  table {
    name: "mytable"
    replication: 1
    // ...
  }

2. Commit the configuration and deploy it
3. Once satisfied with the table, it can be deployed to the production environment updating the table configuration file

.. code-block:: javascript

  profiles = [testing, testing-us-central-1, prod, prod-us-west-4]
  table {
    name: "mytable"
    replication___testing: 1
    replication___prod: 3
    // ...
  }

4. Commit the configuration and deploy it