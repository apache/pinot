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

.. _pluggable-storage:

Pluggable Storage
=================

Pinot enables its users to write a PinotFS abstraction layer to store data in a data layer of their choice for realtime and offline segments.

Some examples of storage backends(other than local storage) currently supported are:

* `HadoopFS <https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/FileSystemShell.html>`_
* `Azure Data Lake <https://azure.microsoft.com/en-us/solutions/data-lake/>`_
* `Google Cloud Storage <https://cloud.google.com/storage/>`_

GcsPinotFS
----------

  To configure the GcsPinotFS filesystem, the server and controller must be configured as shown below.
  The account associated with the ``GCP_KEY_FILE`` must be able to create and drop objects in the gcs bucket,
  for example if the account has the ``Storage Admin`` and ``Storage Object Admin`` roles for the bucket that will
  be sufficient.

  The ``CLASSPATH_PREFIX`` environment variable must also be set to point to the pinot-gcs-filesystem.jar in the server
  and controller.

Controller Config:

.. code-block:: none

    controller.helix.cluster.name=pinot
    controller.host=<pinot host>
    controller.port=9000
    controller.data.dir=gs://MY_BUCKET/MY_DIRECTORY/
    controller.local.temp.dir=MY_LOCAL_TEMP_DIRECTORY
    controller.zk.str=<ZOOKEEPER_HOST>:2181
    pinot.set.instance.id.to.hostname=true
    pinot.controller.storage.factory.class.gs=org.apache.pinot.filesystem.GcsPinotFS
    pinot.controller.storage.factory.gs.projectId=MY_PROJECT_ID
    pinot.controller.storage.factory.gs.gcpKey=GCP_KEY_FILE
    pinot.controller.segment.fetcher.protocols=file,http,gs
    pinot.controller.segment.fetcher.gs.class=org.apache.pinot.common.segment.fetcher.PinotFSSegmentFetcher


Server Config:

.. code-block:: none

    pinot.server.netty.port=8098
    pinot.server.adminapi.port=8097
    pinot.server.instance.dataDir=LOCAL_DATA_DIR
    pinot.server.instance.segmentTarDir=LOCAL_TAR_DIR
    pinot.set.instance.id.to.hostname=true
    pinot.server.storage.factory.class.gs=org.apache.pinot.filesystem.GcsPinotFS
    pinot.server.storage.factory.gs.projectId=MY_PROJECT_ID
    pinot.server.storage.factory.gs.gcpKey=GCP_KEY_FILE
    pinot.server.segment.fetcher.protocols=file,http,gs
    pinot.server.segment.fetcher.gs.class=org.apache.pinot.common.segment.fetcher.PinotFSSegmentFetcher

If the above three filesystems do not meet your needs, you can extend the current `PinotFS <https://github.com/apache/incubator-pinot/blob/master/pinot-common/src/main/java/org/apache/pinot/filesystem/PinotFS.java>`_ to customize for your needs.

New Storage Type implementation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
In order to add a new type of storage backend (say, Amazon s3) implement the following class:

S3FS extends `PinotFS <https://github.com/apache/incubator-pinot/blob/master/pinot-common/src/main/java/org/apache/pinot/filesystem/PinotFS.java>`_


Configurations for Realtime Tables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The example here uses the existing org.apache.pinot.filesystem.HadoopPinotFS to store realtime segments in a HDFS filesytem. In the Pinot controller config, add the following new configs:

.. code-block:: none

    "controller.data.dir": "SET_TO_YOUR_HDFS_ROOT_DIR"
    "controller.local.temp.dir": "SET_TO_A_LOCAL_FILESYSTEM_DIR" 
    "pinot.controller.storage.factory.class.hdfs": "org.apache.pinot.filesystem.HadoopPinotFS"
    "pinot.controller.storage.factory.hdfs.hadoop.conf.path": "SET_TO_YOUR_HDFS_CONFIG_DIR"
    "pinot.controller.storage.factory.hdfs.hadoop.kerberos.principle": "SET_IF_YOU_USE_KERBEROS"
    "pinot.controller.storage.factory.hdfs.hadoop.kerberos.keytab": "SET_IF_YOU_USE_KERBEROS"
    "controller.enable.split.commit": "true"

In the Pinot controller config, add the following new configs:

.. code-block:: none

    "pinot.server.instance.enable.split.commit": "true"
    
    
Note: currently there is a bug in the controller (`issue <https://github.com/apache/incubator-pinot/issues/3847>`), for now you can cherrypick the PR https://github.com/apache/incubator-pinot/pull/3849 to fix the issue as tested already. The PR is under review now.

Configurations for Offline Tables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
These properties for the stream implementation are to be set in your controller and server configurations.

In your controller and server configs, please set the FS class you would like to support. pinot.controller.storage.factory.class.${YOUR_URI_SCHEME} to the full path of the FS class you would like to include

You also need to configure pinot.controller.local.temp.dir for the local dir on the controller machine.

For filesystem specific configs, you can pass in the following with either the pinot.controller prefix or the pinot.server prefix.

All the following configs need to be prefixed with storage.factory.

AzurePinotFS requires the following configs according to your environment:

adl.accountId, adl.authEndpoint, adl.clientId, adl.clientSecret

Sample Controller Config

.. code-block:: none

    "pinot.controller.storage.factory.class.adl": "org.apache.pinot.filesystem.AzurePinotFS"
    "pinot.controller.storage.factory.adl.accountId": "xxxx"
    "pinot.controller.storage.factory.adl.authEndpoint": "xxxx"
    "pinot.controller.storage.factory.adl.clientId": "xxxx"
    "pinot.controller.storage.factory.adl.clientId": "xxxx"
    "pinot.controller.segment.fetcher.protocols": "adl"


Sample Server Config

.. code-block:: none

    "pinot.server.storage.factory.class.adl": "org.apache.pinot.filesystem.AzurePinotFS"
    "pinot.server.storage.factory.adl.accountId": "xxxx"
    "pinot.server.storage.factory.adl.authEndpoint": "xxxx"
    "pinot.server.storage.factory.adl.clientId": "xxxx"
    "pinot.server.storage.factory.adl.clientId": "xxxx"
    "pinot.server.segment.fetcher.protocols": "adl"


You can find the parameters in your account as follows:
https://stackoverflow.com/questions/56349040/what-is-clientid-authtokenendpoint-clientkey-for-accessing-azure-data-lake

Please also make sure to set the following config with the value "adl"

.. code-block:: none

  "segment.fetcher.protocols" : "adl"


To see how to upload segments to different storage systems, check
:file:`../segment_fetcher.rst`.

HadoopPinotFS requires the following configs according to your environment:

hadoop.kerberos.principle, hadoop.kerberos.keytab, hadoop.conf.path

Please make sure to also set the following config with the value "hdfs"

.. code-block:: none

  "segment.fetcher.protocols" : "hdfs"


