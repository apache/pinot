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

.. _segment-fetcher:

Segment Fetchers
================

When pinot segment files are created in external systems (hadoop/spark/etc), there are several ways to push those data to pinot Controller and Server:

#. push segment to shared NFS and let pinot pull segment files from the location of that NFS.
#. push segment to a Web server and let pinot pull segment files from the Web server with http/https link.
#. push segment to HDFS and let pinot pull segment files from HDFS with hdfs location uri.
#. push segment to other system and implement your own segment fetcher to pull data from those systems.

The first two options should be supported out of the box with pinot package. As long your remote jobs send Pinot controller with the corresponding URI to the files it will pick up the file and allocate it to proper Pinot Servers and brokers. To enable Pinot support for HDFS, you will need to provide Pinot Hadoop configuration and proper Hadoop dependencies.

HDFS segment fetcher configs
-----------------------------

In your Pinot controller/server configuration, you will need to provide the following configs:

.. code-block:: none

    pinot.controller.segment.fetcher.hdfs.hadoop.conf.path=`<file path to hadoop conf folder>


or

.. code-block:: none


    pinot.server.segment.fetcher.hdfs.hadoop.conf.path=`<file path to hadoop conf folder>


This path should point the local folder containing ``core-site.xml`` and ``hdfs-site.xml`` files from your Hadoop installation

.. code-block:: none

    pinot.controller.segment.fetcher.hdfs.hadoop.kerberos.principle=`<your kerberos principal>
    pinot.controller.segment.fetcher.hdfs.hadoop.kerberos.keytab=`<your kerberos keytab>

or

.. code-block:: none

    pinot.server.segment.fetcher.hdfs.hadoop.kerberos.principle=`<your kerberos principal>
    pinot.server.segment.fetcher.hdfs.hadoop.kerberos.keytab=`<your kerberos keytab>


These two configs should be the corresponding Kerberos configuration if your Hadoop installation is secured with Kerberos. Please check Hadoop Kerberos guide on how to generate Kerberos security identification.

You will also need to provide proper Hadoop dependencies jars from your Hadoop installation to your Pinot startup scripts.

Push HDFS segment to Pinot Controller
-------------------------------------

To push HDFS segment files to Pinot controller, you just need to ensure you have proper Hadoop configuration as we mentioned in the previous part. Then your remote segment creation/push job can send the HDFS path of your newly created segment files to the Pinot Controller and let it download the files.

For example, the following curl requests to Controller will notify it to download segment files to the proper table:

.. code-block:: none

  curl -X POST -H "UPLOAD_TYPE:URI" -H "DOWNLOAD_URI:hdfs://nameservice1/hadoop/path/to/segment/file.gz" -H "content-type:application/json" -d '' localhost:9000/v2/segments

Implement your own segment fetcher for other systems
----------------------------------------------------

You can also implement your own segment fetchers for other file systems and load into Pinot system with an external jar. All you need to do is to implement a class that extends the interface of `SegmentFetcher <https://github.com/apache/incubator-pinot/blob/master/pinot-common/src/main/java/org/apache/pinot/common/segment/fetcher/SegmentFetcher.java>`_ and provides config to Pinot Controller and Server as follows:

.. code-block:: none

    pinot.controller.segment.fetcher.`<protocol>`.class =`<class path to your implementation>

or

.. code-block:: none

    pinot.server.segment.fetcher.`<protocol>`.class =`<class path to your implementation>

You can also provide other configs to your fetcher under config-root ``pinot.server.segment.fetcher.<protocol>``

