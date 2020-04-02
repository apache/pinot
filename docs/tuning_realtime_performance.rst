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

.. warning::  The documentation is not up-to-date and has moved to `Apache Pinot Docs <https://docs.pinot.apache.org/>`_.

Tuning Realtime Performance
===========================

See the section on :ref:`ingesting-realtime-data` before reading this section.

Pinot servers ingest rows into a consuming segment that resides in volatile memory.
Therefore, pinot servers hosting consuming segments tend to be memory bound.
They may also have long garbage collection cycles when the segment is completed
and memory is released.

Controlling memory allocation
-----------------------------

You can configure pinot servers to use off-heap memory for dictionary and forward
indices of consuming segments by setting the value of ``pinot.server.instance.realtime.alloc.offheap``
to ``true``.  With this configuration in place, the server allocates off-heap memory by memory-mapping
files. These files are never flushed to stable storage by Pinot (the Operating System may do so depending
on demand for memory on the host). The files are discarded when the consuming segment is turned into
a completed segment.

By default the files are created under the directory where the table's segments are stored
in local disk attached to the consuming server.
You can set a specific directory for consuming segments with the configuration 
``pinot.server.consumerDir``.  Given that there is no control over flushing of 
pages from the memory mapped for consuming segments, you may want to set the directory
to point to a memory-based file system, eliminating wasteful disk I/O.

If memory-mapping is not desirable, you can set ``pinot.server.instance.realtime.alloc.offheap.direct``
to ``true``. In this case, pinot allocates direct
`ByteBuffer <https://docs.oracle.com/javase/7/docs/api/java/nio/ByteBuffer.html>`_ objects for 
consuming segments. Using direct allocation can potentially result in address space fragmentation.

.. note::

   We still use heap memory to store inverted indices for consuming segments.


Controlling number of rows in consuming segment
-----------------------------------------------

The number of rows in a consuming segment needs to be balanced. Having too many rows can result in 
memory pressure. On the other hand, having too few rows results in having too many small segments.
Having too many segments can be detrimental to query performance, and also increase pressure on the Helix.

The recommended way to do this is to use the ``realtime.segment.flush.desired.size`` setting as described in
:ref:`stream-config-description`. You can run the administrative tool ``pinot-admin.sh RealtimeProvisioningHelper``
that will help you to come up with an optimal setting for the segment size.

Moving completed segments to different hosts
--------------------------------------------

This feature is avaialble only if the consumption type is ``LowLevel``.

The structure of the consuming segments and the completed segments are very different. The memory, CPU, I/O
and GC characteristics could be very different while processing queries on these segments. Therefore it may be
useful to move the completed segments onto differnt set of hosts in some use cases.

You can host completed segments on a different set of hosts using the ``tagOverrideConfig`` as described in 
:ref:`table-config-section`. Pinot will automatically move them once the consuming segments are completed.

Controlling segment build vs segment download on Realtime servers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This feature is available only if the consumption type is ``LowLevel``.

When a realtime segment completes, a winner server is chosen as a committer amongst all replicas by the controller. That committer builds the segment and uploads to the controller. The non-committer servers are asked to catchup to the winning offset. If the non-committer servers are able to catch up, they are asked to build the segment and replace the in-memory segment. If they are unable to catchup, they are asked to download the segment from the controller.

Building a segment can cause excessive garbage and may result in GC pauses on the server.
Long GC pauses can affect query processing. In order to avoid this, we have a configuration
that allows you to control whether 

It might become desirable to force the non-committer servers to download the segment from the controller, instead of building it again. The ``completionConfig`` as described in :ref:`table-config-section` can be used to configure this.

Fine tuning the segment commit protocol
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This feature is available only if the consumption type is ``LowLevel``.

Once a committer is asked to commit the segment, it builds a segment, and issues an HTTP POST to the controller, with the segment.
The controller than commits the segment in Zookeeper and starts the next consuming segment.

It is possible to conifigure the servers to do a *split* commit, in which the committer performs the following steps:

    * Build the segment
    * Start a transaction with the lead controller to commit the segment (CommitStart phase)
    * Post the completed segment to any of the controllers (and the controller posts it to segment store)
    * End the transaction with the lead controller (CommentEnd phase). Optionally, this step can be done *with* the segment metadata.

This method of committing can be useful if the network bandwidth on the lead controller is limiting segment uploads.

In order to accomplish this, you will need to set the following configurations:
   * On the controller, set ``pinot.controller.enable.split.commit`` to ``true`` (default is ``false``).
   * On the server, set ``pinot.server.enable.split.commit`` to ``true`` (default is ``false``). 
   * On the server, set ``pinot.server.enable.commitend.metadata`` to ``true`` (default is false).
