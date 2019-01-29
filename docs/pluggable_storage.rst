Pluggable Storage
=================

Pinot enables its users to write a PinotFS abstraction layer to store data in a source of truth data layer of their
choice for offline segments. We do not yet have support for realtime consumption in deep storage.

Some examples of storage backends(other than local storage) currently supported are:

* `HadoopFS <https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/FileSystemShell.html>`_
* `Azure Data Lake <https://azure.microsoft.com/en-us/solutions/data-lake/>`_

If the above two filesystems do not meet your needs, please feel free to get in touch with us,
and we can help you out.

New Storage Type implementation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
In order to add a new type of storage backend (say, Amazon s3) implement the following class:

#. S3FS extends `PinotFS <https://github.com/apache/incubator-pinot/blob/master/pinot-filesystem/src/main/java/org/apache/pinot/filesystem/PinotFS.java>`_

The properties for the stream implementation are to be set in your controller and server configurations, `like so <https://github.com/apache/incubator-pinot/wiki/Pluggable-Storage>`_.