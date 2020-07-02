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

Record Reader
=============

Pinot supports indexing data from various file formats. To support reading from a file format, a record reader need to
be provided to read the file and convert records into the general format which the indexing engine can understand. The
record reader serves as the connector from each individual file format to Pinot record format.

Pinot package provides the following record readers out of the box:

- Avro record reader: record reader for Avro format files
- CSV record reader: record reader for CSV format files
- JSON record reader: record reader for JSON format files
- ORC record reader: record reader for ORC format files
- Thrift record reader: record reader for Thrift format files
- Pinot segment record reader: record reader for Pinot segment

Initialize Record Reader
------------------------

To initialize a record reader, the data file and table schema should be provided (for Pinot segment record reader, only
need to provide the index directory because schema can be derived from the segment). The output record will follow the
table schema provided.

For Avro/JSON/ORC/Pinot segment record reader, no extra configuration is required as column names and multi-values are
embedded in the data file.

For CSV/Thrift record reader, extra configuration might be provided to determine the column names and multi-values for
the data.

CSV Record Reader Config
~~~~~~~~~~~~~~~~~~~~~~~~

The CSV record reader config contains the following settings:

- Header: the header for the CSV file (column names)
- Column delimiter: delimiter for each column
- Multi-value delimiter: delimiter for each value for a multi-valued column

If no config provided, use the default setting:

- Use the first row in the data file as the header
- Use ',' as the column delimiter
- Use ';' as the multi-value delimiter

Thrift Record Reader Config
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Thrift record reader config is mandatory. It contains the Thrift class name for the record reader to de-serialize
the Thrift objects.

ORC Record Reader Config
~~~~~~~~~~~~~~~~~~~~~~~~
The following property is to be set during segment generation in your Hadoop properties.

record.reader.path: ${FULL_PATH_OF_YOUR_RECORD_READER_CLASS}

For ORC, it would be:

record.reader.path: org.apache.pinot.orc.data.readers.ORCRecordReader


Implement Your Own Record Reader
--------------------------------

For other file formats, we provide a general interface for record reader - `RecordReader <https://github.com/apache/incubator-pinot/blob/master/pinot-spi/src/main/java/org/apache/pinot/spi/data/readers/RecordReader.java>`_.
To index the file into Pinot segment, simply implement the interface and plug it into the index engine - `SegmentCreationDriverImpl <https://github.com/apache/incubator-pinot/blob/master/pinot-core/src/main/java/org/apache/pinot/core/segment/creator/impl/SegmentIndexCreationDriverImpl.java>`_.
We use a 2-passes algorithm to index the file into Pinot segment, hence the *rewind()* method is required for the record
reader.

Generic Row
~~~~~~~~~~~

`GenericRow <https://github.com/apache/incubator-pinot/blob/master/pinot-core/src/main/java/org/apache/pinot/core/data/GenericRow.java>`_
is the record abstraction which the index engine can read and index with. It is a map from column name (String) to
column value (Object). For multi-valued column, the value should be an object array (Object[]).

Contracts for Record Reader
~~~~~~~~~~~~~~~~~~~~~~~~~~~

There are several contracts for record readers that developers should follow when implementing their own record readers:

- The output GenericRow should follow the table schema provided, in the sense that:

  - All the columns in the schema should be preserved (if column does not exist in the original record, put default
    value instead)
  - Columns not in the schema should not be included
  - Values for the column should follow the field spec from the schema (data type, single-valued/multi-valued)

- For the time column (refer to `TimeFieldSpec <https://github.com/apache/incubator-pinot/blob/master/pinot-common/src/main/java/org/apache/pinot/common/data/TimeFieldSpec.java>`_),
  record reader should be able to read both incoming and outgoing time (we allow *incoming time - time value from the
  original data* to *outgoing time - time value stored in Pinot* conversion during index creation).

  - If incoming and outgoing time column name are the same, use incoming time field spec
  - If incoming and outgoing time column name are different, put both of them as time field spec
  - We keep both incoming and outgoing time column to handle cases where the input file contains time values that are
    already converted
