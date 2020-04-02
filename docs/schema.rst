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

.. _schema-section:

Pinot Schema
============

Pinot Schema consists of columns that can be categorized as dimensions, metric or time.

**Dimensions** - These are columns that organize the data. For example, ``accountId``, ``country``, ``industry``, etc. These columns are used to slice/dice the data and typically appear in the ``selection``, ``filter`` and ``group-by`` sections in queries.

**Metrics** - These are columns that represent quantative measurements. For example, ``numClicks``, ``pageViews``, etc. These columns typically appear in the aggregation section of the query, e.g., ``select sum(pageViews) from...``.

**Time** - The time column represents the timestamp of the data. There is only one time column in a schema, and typically appears in the ``filter`` and ``group-by`` sections in queries. 

A sample schema is shown below and will be used as an example to described the various fields.

Sample Schema:
~~~~~~~~~~~~~~

.. code-block:: json

   {
     "schemaName": "flights",
     "dimensionFieldSpecs": [
       {
         "name": "flightNumber",
         "dataType": "LONG"
       },
       {
         "name": "tags",
         "dataType": "STRING",
         "singleValueField": false,
         "defaultNullValue": "null"
       }
     ],
     "metricFieldSpecs": [
       {
         "name": "price",
         "dataType": "DOUBLE",
         "defaultNullValue": 0
       }
     ],
     "timeFieldSpec": {
       "incomingGranularitySpec": {
         "name": "secondsSinceEpoch",
         "dataType": "INT",
         "timeFormat" : "EPOCH",
         "timeType": "SECONDS"
       },
        "outgoingGranularitySpec": {
         "name": "messageTimeHours",
         "dataType": "INT",
         "timeFormat" : "EPOCH",
         "timeType": "DAYS"
       }
     }
   }

Schema Name:
~~~~~~~~~~~~

Every Pinot schema has a name that is used to identify it. For example, schema for a table is specified in the table conig by its name.


FieldSpecs:
~~~~~~~~~~~

Each column in the schema is described using a ``fieldSpec`` that captures various attributes of the column, such as name, data-type, etc. A schema may contain an array of ``dimensionFieldSpecs`` that describe all the dimension columns, ``metricFieldSpecs`` that describe all the metric columns, and a ``timeFieldSpec`` that describes the time column.

Dimensions:
~~~~~~~~~~~

The schema example above has two dimensions ``flightNumber`` of type ``Long`` and ``tags`` of type ``String``. Data types supported for dimension columns are:

* **INT**
* **FLOAT**
* **LONG**
* **DOUBLE**
* **STRING**
* **BYTES**

Dimension columns also have an optional attribute named ``singleValuedFiled`` with a default value of ``true``. This attribute describes whether the column can take a single value or multiple values for a row. In the example above, the dimension ``tags`` is multi-valued. This means that it can have multiple values for a particular row, say ``tag1, tag2, tag3``. For a multi-valued column, individual rows don't necessarily need to have the same number of values. Typical use case for this would be a column such as ``skillSet`` for a person (one row in the table) that can have multiple values such as ``Real Estate, Mortgages``.


Metrics:
~~~~~~~~

The column ``price`` in the schema is a metric column, as one can query aggregations such as ``average``, ``min``, ``max``, etc on it and are typically single-valued. Metric columns can have the following data types:

* **INT**
* **FLOAT**
* **LONG**
* **DOUBLE**
* **BYTES**

Metric columns are typically numeric. However, note that ``BYTES`` is an allowed data type for metric columns. This is typically used in cases of specialized representations such as HLL, TDigest, etc, where the column actually stores byte serialized version of the value.

Time:
~~~~~

The schema above also contains a ``timeFieldSpec`` that is used to specify the attributes of the time column:

* **incomingGranularitySpec** : Specifies the name, data type and time type for the time stamp present in the incoming data into Pinot.
* **outgoingGranularitySpec** : Specifies the name, data type and time type for the time stamp as desired to be stored in Pinot.

In this example, the input timestamp specified in ``SECONDS`` will be automatically converted into ``DAYS`` before storing into Pinot. The ``timeFieldSpec`` also has an optional attribute ``timeFormat`` that can take values ``EPOCH`` (default) and ``SIMPLE_DATE_FORMAT:<format>``.

Time columns are mandatory for ``APPEND`` (incremental data push) use cases but optional for ``REFRESH`` (data refresh with each push) use cases. More details on this can be found at the `Segment Config <tableconfig_schema.html#segments-config-section>`_ section. 


Default Null Value:
~~~~~~~~~~~~~~~~~~~

Pinot does not store null values natively, so null values are stored as placeholder values as specified in the fieldspec, or a default value chosen by the system. In the schema example above, all null values in the input for column ``flights`` are converted to String ``"null"`` and stored internally. Simiarly, null values in the input for column ``price`` is converted to integer ``0`` and stored internally. Since this is an optional field, if not specified, default null values are chosen by the system as follows:

* **Dimensions**: Numeric fields get a default null value of MIN_VALUE (e.g Integer.MIN_VALUE, Double.MIN_VALUE, etc), and String fields get a default null value of String ``"null"``, if not specified in the field spec.
* **Metrics**: All metrics get a default null value of numeric ``0`` if not specified in the field spec.

Dimension and metric columns of data type ``BYTES`` get a defaulit null value of ``byte[0]``, if not specified in the field spec. For specifiying a custom defaultNullValue for column of ``BYTES`` type, use the Hex String representation of the byte[] using `this <https://commons.apache.org/proper/commons-codec/apidocs/org/apache/commons/codec/binary/Hex.html>`_ library.

