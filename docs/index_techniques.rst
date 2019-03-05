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

.. TODO: add more details


Index Techniques
================

Pinot currently supports the following index techniques, where each of them have their own advantages in different query
scenarios.

Forward Index
-------------

Dictionary-Encoded Forward Index with Bit Compression
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For each unique value from a column, we assign an id to it, and build a dictionary from the id to the value. Then in the
forward index, we only store the bit-compressed ids instead of the values.

With few number of unique values, dictionary-encoding can significantly improve the space efficiency of the storage.

Raw Value Forward Index
~~~~~~~~~~~~~~~~~~~~~~~

In contrast to the dictionary-encoded forward index, raw value forward index directly stores values instead of ids.

Without the dictionary, the dictionary lookup step can be skipped for each value fetch. Also, the index can take
advantage of the good locality of the values, thus improve the performance of scanning large number of values.

Sorted Forward Index with Run-Length Encoding
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

On top of the dictionary-encoding, all the values are sorted, so sorted forward index has the advantages of both good
compression and data locality.

Sorted forward index can also be used as inverted index.

Inverted Index (only available with dictionary-encoded indexes)
---------------------------------------------------------------

Bitmap Inverted Index
~~~~~~~~~~~~~~~~~~~~~

Pinot maintains a map from each value to a bitmap, which makes value lookup to be constant time.

Sorted Inverted Index
~~~~~~~~~~~~~~~~~~~~~
Because the values are sorted, the sorted forward index can directly be used as inverted index, with constant time
lookup and good data locality.

Advanced Index
--------------

Star-Tree Index
~~~~~~~~~~~~~~~

Unlike other index techniques which work on single column, Star-Tree index is built on multiple columns, and utilize the
pre-aggregated results to significantly reduce the number of values to be processed, thus improve the query performance.
