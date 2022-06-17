/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.common;

import java.io.IOException;
import java.math.BigDecimal;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;


/**
 * Datatable that holds data in a matrix form. The purpose of this class is to
 * provide a way to construct a datatable and ability to serialize and
 * deserialize.<br>
 * Why can't we use existing serialization/deserialization mechanism. Most
 * existing techniques protocol buffer, thrift, avro are optimized for
 * transporting a single record but Pinot transfers quite a lot of data from
 * server to broker during the scatter/gather operation. The cost of
 * serialization and deserialization directly impacts the performance. Most
 * ser/deser requires us to convert the primitives data types in objects like
 * Integer etc. This is waste of cpu resource and increase the payload size. We
 * optimize the data format for Pinot usecase. We can also support lazy
 * construction of obejcts. Infact we retain the bytes as it is and will be able
 * to lookup the a field directly within a byte buffer.<br>
 *
 * USAGE:
 *
 * Datatable is initialized with the schema of the table. Schema describes the
 * columnnames, their order and data type for each column.<br>
 * Each row must follow the same convention. We don't support MultiValue columns
 * for now. Format,
 * |VERSION,DATA_START_OFFSET,DICTIONARY_START_OFFSET,INDEX_START_OFFSET
 * ,METADATA_START_OFFSET | |&lt;DATA&gt; |
 *
 * |&lt;DICTIONARY&gt;|
 *
 *
 * |&lt;METADATA&gt;| Data contains the actual values written by the application We
 * first write the entire data in its raw byte format. For example if you data
 * type is Int, it will write 4 bytes. For most data types that are fixed width,
 * we just write the raw data. For special cases like String, we create a
 * dictionary. Dictionary will be never exposed to the user. All conversions
 * will be done internally. In future, we might decide dynamically if dictionary
 * creation is needed, for now we will always create dictionaries for string
 * columns. During deserialization we will always load the dictionary
 * first.Overall having dictionary allow us to convert data table into a fixed
 * width matrix and thus allowing look up and easy traversal.
 */
public interface DataTableBuilder {

  void setNullRowIds(RoaringBitmap nullBitmap)
      throws IOException;

  void startRow();

  void setColumn(int colId, boolean value);

  void setColumn(int colId, byte value);

  void setColumn(int colId, char value);

  void setColumn(int colId, short value);

  void setColumn(int colId, int value);

  void setColumn(int colId, long value);

  void setColumn(int colId, float value);

  void setColumn(int colId, double value);

  void setColumn(int colId, BigDecimal value)
      throws IOException;

  void setColumn(int colId, String value);

  void setColumn(int colId, ByteArray value)
      throws IOException;

  void setColumn(int colId, Object value)
      throws IOException;

  void setColumn(int colId, int[] values)
      throws IOException;

  void setColumn(int colId, long[] values)
      throws IOException;

  void setColumn(int colId, float[] values)
      throws IOException;

  void setColumn(int colId, double[] values)
      throws IOException;

  void setColumn(int colId, String[] values)
      throws IOException;

  void finishRow()
      throws IOException;

  DataTable build();
}
