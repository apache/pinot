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
package org.apache.pinot.core.common.datatable;

import java.io.IOException;
import java.math.BigDecimal;
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.spi.annotations.InterfaceAudience;
import org.apache.pinot.spi.annotations.InterfaceStability;
import org.apache.pinot.spi.data.readers.Vector;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;


/**
 * DataTable holds data in a matrix form. The purpose of this interface is to provide a way to construct a data table
 * and ability to serialize and deserialize.
 *
 * <p>Why can't we use existing serialization/deserialization mechanism:
 * <p>Most existing techniques (protocol buffer, thrift, avro) are optimized for transporting a single record but Pinot
 * transfers quite a lot of data from server to broker during the scatter/gather operation. The cost of serialization
 * and deserialization directly impacts the performance. Most ser/deser requires us to convert the primitive data types
 * into objects like Integer etc. This will waste cpu resource and increase the payload size. We optimize the data
 * format for Pinot use case. We can also support lazy construction of objects. In fact we retain the bytes as it is and
 * will be able to look up a field directly within a byte buffer.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface DataTableBuilder {

  void startRow();

  void setColumn(int colId, int value);

  void setColumn(int colId, long value);

  void setColumn(int colId, float value);

  void setColumn(int colId, double value);

  void setColumn(int colId, BigDecimal value)
      throws IOException;

  void setColumn(int colId, String value);

  void setColumn(int colId, ByteArray value)
      throws IOException;

  void setColumn(int colId, Vector value)
      throws IOException;

  // TODO: Move ser/de into AggregationFunction interface
  void setColumn(int colId, @Nullable Object value)
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

  // TODO: Support MV BYTES

  void finishRow()
      throws IOException;

  /**
   * NOTE: When setting nullRowIds, we don't pass the colId currently, and this method must be invoked for all columns.
   * TODO: Revisit this
   */
  void setNullRowIds(@Nullable RoaringBitmap nullRowIds)
      throws IOException;

  DataTable build();
}
