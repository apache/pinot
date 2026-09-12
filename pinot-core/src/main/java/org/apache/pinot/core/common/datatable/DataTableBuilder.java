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
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.spi.annotations.InterfaceAudience;
import org.apache.pinot.spi.annotations.InterfaceStability;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;


/// DataTable holds data in a matrix form. The purpose of this interface is to provide a way to construct a data table
/// and ability to serialize and deserialize.
///
/// Why can't we use existing serialization/deserialization mechanism:
///
/// Most existing techniques (protocol buffer, thrift, avro) are optimized for transporting a single record but Pinot
/// transfers quite a lot of data from server to broker during the scatter/gather operation. The cost of serialization
/// and deserialization directly impacts the performance. Most ser/deser requires us to convert the primitive data
/// types into objects like Integer etc. This will waste cpu resource and increase the payload size. We optimize the
/// data format for Pinot use case. We can also support lazy construction of objects. In fact we retain the bytes as
/// it is and will be able to look up a field directly within a byte buffer.
///
/// TODO: Consider skipping seeking for the column offsets and directly write to the byte buffer
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

  void setColumn(int colId, @Nullable Map<String, Object> value)
      throws IOException;

  void setColumn(int colId, int[] values)
      throws IOException;

  void setColumn(int colId, long[] values)
      throws IOException;

  void setColumn(int colId, float[] values)
      throws IOException;

  void setColumn(int colId, double[] values)
      throws IOException;

  void setColumn(int colId, BigDecimal[] values)
      throws IOException;

  void setColumn(int colId, String[] values)
      throws IOException;

  void setColumn(int colId, ByteArray[] values)
      throws IOException;

  void setColumn(int colId, AggregationFunction.SerializedIntermediateResult value)
      throws IOException;

  /// Writes a `null` for the given column of the current row.
  ///
  /// Valid for every column type, independent of the query's null-handling option. Types that can represent `null`
  /// in-band (`OBJECT`, `UNKNOWN`, `MAP`) keep their own encoding; every other type is written as the column's null
  /// placeholder and the row is recorded in a per-column null bitmap that `build()` appends to the table. Readers
  /// restore the `null` via `DataTable.getNullRowIds`.
  void setNull(int colId)
      throws IOException;

  void finishRow()
      throws IOException;

  /// Merges a pre-computed null bitmap into the bitmap the builder maintains for the next column.
  ///
  /// NOTE: The colId is positional -- the first call targets column 0, the second column 1, and so on -- so callers
  /// that use this method must invoke it once for every column, in order. Callers that instead report nulls per cell
  /// through [#setNull] need not call this at all.
  void setNullRowIds(@Nullable RoaringBitmap nullRowIds)
      throws IOException;

  DataTable build();
}
