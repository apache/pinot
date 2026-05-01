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
package org.apache.pinot.plugin.inputformat.orc;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.TypeDescription;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.annotations.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


/// Tests [ORCRecordExtractor] directly — no ORC file IO. Each test builds a one-row [VectorizedRowBatch]
/// in-memory, populates the column vector via the test-provided lambda, and runs the extractor. See the
/// extractor's class Javadoc for the ORC schema category → Java output type matrix.
public class ORCRecordExtractorTest {

  private static final String COLUMN = "col";

  // === Single-value — order follows the ORC type list in the class Javadoc ===

  @Test
  public void testBooleanPreserved() {
    Object result = extract("boolean", batch -> ((LongColumnVector) batch.cols[0]).vector[0] = 1);
    assertEquals(result, true);
  }

  @Test
  public void testTinyIntExtractedAsInteger() {
    Object result = extract("tinyint", batch -> ((LongColumnVector) batch.cols[0]).vector[0] = 7);
    assertEquals(result, 7);
  }

  @Test
  public void testSmallIntExtractedAsInteger() {
    Object result = extract("smallint", batch -> ((LongColumnVector) batch.cols[0]).vector[0] = 42);
    assertEquals(result, 42);
  }

  @Test
  public void testIntPreserved() {
    Object result = extract("int", batch -> ((LongColumnVector) batch.cols[0]).vector[0] = 1234);
    assertEquals(result, 1234);
  }

  @Test
  public void testBigIntPreservedAsLong() {
    Object result = extract("bigint", batch -> ((LongColumnVector) batch.cols[0]).vector[0] = 1_588_469_340_000L);
    assertEquals(result, 1_588_469_340_000L);
  }

  @Test
  public void testFloatPreserved() {
    Object result = extract("float", batch -> ((DoubleColumnVector) batch.cols[0]).vector[0] = 1.5d);
    assertEquals(result, 1.5f);
  }

  @Test
  public void testDoublePreserved() {
    Object result = extract("double", batch -> ((DoubleColumnVector) batch.cols[0]).vector[0] = 2.5d);
    assertEquals(result, 2.5d);
  }

  @Test
  public void testStringPreserved() {
    Object result = extract("string", batch -> ((BytesColumnVector) batch.cols[0]).setVal(0, "hello".getBytes(UTF_8)));
    assertEquals(result, "hello");
  }

  @Test
  public void testBinaryExtractedAsByteArray() {
    byte[] bytes = {0, 1, 2, 3};
    Object result = extract("binary", batch -> ((BytesColumnVector) batch.cols[0]).setVal(0, bytes));
    assertEquals((byte[]) result, bytes);
  }

  @Test
  public void testTimestampExtractedAsTimestamp() {
    long epochMillis = 1_649_924_302_123L;
    Object result = extract("timestamp", batch -> {
      TimestampColumnVector tsv = (TimestampColumnVector) batch.cols[0];
      // Reader splits epoch millis into seconds-precision `time` + nanos. Match that representation.
      tsv.time[0] = (epochMillis / 1000) * 1000;
      tsv.nanos[0] = (int) (epochMillis % 1000) * 1_000_000;
    });
    assertEquals(result, new Timestamp(epochMillis));
  }

  @Test
  public void testTimestampPreservesSubMillisecondNanos() {
    // `nanos[rowId]` carries full sub-second precision. The extractor must propagate it via `setNanos` so
    // micro/nano-precision survives.
    long epochSecondsMillis = 1_649_924_302_000L;
    int subSecondNanos = 123_456_789;
    Object result = extract("timestamp", batch -> {
      TimestampColumnVector tsv = (TimestampColumnVector) batch.cols[0];
      tsv.time[0] = epochSecondsMillis;
      tsv.nanos[0] = subSecondNanos;
    });
    Timestamp expected = new Timestamp(epochSecondsMillis);
    expected.setNanos(subSecondNanos);
    assertEquals(result, expected);
    assertEquals(((Timestamp) result).getNanos(), subSecondNanos);
  }

  @Test
  public void testTimestampInstantExtractedAsTimestamp() {
    // ORC's `timestamp with local time zone` (TIMESTAMP_INSTANT) shares TimestampColumnVector with TIMESTAMP.
    long epochMillis = 1_649_924_302_123L;
    Object result = extract("timestamp with local time zone", batch -> {
      TimestampColumnVector tsv = (TimestampColumnVector) batch.cols[0];
      tsv.time[0] = (epochMillis / 1000) * 1000;
      tsv.nanos[0] = (int) (epochMillis % 1000) * 1_000_000;
    });
    assertEquals(result, new Timestamp(epochMillis));
  }

  @Test
  public void testDateExtractedAsLocalDate() {
    // ORC stores `date` as days-since-epoch in a `LongColumnVector`; surface as [LocalDate] via
    // `LocalDate.ofEpochDay(...)`. 19_456 days = 2023-04-09.
    Object result = extract("date", batch -> ((LongColumnVector) batch.cols[0]).vector[0] = 19_456L);
    assertEquals(result, LocalDate.of(2023, 4, 9));
  }

  @Test
  public void testDecimalExtractedAsBigDecimal() {
    Object result = extract("decimal(10,5)", batch -> {
      DecimalColumnVector vec = (DecimalColumnVector) batch.cols[0];
      vec.set(0, new HiveDecimalWritable(HiveDecimal.create("123.45000")));
    });
    assertEquals(result, new BigDecimal("123.45"));
  }

  // === Complex types ===

  @Test
  public void testListOfIntsExtractedAsArray() {
    Object[] result = (Object[]) extract("array<int>", batch -> {
      ListColumnVector list = (ListColumnVector) batch.cols[0];
      LongColumnVector child = (LongColumnVector) list.child;
      child.ensureSize(3, false);
      list.offsets[0] = 0;
      list.lengths[0] = 3;
      child.vector[0] = 10;
      child.vector[1] = 20;
      child.vector[2] = 30;
    });
    assertEquals(result, new Object[]{10, 20, 30});
  }

  @Test
  public void testListPreservesNullElements() {
    // The contract requires multi-value shape preservation: null elements stay null in `Object[]`.
    Object[] result = (Object[]) extract("array<int>", batch -> {
      ListColumnVector list = (ListColumnVector) batch.cols[0];
      LongColumnVector child = (LongColumnVector) list.child;
      child.ensureSize(3, false);
      child.noNulls = false;
      child.isNull[1] = true;
      list.offsets[0] = 0;
      list.lengths[0] = 3;
      child.vector[0] = 10;
      child.vector[2] = 30;
    });
    assertEquals(result, new Object[]{10, null, 30});
  }

  @Test
  public void testEmptyListExtractedAsEmptyArray() {
    // The contract requires shape preservation: an empty list surfaces as an empty `Object[]`, not `null`.
    Object[] result = (Object[]) extract("array<int>", batch -> {
      ListColumnVector list = (ListColumnVector) batch.cols[0];
      list.offsets[0] = 0;
      list.lengths[0] = 0;
    });
    assertEquals(result, new Object[]{});
  }

  @Test
  public void testMapStringToIntExtractedAsMap() {
    Object result = extract("map<string,int>", batch -> {
      MapColumnVector map = (MapColumnVector) batch.cols[0];
      BytesColumnVector keys = (BytesColumnVector) map.keys;
      LongColumnVector values = (LongColumnVector) map.values;
      keys.ensureSize(2, false);
      values.ensureSize(2, false);
      map.offsets[0] = 0;
      map.lengths[0] = 2;
      keys.setVal(0, "a".getBytes(UTF_8));
      keys.setVal(1, "b".getBytes(UTF_8));
      values.vector[0] = 1;
      values.vector[1] = 2;
    });
    Map<?, ?> resultMap = (Map<?, ?>) result;
    assertEquals(resultMap.size(), 2);
    assertEquals(resultMap.get("a"), 1);
    assertEquals(resultMap.get("b"), 2);
  }

  @Test
  public void testStructExtractedAsMap() {
    Object result = extract("struct<s:string,i:int>", batch -> {
      StructColumnVector struct = (StructColumnVector) batch.cols[0];
      BytesColumnVector sVec = (BytesColumnVector) struct.fields[0];
      LongColumnVector iVec = (LongColumnVector) struct.fields[1];
      sVec.setVal(0, "hello".getBytes(UTF_8));
      iVec.vector[0] = 42;
    });
    Map<?, ?> resultMap = (Map<?, ?>) result;
    assertEquals(resultMap.get("s"), "hello");
    assertEquals(resultMap.get("i"), 42);
  }

  // === Null handling ===

  @Test
  public void testNullValueReturnsNull() {
    Object result = extract("int", batch -> {
      LongColumnVector vec = (LongColumnVector) batch.cols[0];
      vec.noNulls = false;
      vec.isNull[0] = true;
    });
    assertNull(result);
  }

  // === Helpers ===

  /// Build a one-row [VectorizedRowBatch] with a single column of the given ORC type, populate it via
  /// `populate`, then run the extractor and return the extracted column value.
  private static Object extract(String orcType, Consumer<VectorizedRowBatch> populate) {
    TypeDescription schema = TypeDescription.fromString("struct<" + COLUMN + ":" + orcType + ">");
    VectorizedRowBatch batch = schema.createRowBatch(1);
    populate.accept(batch);
    batch.size = 1;

    ORCRecordExtractor.Record record = new ORCRecordExtractor.Record();
    record.set(batch, schema, 0);

    ORCRecordExtractor extractor = new ORCRecordExtractor();
    extractor.init(null, null);
    GenericRow row = new GenericRow();
    extractor.extract(record, row);
    return row.getValue(COLUMN);
  }
}
