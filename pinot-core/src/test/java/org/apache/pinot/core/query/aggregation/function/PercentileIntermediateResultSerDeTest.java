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
package org.apache.pinot.core.query.aggregation.function;

import com.sun.management.ThreadMXBean;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.common.datablock.DataBlockBuilder;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/// Checks that PERCENTILE writes its intermediate result straight into data blocks and data tables, and merges it
/// straight from its serialized form, with the same bytes and results as the byte array based defaults.
public class PercentileIntermediateResultSerDeTest {
  private static final DataSchema SCHEMA = new DataSchema(new String[]{"key", "percentile"},
      new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.OBJECT});
  /// 8 MB of doubles: a humongous object for G1 with any region size up to 16 MB.
  private static final int LARGE_SIZE = 1 << 20;

  private final PercentileAggregationFunction _function = newFunction();
  /// The behavior before the change: serializes to a full byte array, and merges the deserialized value.
  private final PercentileAggregationFunction _byteArrayFunction =
      new PercentileAggregationFunction(ExpressionContext.forIdentifier("$1"), 50, true) {
        @Override
        public SerializedIntermediateResult serializeIntermediateResult(DoubleArrayList values) {
          return new SerializedIntermediateResult(ObjectSerDeUtils.ObjectType.DoubleArrayList.getValue(),
              ObjectSerDeUtils.DOUBLE_ARRAY_LIST_SER_DE.serialize(values));
        }

        @Override
        public DoubleArrayList mergeSerializedIntermediateResult(DoubleArrayList intermediateResult,
            CustomObject serialized) {
          DoubleArrayList incoming = deserializeIntermediateResult(serialized);
          return intermediateResult == null ? incoming : merge(intermediateResult, incoming);
        }
      };

  private static PercentileAggregationFunction newFunction() {
    return new PercentileAggregationFunction(ExpressionContext.forIdentifier("$1"), 50, true);
  }

  private static DoubleArrayList sequence(int size) {
    DoubleArrayList values = new DoubleArrayList(size);
    for (int i = 0; i < size; i++) {
      values.add(i * 0.5 - 7);
    }
    return values;
  }

  private static CustomObject serialize(DoubleArrayList values) {
    return new CustomObject(ObjectSerDeUtils.ObjectType.DoubleArrayList.getValue(),
        ByteBuffer.wrap(ObjectSerDeUtils.DOUBLE_ARRAY_LIST_SER_DE.serialize(values)));
  }

  @DataProvider
  public Object[][] mergeCases() {
    return new Object[][]{
        {null, sequence(3)},
        {sequence(0), sequence(0)},
        {sequence(0), sequence(5)},
        {sequence(4), sequence(0)},
        {sequence(10), sequence(10)},
        // Fills the exact capacity, then grows past it.
        {new DoubleArrayList(new double[]{1, 2}), sequence(1)},
        {sequence(1000), sequence(3000)},
        {sequence(2), new DoubleArrayList(new double[]{Double.longBitsToDouble(0x7ff8_0000_0000_0123L), -0.0})}
    };
  }

  @Test(dataProvider = "mergeCases")
  public void testMergeSerializedMatchesMergeOfDeserialized(DoubleArrayList state, DoubleArrayList incoming) {
    DoubleArrayList expectedState = state == null ? null : new DoubleArrayList(state);
    DoubleArrayList expected = _byteArrayFunction.mergeSerializedIntermediateResult(expectedState, serialize(incoming));

    CustomObject serialized = serialize(incoming);
    DoubleArrayList actual = _function.mergeSerializedIntermediateResult(state, serialized);

    // Compares the serialized bytes, so the values must match bit for bit, NaN payloads included.
    assertEquals(ObjectSerDeUtils.DOUBLE_ARRAY_LIST_SER_DE.serialize(actual),
        ObjectSerDeUtils.DOUBLE_ARRAY_LIST_SER_DE.serialize(expected));
    assertEquals(serialized.getBuffer().remaining(), 0, "Consumes the whole value");
    if (state != null) {
      assertSame(actual, state, "Merges in place");
    }
  }

  @Test
  public void testMergeSerializedSkipsNull() {
    DoubleArrayList state = sequence(2);
    assertSame(AggregationFunctionUtils.mergeSerialized(_function, state, null), state);
  }

  @Test
  public void testSerializedIntermediateResultMatchesByteArray()
      throws IOException {
    // A NaN with a payload must keep its raw bits, like ByteBuffer.putDouble in the byte array serializer.
    DoubleArrayList specialValues = new DoubleArrayList(
        new double[]{Double.NaN, Double.longBitsToDouble(0x7ff8_0000_0000_0123L), -0.0, Double.NEGATIVE_INFINITY});
    for (DoubleArrayList values : List.of(sequence(0), sequence(1), sequence(1000), specialValues)) {
      AggregationFunction.SerializedIntermediateResult expected =
          _byteArrayFunction.serializeIntermediateResult(values);
      AggregationFunction.SerializedIntermediateResult actual = _function.serializeIntermediateResult(values);

      assertEquals(actual.getType(), expected.getType());
      assertEquals(actual.getSize(), expected.getBytes().length);
      assertEquals(actual.getBytes(), expected.getBytes());
      ByteArrayOutputStream written = new ByteArrayOutputStream();
      actual.writeTo(new DataOutputStream(written));
      assertEquals(written.toByteArray(), expected.getBytes());
    }
  }

  private static List<Object[]> rows(int largeSize) {
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{1, sequence(3)});
    rows.add(new Object[]{2, null});
    rows.add(new Object[]{3, sequence(0)});
    // Spans several pages of the variable size output stream.
    rows.add(new Object[]{4, sequence(largeSize)});
    rows.add(new Object[]{5, sequence(17)});
    return rows;
  }

  private static byte[] concat(List<ByteBuffer> buffers) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (ByteBuffer buffer : buffers) {
      byte[] bytes = new byte[buffer.remaining()];
      buffer.duplicate().get(bytes);
      out.writeBytes(bytes);
    }
    return out.toByteArray();
  }

  private static byte[] dataBlockBytes(List<Object[]> rows, AggregationFunction function)
      throws IOException {
    DataBlock dataBlock = DataBlockBuilder.buildFromRows(rows, SCHEMA, new AggregationFunction[]{function});
    return concat(dataBlock.serialize());
  }

  /// Builds a columnar data block of the state column only: buildFromColumns does not support key columns together
  /// with aggregation functions.
  private static byte[] columnarDataBlockBytes(List<Object[]> rows, AggregationFunction function)
      throws IOException {
    Object[] values = new Object[rows.size()];
    for (int i = 0; i < rows.size(); i++) {
      values[i] = rows.get(i)[1];
    }
    DataSchema schema = new DataSchema(new String[]{"percentile"}, new ColumnDataType[]{ColumnDataType.OBJECT});
    DataBlock dataBlock = DataBlockBuilder.buildFromColumns(List.<Object[]>of(values), schema,
        new AggregationFunction[]{function});
    return concat(dataBlock.serialize());
  }

  private static byte[] dataTableBytes(List<Object[]> rows, AggregationFunction function)
      throws IOException {
    DataTableBuilder builder = DataTableBuilderFactory.getDataTableBuilder(SCHEMA);
    for (Object[] row : rows) {
      builder.startRow();
      builder.setColumn(0, (int) row[0]);
      if (row[1] == null) {
        builder.setNull(1);
      } else {
        builder.setColumn(1, function.serializeIntermediateResult(row[1]));
      }
      builder.finishRow();
    }
    return builder.build().toBytes();
  }

  @Test
  public void testDataBlockAndDataTableBytesMatchByteArraySerialization()
      throws IOException {
    List<Object[]> rows = rows(100_000);
    assertEquals(dataBlockBytes(rows, _function), dataBlockBytes(rows, _byteArrayFunction));
    assertEquals(columnarDataBlockBytes(rows, _function), columnarDataBlockBytes(rows, _byteArrayFunction));
    assertEquals(dataTableBytes(rows, _function), dataTableBytes(rows, _byteArrayFunction));
  }

  /// Building a data block writes the large list into the paged output stream (8 MB of small pages) without the 8 MB
  /// byte array the byte array path allocates first.
  @Test
  public void testDataBlockBuildDoesNotAllocateFullSizeByteArray() {
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{1, sequence(LARGE_SIZE)});
    long payloadBytes = Integer.BYTES + (long) LARGE_SIZE * Double.BYTES;

    long byteArrayAllocated = allocatedBytes(
        () -> DataBlockBuilder.buildFromRows(rows, SCHEMA, new AggregationFunction[]{_byteArrayFunction}));
    long allocated =
        allocatedBytes(() -> DataBlockBuilder.buildFromRows(rows, SCHEMA, new AggregationFunction[]{_function}));

    assertTrue(byteArrayAllocated > payloadBytes * 3 / 2, "byte array path: " + byteArrayAllocated);
    assertTrue(allocated < payloadBytes * 3 / 2, "stream path: " + allocated);
  }

  /// Merging reads the doubles straight into the merged list, which already has room for them, instead of
  /// deserializing an 8 MB list first.
  @Test
  public void testMergeSerializedDoesNotAllocateIncomingList() {
    CustomObject serialized = serialize(sequence(LARGE_SIZE));
    long payloadBytes = serialized.getBuffer().remaining();

    long byteArrayAllocated = allocatedBytes(() -> _byteArrayFunction.mergeSerializedIntermediateResult(
        new DoubleArrayList(LARGE_SIZE), new CustomObject(serialized.getType(), serialized.getBuffer().duplicate())));
    long allocated = allocatedBytes(() -> _function.mergeSerializedIntermediateResult(
        new DoubleArrayList(LARGE_SIZE), new CustomObject(serialized.getType(), serialized.getBuffer().duplicate())));

    // Both allocate the 8 MB state of the merge target.
    assertTrue(byteArrayAllocated > payloadBytes * 3 / 2, "deserialize path: " + byteArrayAllocated);
    assertTrue(allocated < payloadBytes * 3 / 2, "in place path: " + allocated);
  }

  /// Heap bytes the current thread allocates while running `action`. A warm-up run keeps class loading and lazy
  /// initialization out of the measurement.
  private static long allocatedBytes(ThrowingRunnable action) {
    ThreadMXBean threadBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    long threadId = Thread.currentThread().threadId();
    try {
      action.run();
      long before = threadBean.getThreadAllocatedBytes(threadId);
      action.run();
      return threadBean.getThreadAllocatedBytes(threadId) - before;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private interface ThrowingRunnable {
    void run()
        throws Exception;
  }
}
