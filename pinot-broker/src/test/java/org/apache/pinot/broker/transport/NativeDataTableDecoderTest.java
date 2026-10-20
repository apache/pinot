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
package org.apache.pinot.broker.transport;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTable.MetadataKey;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


/// Uses ordinary V4 writers and small malformed frames to verify native checks precede allocation and reduction.
public class NativeDataTableDecoderTest {
  @Test
  public void testOrdinaryValuesAndEmptyTable() throws IOException {
    ColumnDataType[] types = {ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.FLOAT, ColumnDataType.DOUBLE,
        ColumnDataType.STRING, ColumnDataType.BYTES, ColumnDataType.BIG_DECIMAL, ColumnDataType.INT_ARRAY,
        ColumnDataType.LONG_ARRAY, ColumnDataType.FLOAT_ARRAY, ColumnDataType.DOUBLE_ARRAY,
        ColumnDataType.STRING_ARRAY, ColumnDataType.BYTES_ARRAY, ColumnDataType.BIG_DECIMAL_ARRAY};
    String[] names = new String[types.length];
    for (int i = 0; i < names.length; i++) {
      names[i] = "column" + i;
    }
    DataSchema schema = new DataSchema(names, types);
    DataTableBuilder builder = DataTableBuilderFactory.getDataTableBuilder(schema);
    builder.startRow();
    builder.setColumn(0, 7);
    builder.setColumn(1, 8L);
    builder.setColumn(2, 1.5F);
    builder.setColumn(3, 2.5D);
    builder.setColumn(4, "hello");
    builder.setColumn(5, new ByteArray(new byte[]{1, 2}));
    builder.setColumn(6, new BigDecimal("1.25"));
    builder.setColumn(7, new int[]{1, 2});
    builder.setColumn(8, new long[]{3, 4});
    builder.setColumn(9, new float[]{1.5F});
    builder.setColumn(10, new double[]{2.5D});
    builder.setColumn(11, new String[]{"hello", "world"});
    builder.setColumn(12, new ByteArray[]{new ByteArray(new byte[]{3}), new ByteArray(new byte[0])});
    builder.setColumn(13, new BigDecimal[]{new BigDecimal("2.75")});
    builder.finishRow();
    DataTable original = builder.build();
    original.getMetadata().put(MetadataKey.REQUEST_ID.getName(), "42");
    original.getMetadata().put(MetadataKey.TRACE_INFO.getName(), "trace");
    ByteBuffer input = ByteBuffer.wrap(original.toBytes());
    DataTable decoded = NativeDataTableDecoder.decode(input);
    assertEquals(input.position(), 0);
    assertEquals(decoded.getDataSchema(), schema);
    assertEquals(decoded.getNumberOfRows(), 1);
    assertEquals(decoded.getInt(0, 0), 7);
    assertEquals(decoded.getLong(0, 1), 8L);
    assertEquals(decoded.getFloat(0, 2), 1.5F);
    assertEquals(decoded.getDouble(0, 3), 2.5D);
    assertEquals(decoded.getString(0, 4), "hello");
    assertEquals(decoded.getBytes(0, 5), new ByteArray(new byte[]{1, 2}));
    assertEquals(decoded.getBigDecimal(0, 6), new BigDecimal("1.25"));
    assertEquals(decoded.getIntArray(0, 7), new int[]{1, 2});
    assertEquals(decoded.getLongArray(0, 8), new long[]{3, 4});
    assertEquals(decoded.getFloatArray(0, 9), new float[]{1.5F});
    assertEquals(decoded.getDoubleArray(0, 10), new double[]{2.5D});
    assertEquals(decoded.getStringArray(0, 11), new String[]{"hello", "world"});
    assertEquals(decoded.getBytesArray(0, 12),
        new ByteArray[]{new ByteArray(new byte[]{3}), new ByteArray(new byte[0])});
    assertEquals(decoded.getBigDecimalArray(0, 13), new BigDecimal[]{new BigDecimal("2.75")});
    assertEquals(decoded.getMetadata(), original.getMetadata());
    DataTable empty = DataTableBuilderFactory.getDataTableBuilder(schema).build();
    assertEquals(NativeDataTableDecoder.decode(ByteBuffer.wrap(empty.toBytes())).getNumberOfRows(), 0);
  }

  @Test
  public void testNullBitmapArrayBitmapAndRunContainers() throws IOException {
    RoaringBitmap array = RoaringBitmap.bitmapOf(0, 70_000);
    RoaringBitmap bitmap = new RoaringBitmap();
    for (int i = 0; i < 10_000; i += 2) {
      bitmap.add(i);
    }
    RoaringBitmap run = new RoaringBitmap();
    run.add(0L, 5000L);
    run.runOptimize();
    for (RoaringBitmap nulls : List.of(array, bitmap, run)) {
      int rows = nulls.last() + 1;
      DataTableBuilder builder = DataTableBuilderFactory.getDataTableBuilder(
          new DataSchema(new String[]{"id"}, new ColumnDataType[]{ColumnDataType.INT}));
      for (int i = 0; i < rows; i++) {
        builder.startRow();
        builder.setColumn(0, i);
        builder.finishRow();
      }
      builder.setNullRowIds(nulls);
      byte[] bytes = builder.build().toBytes();
      DataTable decoded = NativeDataTableDecoder.decode(ByteBuffer.wrap(bytes));
      assertEquals(decoded.getNullRowIds(0), nulls);
      int fixed = ByteBuffer.wrap(bytes).getInt(9 * Integer.BYTES);
      rejectInt(bytes, fixed + rows * Integer.BYTES + Integer.BYTES, Integer.MAX_VALUE);
      int variable = ByteBuffer.wrap(bytes).getInt(11 * Integer.BYTES);
      rejectInt(bytes, variable, 0);
    }
  }

  @Test
  public void testRejectAllocationLengthsBeforeDecoding() throws IOException {
    DataTableBuilder builder = DataTableBuilderFactory.getDataTableBuilder(
        new DataSchema(new String[]{"value"}, new ColumnDataType[]{ColumnDataType.STRING}));
    builder.startRow();
    builder.setColumn(0, "small");
    builder.finishRow();
    DataTable table = builder.build();
    table.getMetadata().put(MetadataKey.TRACE_INFO.getName(), "small");
    byte[] bytes = table.toBytes();
    ByteBuffer header = ByteBuffer.wrap(bytes);
    for (int field : new int[]{0, 1, 2, 4, 6, 8, 10, 12}) {
      rejectInt(bytes, field * Integer.BYTES, Integer.MAX_VALUE);
      rejectInt(bytes, field * Integer.BYTES, -1);
    }
    int dictionary = header.getInt(5 * Integer.BYTES);
    rejectInt(bytes, dictionary, Integer.MAX_VALUE);
    rejectInt(bytes, dictionary + Integer.BYTES, Integer.MAX_VALUE);
    int schema = header.getInt(7 * Integer.BYTES);
    rejectInt(bytes, schema, Integer.MAX_VALUE);
    rejectInt(bytes, schema + Integer.BYTES, Integer.MAX_VALUE);
    int fixed = header.getInt(9 * Integer.BYTES);
    rejectInt(bytes, fixed, Integer.MAX_VALUE);
    int metadata = header.getInt(11 * Integer.BYTES) + header.getInt(12 * Integer.BYTES);
    rejectInt(bytes, metadata, Integer.MAX_VALUE);
    rejectInt(bytes, metadata + Integer.BYTES, Integer.MAX_VALUE);
    rejectInt(bytes, metadata + 2 * Integer.BYTES, Integer.MAX_VALUE);
    ByteBuffer metadataEntries = ByteBuffer.wrap(bytes);
    metadataEntries.position(metadata + 2 * Integer.BYTES);
    while (metadataEntries.hasRemaining()) {
      MetadataKey key = MetadataKey.getById(metadataEntries.getInt());
      if (key == MetadataKey.TRACE_INFO) {
        rejectInt(bytes, metadataEntries.position(), Integer.MAX_VALUE);
        break;
      }
      assertEquals(key.getValueType(), DataTable.MetadataValueType.LONG);
      metadataEntries.getLong();
    }
    for (int size : new int[]{0, 4, 51, bytes.length - 1}) {
      assertThrows(IOException.class, () -> NativeDataTableDecoder.decode(ByteBuffer.wrap(bytes, 0, size)));
    }
  }

  @Test
  public void testRejectVariableReferencesAndUnsupportedCodecs() throws IOException {
    for (ColumnDataType type : List.of(ColumnDataType.INT_ARRAY, ColumnDataType.BYTES_ARRAY,
        ColumnDataType.BIG_DECIMAL_ARRAY)) {
      DataTableBuilder builder = DataTableBuilderFactory.getDataTableBuilder(
          new DataSchema(new String[]{"value"}, new ColumnDataType[]{type}));
      builder.startRow();
      if (type == ColumnDataType.INT_ARRAY) {
        builder.setColumn(0, new int[]{1});
      } else if (type == ColumnDataType.BYTES_ARRAY) {
        builder.setColumn(0, new ByteArray[]{new ByteArray(new byte[]{1})});
      } else {
        builder.setColumn(0, new BigDecimal[]{BigDecimal.ONE});
      }
      builder.finishRow();
      byte[] bytes = builder.build().toBytes();
      ByteBuffer header = ByteBuffer.wrap(bytes);
      int fixed = header.getInt(9 * Integer.BYTES);
      rejectInt(bytes, fixed, -1);
      rejectInt(bytes, fixed + Integer.BYTES, Integer.MAX_VALUE);
      if (type != ColumnDataType.INT_ARRAY) {
        rejectInt(bytes, header.getInt(11 * Integer.BYTES), Integer.MAX_VALUE);
      }
    }
    for (ColumnDataType type : List.of(ColumnDataType.MAP, ColumnDataType.OBJECT, ColumnDataType.UNKNOWN)) {
      DataTable table = DataTableBuilderFactory.getDataTableBuilder(
          new DataSchema(new String[]{"value"}, new ColumnDataType[]{type})).build();
      assertThrows(IOException.class, () -> NativeDataTableDecoder.decode(ByteBuffer.wrap(table.toBytes())));
    }
    DataSchema schema = new DataSchema(new String[]{"value"}, new ColumnDataType[]{ColumnDataType.BYTES});
    ByteBuffer fixed = ByteBuffer.allocate(65 * 8);
    for (int i = 0; i < 65; i++) {
      fixed.putInt(0).putInt(1024 * 1024);
    }
    byte[][] sections = {new byte[4], new byte[4], schema.toBytes(), fixed.array(), new byte[1024 * 1024]};
    int next = 13 * Integer.BYTES;
    ByteBuffer input = ByteBuffer.allocate(next + 8 + List.of(sections).stream().mapToInt(s -> s.length).sum());
    input.putInt(4).putInt(65).putInt(1);
    for (byte[] section : sections) {
      input.putInt(next).putInt(section.length);
      next += section.length;
    }
    for (byte[] section : sections) {
      input.put(section);
    }
    input.putInt(4).putInt(0).flip();
    assertThrows(IOException.class, () -> NativeDataTableDecoder.decode(input));
  }

  private static void rejectInt(byte[] original, int offset, int value) {
    byte[] malformed = original.clone();
    ByteBuffer.wrap(malformed).putInt(offset, value);
    assertThrows(IOException.class, () -> NativeDataTableDecoder.decode(ByteBuffer.wrap(malformed)));
  }
}
