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
package org.apache.pinot.segment.local.segment.index.map;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.segment.local.segment.creator.impl.map.DenseMapHeader;
import org.apache.pinot.segment.local.segment.creator.impl.map.MapIndexHeader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadataImpl;
import org.apache.pinot.segment.spi.memory.ByteBufferPinotBufferFactory;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.Assert;
import org.testng.annotations.Test;


public class HeaderTest {
  @Test
  public void testSize() {
    MapIndexHeader.HeaderSizeComputer writer = new MapIndexHeader.HeaderSizeComputer();

    long offset = writer.putByte(0L, (byte) 1);
    Assert.assertEquals(writer.size(), 1);

    offset = writer.putInt(offset, 5);
    Assert.assertEquals(writer.size(), 5);

    offset = writer.putLong(offset, 5L);
    Assert.assertEquals(writer.size(), 13);

    offset = writer.putFloat(offset, 1.0F);
    Assert.assertEquals(writer.size(), 17);

    offset = writer.putDouble(offset, 1.0D);
    Assert.assertEquals(writer.size(), 25);

    offset = writer.putString(offset, "Hello");
    Assert.assertEquals(writer.size(), 25 + 4 + 10);

    offset = writer.putValue(offset, FieldSpec.DataType.STRING, "Test");
    Assert.assertEquals(writer.size(), 39 + 4 + 8);

    offset = writer.putValue(offset, FieldSpec.DataType.INT, 1);
    Assert.assertEquals(writer.size(), 51 + 4);

    offset = writer.putValue(offset, FieldSpec.DataType.LONG, 1L);
    Assert.assertEquals(writer.size(), 55 + 8);

    offset = writer.putValue(offset, FieldSpec.DataType.FLOAT, 1.0F);
    Assert.assertEquals(writer.size(), 63 + 4);

    offset = writer.putValue(offset, FieldSpec.DataType.DOUBLE, 1.0D);
    Assert.assertEquals(writer.size(), 67 + 8);
  }

  @Test
  public void testWritingColumnMetadataSize() {
    ColumnMetadata meta = createIntKeyMetadata(new DimensionFieldSpec("test", FieldSpec.DataType.INT, true));
    DenseMapHeader.DenseKeyMetadata dk = new DenseMapHeader.DenseKeyMetadata(
        "test",
        meta,
        Map.of(StandardIndexes.forward(), -1L),
        5
    );

    /* Size computation

    Data Type: 4 + 6 | 10
    Is SV: 1 | 11
    has dict : 1 | 12
    is sorted: 1 | 13
    total docs: 4 | 17
    card: 4 | 21
    max length: 4 | 25
    bits per: 4 | 29
    max mvs: 4 | 33
    total entries: 4 | 37
    min value: 4 | 41
    max value: 4 | 45
    minmax invalid: 1 | 46
    number indexes: 4 | 50

    Index Size:
      Forward Index:
        ID ("forward_index"): 4 + 26 | 80
        Size: 8 | 88

    Total Size: 88
     */

    MapIndexHeader.HeaderSizeComputer writer = new MapIndexHeader.HeaderSizeComputer();
    dk.writeColumnMetadata(writer, 0);
    Assert.assertEquals(writer.size(), 88);
  }

  @Test
  public void testDenseKeyMetadataSize() throws IOException {
    ColumnMetadata meta = createIntKeyMetadata(new DimensionFieldSpec("test", FieldSpec.DataType.INT, true));
    DenseMapHeader.DenseKeyMetadata dk = new DenseMapHeader.DenseKeyMetadata(
        "test",
        meta,
        Map.of(StandardIndexes.forward(), 100L),
        5
    );

    /*
    Key ("test"): 4 + 8
    Doc Id offset: 4
    Number of Indices : 4
    Index ID ("forward_index"): 4 + 26
    Index Offset: 8
    Col Metadata: 88

    Total: 12 + 4 + 4 + 30 + 8 + 88 = 146
     */

    MapIndexHeader.HeaderSizeComputer writer = new MapIndexHeader.HeaderSizeComputer();
    dk.write(writer, 0);
    Assert.assertEquals(writer.size(), 146);
  }

  @Test
  public void testDenseMapMetadataSize() throws IOException {
    ColumnMetadata meta = createIntKeyMetadata(new DimensionFieldSpec("test", FieldSpec.DataType.INT, true));
    DenseMapHeader.DenseKeyMetadata dk = new DenseMapHeader.DenseKeyMetadata(
        "test",
        meta,
        Map.of(StandardIndexes.forward(), 100L),
        5
    );
    ColumnMetadata meta2 = createIntKeyMetadata(new DimensionFieldSpec("test2", FieldSpec.DataType.INT, true));
    DenseMapHeader.DenseKeyMetadata dk2 = new DenseMapHeader.DenseKeyMetadata(
        "test2",
        meta2,
        Map.of(StandardIndexes.forward(), 100L),
        500
    );

    MapIndexHeader.HeaderSizeComputer writer = new MapIndexHeader.HeaderSizeComputer();
    dk.write(writer, 0);
    Assert.assertEquals(writer.size(), 146);

    MapIndexHeader.HeaderSizeComputer writer2 = new MapIndexHeader.HeaderSizeComputer();
    dk2.write(writer2, 0);
    Assert.assertEquals(writer2.size(), 148);

    DenseMapHeader mapMD = new DenseMapHeader();
    mapMD.addKey("test", meta, List.of(StandardIndexes.forward()), 5);
    mapMD.addKey("test2", meta2, List.of(StandardIndexes.forward()), 500);

    /*
    Size:

    Index type ("dense_map_index"): 4 + 30
    Num Keys: 4

    Key ('test'): 146
    Key ('test2'): 148

    total = 146 + 148 + 34 + 4 = 332
     */

    MapIndexHeader.HeaderSizeComputer writer3 = new MapIndexHeader.HeaderSizeComputer();
    mapMD.write(writer3, 0);
    Assert.assertEquals(writer3.size(), 332);
  }

  @Test
  public void testWriterMapIndexHeader() throws IOException {
    ColumnMetadata meta = createIntKeyMetadata(new DimensionFieldSpec("test", FieldSpec.DataType.INT, true));
    DenseMapHeader.DenseKeyMetadata dk = new DenseMapHeader.DenseKeyMetadata(
        "test",
        meta,
        Map.of(StandardIndexes.forward(), 100L),
        5
    );
    ColumnMetadata meta2 = createIntKeyMetadata(new DimensionFieldSpec("test2", FieldSpec.DataType.INT, true));
    DenseMapHeader.DenseKeyMetadata dk2 = new DenseMapHeader.DenseKeyMetadata(
        "test2",
        meta2,
        Map.of(StandardIndexes.forward(), 100L),
        500
    );


    DenseMapHeader mapMD = new DenseMapHeader();
    mapMD.addKey("test", meta, List.of(StandardIndexes.forward()), 5);
    mapMD.addKey("test2", meta2, List.of(StandardIndexes.forward()), 500);

    MapIndexHeader header = new MapIndexHeader();
    header.addMapIndex(mapMD);

    /*
    Size:

   Version: 4
   number indexes: 4
    Dense Map: 332
     */

    Assert.assertEquals(header.size(), 340);
  }

  @Test
  public void testWritingColumnMetadata() {
    ColumnMetadata meta = createIntKeyMetadata(new DimensionFieldSpec("test", FieldSpec.DataType.INT, true));
    DenseMapHeader.DenseKeyMetadata dk = new DenseMapHeader.DenseKeyMetadata(
        "test",
        meta,
        Map.of(StandardIndexes.forward(), 100L),
        5
    );
    MapIndexHeader.HeaderSizeComputer writer = new MapIndexHeader.HeaderSizeComputer();
    dk.writeColumnMetadata(writer, 0);
    long size = writer.size();

    PinotDataBuffer buffer = new ByteBufferPinotBufferFactory().allocateDirect(size, ByteOrder.BIG_ENDIAN);
    MapIndexHeader.PinotDataBufferWriter bufferWriter = new MapIndexHeader.PinotDataBufferWriter(buffer);
    dk.writeColumnMetadata(bufferWriter, 0);
    buffer.flush();

    Pair<ColumnMetadata, Integer> actualMD = DenseMapHeader.readColumnMetadata(
        buffer, 0, "test");
    Assert.assertEquals(actualMD.getLeft(), meta);
  }

  @Test
  public void testWritingDenseKeyMetadata() throws IOException {
    ColumnMetadata meta = createIntKeyMetadata(new DimensionFieldSpec("test", FieldSpec.DataType.INT, true));
    DenseMapHeader.DenseKeyMetadata dk = new DenseMapHeader.DenseKeyMetadata(
        "test",
        meta,
        Map.of(StandardIndexes.forward(), 100L),
        5
    );
    MapIndexHeader.HeaderSizeComputer writer = new MapIndexHeader.HeaderSizeComputer();
    dk.write(writer, 0);
    long size = writer.size();

    PinotDataBuffer buffer = new ByteBufferPinotBufferFactory().allocateDirect(size, ByteOrder.BIG_ENDIAN);

    MapIndexHeader.PinotDataBufferWriter bufferWriter = new MapIndexHeader.PinotDataBufferWriter(buffer);
    dk.write(bufferWriter, 0);
    buffer.flush();

    Pair<DenseMapHeader.DenseKeyMetadata, Integer> result =
        DenseMapHeader.DenseKeyMetadata.read(buffer, 0);
    Assert.assertEquals(result.getLeft(), dk);
    Assert.assertEquals(result.getLeft().getIndexOffset(StandardIndexes.forward()), 100L);
  }

  @Test
  public void testUpdatingIndexOffsetDenseKeyMetadata() throws IOException {
    ColumnMetadata meta = createIntKeyMetadata(new DimensionFieldSpec("test", FieldSpec.DataType.INT, true));
    DenseMapHeader.DenseKeyMetadata dk = new DenseMapHeader.DenseKeyMetadata(
        "test",
        meta,
        List.of(StandardIndexes.forward()),
        5
    );
    MapIndexHeader.HeaderSizeComputer writer = new MapIndexHeader.HeaderSizeComputer();
    dk.write(writer, 0);
    long size = writer.size();

    PinotDataBuffer buffer = new ByteBufferPinotBufferFactory().allocateDirect(size, ByteOrder.BIG_ENDIAN);

    MapIndexHeader.PinotDataBufferWriter bufferWriter = new MapIndexHeader.PinotDataBufferWriter(buffer);
    dk.write(bufferWriter, 0);
    buffer.flush();

    {
      Pair<DenseMapHeader.DenseKeyMetadata, Integer> result
          = DenseMapHeader.DenseKeyMetadata.read(buffer, 0);
      Assert.assertEquals(result.getLeft(), dk);
      Assert.assertEquals(result.getLeft().getIndexOffset(StandardIndexes.forward()), 0xDEADBEEFDEADBEEFL);
    }

    dk.setIndexOffset(bufferWriter, StandardIndexes.forward(), 100L);
    buffer.flush();

    {
      Pair<DenseMapHeader.DenseKeyMetadata, Integer> result
          = DenseMapHeader.DenseKeyMetadata.read(buffer, 0);
      Assert.assertEquals(result.getLeft(), dk);
      Assert.assertEquals(result.getLeft().getIndexOffset(StandardIndexes.forward()), 100L);
    }
  }

  @Test
  public void testDenseMapMetadataWrite() throws IOException {
    ColumnMetadata meta = createIntKeyMetadata(new DimensionFieldSpec("test", FieldSpec.DataType.INT, true));
    ColumnMetadata meta2 = createIntKeyMetadata(new DimensionFieldSpec("test2", FieldSpec.DataType.INT, true));

    DenseMapHeader mapMD = new DenseMapHeader();
    mapMD.addKey("test", meta, List.of(StandardIndexes.forward()), 5);
    mapMD.addKey("test2", meta2, List.of(StandardIndexes.forward(), StandardIndexes.range()), 500);

    MapIndexHeader.HeaderSizeComputer sizer = new MapIndexHeader.HeaderSizeComputer();
    mapMD.write(sizer, 0);

    PinotDataBuffer buffer = new ByteBufferPinotBufferFactory().allocateDirect(sizer.size(), ByteOrder.BIG_ENDIAN);

    MapIndexHeader.PinotDataBufferWriter bufferWriter = new MapIndexHeader.PinotDataBufferWriter(buffer);
    mapMD.write(bufferWriter, 0);
    buffer.flush();

    Pair<DenseMapHeader, Integer> result =
        DenseMapHeader.read(buffer, 0);
    Assert.assertEquals(result.getLeft(), mapMD);
  }

  @Test
  public void testMapHeaderWrite() throws IOException {
    ColumnMetadata meta = createIntKeyMetadata(new DimensionFieldSpec("test", FieldSpec.DataType.INT, true));
    ColumnMetadata meta2 = createIntKeyMetadata(new DimensionFieldSpec("test2", FieldSpec.DataType.INT, true));
    ColumnMetadata meta3 = createLongKeyMetadata(new DimensionFieldSpec("test3", FieldSpec.DataType.LONG, true));

    DenseMapHeader mapMD = new DenseMapHeader();
    mapMD.addKey("test", meta, List.of(StandardIndexes.forward()), 5);
    mapMD.addKey("test2", meta2, List.of(StandardIndexes.forward(), StandardIndexes.range()), 500);
    mapMD.addKey("test3", meta3, List.of(StandardIndexes.forward(), StandardIndexes.range()), 1500);

    MapIndexHeader header = new MapIndexHeader();
    header.addMapIndex(mapMD);

    long size = header.size();
    PinotDataBuffer buffer = new ByteBufferPinotBufferFactory().allocateDirect(size, ByteOrder.BIG_ENDIAN);

    header.write(buffer, 0);
    buffer.flush();

    Pair<MapIndexHeader, Integer> result = MapIndexHeader.read(buffer, 0);
    Assert.assertEquals(result.getLeft(), header);
  }

  private ColumnMetadata createIntKeyMetadata(FieldSpec spec) {
    HashMap<IndexType<?, ?, ?>, Long> indexSizeMap = new HashMap<>();
    indexSizeMap.put(StandardIndexes.forward(), 100L);
    ColumnMetadataImpl.Builder builder = ColumnMetadataImpl.builder()
        .setFieldSpec(spec)
        .setCardinality(1)
        .setTotalDocs(10)
        .setAutoGenerated(false)
        .setBitsPerElement(5)
        .setMinValue(0)
        .setMaxValue(1000)
        .setHasDictionary(false)
        .setSorted(false)
        .setMaxNumberOfMultiValues(0)
        .setColumnMaxLength(4)
        .setPartitions(null)
        .setPartitionFunction(null)
        .setMinMaxValueInvalid(false)
        .setTotalNumberOfEntries(5);
    builder.setIndexSizeMap(indexSizeMap);

    return builder.build();
  }

  private ColumnMetadata createLongKeyMetadata(FieldSpec spec) {
    HashMap<IndexType<?, ?, ?>, Long> indexSizeMap = new HashMap<>();
    indexSizeMap.put(StandardIndexes.forward(), 100L);
    ColumnMetadataImpl.Builder builder = ColumnMetadataImpl.builder()
        .setFieldSpec(spec)
        .setCardinality(1)
        .setTotalDocs(10)
        .setAutoGenerated(false)
        .setBitsPerElement(5)
        .setMinValue(0L)
        .setMaxValue(1000L)
        .setHasDictionary(false)
        .setSorted(false)
        .setMaxNumberOfMultiValues(0)
        .setColumnMaxLength(4)
        .setPartitions(null)
        .setPartitionFunction(null)
        .setMinMaxValueInvalid(false)
        .setTotalNumberOfEntries(5);
    builder.setIndexSizeMap(indexSizeMap);

    return builder.build();
  }
}
