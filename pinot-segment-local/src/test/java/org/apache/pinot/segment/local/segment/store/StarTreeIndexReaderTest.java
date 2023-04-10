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
package org.apache.pinot.segment.local.segment.store;

import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.segment.local.startree.v2.store.StarTreeIndexMapUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Constants;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Metadata;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class StarTreeIndexReaderTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), StarTreeIndexReaderTest.class.toString());

  private SegmentMetadataImpl _segmentMetadata;

  @BeforeMethod
  public void setUp()
      throws IOException {
    TestUtils.ensureDirectoriesExistAndEmpty(TEMP_DIR);
    writeMetadata();
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  void writeMetadata() {
    SegmentMetadataImpl meta = mock(SegmentMetadataImpl.class);
    when(meta.getVersion()).thenReturn(SegmentVersion.v3);
    when(meta.getStarTreeV2MetadataList()).thenReturn(null);
    _segmentMetadata = meta;
  }

  @Test
  public void testLoadStarTreeIndexBuffers()
      throws IOException {
    // Test with 2 index trees.
    StarTreeV2Metadata stMeta1 = mock(StarTreeV2Metadata.class);
    when(stMeta1.getDimensionsSplitOrder()).thenReturn(Arrays.asList("dim0", "dim1"));
    when(stMeta1.getFunctionColumnPairs()).thenReturn(
        Collections.singleton(new AggregationFunctionColumnPair(AggregationFunctionType.COUNT, "*")));
    StarTreeV2Metadata stMeta2 = mock(StarTreeV2Metadata.class);
    when(stMeta2.getDimensionsSplitOrder()).thenReturn(Arrays.asList("dimX", "dimY"));
    when(stMeta2.getFunctionColumnPairs()).thenReturn(
        Collections.singleton(new AggregationFunctionColumnPair(AggregationFunctionType.SUM, "dimX")));
    when(_segmentMetadata.getStarTreeV2MetadataList()).thenReturn(Arrays.asList(stMeta1, stMeta2));
    // Mock the offset/sizes for the index buffers.
    List<List<Pair<StarTreeIndexMapUtils.IndexKey, StarTreeIndexMapUtils.IndexValue>>> indexMaps = new ArrayList<>();
    List<Pair<StarTreeIndexMapUtils.IndexKey, StarTreeIndexMapUtils.IndexValue>> indexMap = new ArrayList<>();
    indexMap.add(Pair.of(new StarTreeIndexMapUtils.IndexKey(StarTreeIndexMapUtils.IndexType.STAR_TREE, null),
        new StarTreeIndexMapUtils.IndexValue(0, 1)));
    indexMap.add(Pair.of(new StarTreeIndexMapUtils.IndexKey(StarTreeIndexMapUtils.IndexType.FORWARD_INDEX, "dim0"),
        new StarTreeIndexMapUtils.IndexValue(1, 1)));
    indexMap.add(Pair.of(new StarTreeIndexMapUtils.IndexKey(StarTreeIndexMapUtils.IndexType.FORWARD_INDEX, "dim1"),
        new StarTreeIndexMapUtils.IndexValue(2, 1)));
    indexMap.add(Pair.of(new StarTreeIndexMapUtils.IndexKey(StarTreeIndexMapUtils.IndexType.FORWARD_INDEX, "count__*"),
        new StarTreeIndexMapUtils.IndexValue(3, 1)));
    indexMaps.add(indexMap);
    indexMap = new ArrayList<>();
    indexMap.add(Pair.of(new StarTreeIndexMapUtils.IndexKey(StarTreeIndexMapUtils.IndexType.STAR_TREE, null),
        new StarTreeIndexMapUtils.IndexValue(10, 3)));
    indexMap.add(Pair.of(new StarTreeIndexMapUtils.IndexKey(StarTreeIndexMapUtils.IndexType.FORWARD_INDEX, "dimX"),
        new StarTreeIndexMapUtils.IndexValue(13, 3)));
    indexMap.add(Pair.of(new StarTreeIndexMapUtils.IndexKey(StarTreeIndexMapUtils.IndexType.FORWARD_INDEX, "dimY"),
        new StarTreeIndexMapUtils.IndexValue(16, 3)));
    indexMap.add(Pair.of(new StarTreeIndexMapUtils.IndexKey(StarTreeIndexMapUtils.IndexType.FORWARD_INDEX, "sum__dimX"),
        new StarTreeIndexMapUtils.IndexValue(19, 3)));
    indexMaps.add(indexMap);
    File indexMapFile = new File(TEMP_DIR, StarTreeV2Constants.INDEX_MAP_FILE_NAME);
    StarTreeIndexMapUtils.storeToFile(indexMaps, indexMapFile);

    File indexFile = new File(TEMP_DIR, StarTreeV2Constants.INDEX_FILE_NAME);
    if (!indexFile.exists()) {
      indexFile.createNewFile();
    }
    byte[] data = new byte[32];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) i;
    }
    try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapFile(indexFile, false, 0, data.length, ByteOrder.LITTLE_ENDIAN,
        "StarTree V2 data buffer from: " + indexFile)) {
      dataBuffer.readFrom(0, data);
    }

    try (StarTreeIndexReader reader = new StarTreeIndexReader(TEMP_DIR, _segmentMetadata, ReadMode.mmap)) {
      // Check the bytes of the 1st ST index
      assertTrue(reader.hasIndexFor(0, "0", StandardIndexes.inverted()));
      PinotDataBuffer buf = reader.getBuffer(0, "0", StandardIndexes.inverted());
      assertEquals(buf.size(), 1);
      assertEquals(buf.getByte(0), 0);

      assertTrue(reader.hasIndexFor(0, "dim0", StandardIndexes.forward()));
      buf = reader.getBuffer(0, "dim0", StandardIndexes.forward());
      assertEquals(buf.size(), 1);
      assertEquals(buf.getByte(0), 1);

      assertTrue(reader.hasIndexFor(0, "dim1", StandardIndexes.forward()));
      buf = reader.getBuffer(0, "dim1", StandardIndexes.forward());
      assertEquals(buf.size(), 1);
      assertEquals(buf.getByte(0), 2);

      assertTrue(reader.hasIndexFor(0, "count__*", StandardIndexes.forward()));
      buf = reader.getBuffer(0, "count__*", StandardIndexes.forward());
      assertEquals(buf.size(), 1);
      assertEquals(buf.getByte(0), 3);

      // Check the bytes of the 2nd ST index
      assertTrue(reader.hasIndexFor(1, "1", StandardIndexes.inverted()));
      buf = reader.getBuffer(1, "1", StandardIndexes.inverted());
      assertEquals(buf.size(), 3);
      assertEquals(buf.getByte(2), 12);

      assertTrue(reader.hasIndexFor(1, "dimX", StandardIndexes.forward()));
      buf = reader.getBuffer(1, "dimX", StandardIndexes.forward());
      assertEquals(buf.size(), 3);
      assertEquals(buf.getByte(2), 15);

      assertTrue(reader.hasIndexFor(1, "dimY", StandardIndexes.forward()));
      buf = reader.getBuffer(1, "dimY", StandardIndexes.forward());
      assertEquals(buf.size(), 3);
      assertEquals(buf.getByte(2), 18);

      assertTrue(reader.hasIndexFor(1, "sum__dimX", StandardIndexes.forward()));
      buf = reader.getBuffer(1, "sum__dimX", StandardIndexes.forward());
      assertEquals(buf.size(), 3);
      assertEquals(buf.getByte(2), 21);
    }
  }
}
