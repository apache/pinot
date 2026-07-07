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
package org.apache.pinot.segment.local.segment.index.loader.bloomfilter;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.bloom.OnHeapGuavaBloomFilterCreator;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.BloomFilterConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Unit tests for {@link BloomFilterHandler} fpp-change detection.
 *
 * <p>The key invariant being tested: fpp-change detection reads the effective fpp stored directly
 * in the v2 bloom filter header, rather than inferring it from Guava-internal fields (numHashFunctions,
 * numLongs).  This avoids a fragile dependency on Guava's internal serialisation layout.
 *
 * <p>Bloom filter file format versions:
 * <ul>
 *   <li><b>v1 (legacy):</b> {@code [TYPE_VALUE=1 (int)][VERSION (int)][Guava bytes...]} — fpp not stored.</li>
 *   <li><b>v2:</b> {@code [TYPE_VALUE_V2=2 (int)][VERSION (int)][FPP (double)][Guava bytes...]} — fpp stored
 *       explicitly at byte offset {@link OnHeapGuavaBloomFilterCreator#FPP_OFFSET}.</li>
 * </ul>
 *
 * <p>Legacy v1 segments skip fpp-change detection (return false) because the fpp is absent from the
 * header.  They are upgraded to v2 the next time the bloom filter is rebuilt for any other reason.
 */
public class BloomFilterHandlerTest {
  private static final String COLUMN = "myBloomCol";
  private static final int CARDINALITY = 1000;

  private static final double FPP_STORED = 0.05;
  private static final double FPP_DIFFERENT = 0.01;

  private File _tempDir;
  private File _bloomFilterFile;

  @BeforeMethod
  public void setUp()
      throws Exception {
    _tempDir = new File(FileUtils.getTempDirectory(), "bloom-filter-handler-test-" + System.nanoTime());
    assertTrue(_tempDir.mkdirs());
    _bloomFilterFile = new File(_tempDir, "bloom.tmp");
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(_tempDir);
  }

  /**
   * Legacy v1 segments do not carry fpp in the header; fpp-change detection must be skipped regardless
   * of what the current config says.  The segment will be upgraded to v2 on its next rebuild.
   */
  @Test
  public void testV1LegacyFileSkipsFppChangeDetection()
      throws Exception {
    writeV1BloomFilter(CARDINALITY, FPP_STORED);

    // Config uses a clearly different fpp, but because the file is v1 the handler must not trigger a rebuild.
    BloomFilterHandler handler = createHandler(COLUMN, new BloomFilterConfig(FPP_DIFFERENT, 0, false));
    SegmentDirectory.Reader reader = mockReaderWithBloomFilter(COLUMN, _bloomFilterFile);

    assertFalse(handler.needUpdateIndices(reader),
        "No rebuild expected for legacy v1 segments — fpp is not stored in the header");
  }

  /**
   * Sanity check: when the v2 header fpp matches the current config, the handler must not trigger a rebuild.
   */
  @Test
  public void testNeedUpdateReturnsFalseWhenFppUnchangedV2()
      throws Exception {
    writeV2BloomFilter(CARDINALITY, FPP_STORED);

    BloomFilterHandler handler = createHandler(COLUMN, new BloomFilterConfig(FPP_STORED, 0, false));
    SegmentDirectory.Reader reader = mockReaderWithBloomFilter(COLUMN, _bloomFilterFile);

    assertFalse(handler.needUpdateIndices(reader),
        "No rebuild expected when stored v2 fpp matches the current config");
  }

  /**
   * When the v2 header fpp differs from the current config, the handler must trigger a rebuild.
   */
  @Test
  public void testNeedUpdateReturnsTrueWhenFppChangedV2()
      throws Exception {
    writeV2BloomFilter(CARDINALITY, FPP_STORED);

    BloomFilterHandler handler = createHandler(COLUMN, new BloomFilterConfig(FPP_DIFFERENT, 0, false));
    SegmentDirectory.Reader reader = mockReaderWithBloomFilter(COLUMN, _bloomFilterFile);

    assertTrue(handler.needUpdateIndices(reader),
        "Rebuild expected when stored v2 fpp differs from the current config");
  }

  // --- helpers ---

  /**
   * Writes a legacy v1 Pinot bloom filter file: {@code [TYPE_VALUE=1 (int)][VERSION (int)][Guava bytes]}.
   */
  private void writeV1BloomFilter(int cardinality, double fpp)
      throws Exception {
    BloomFilter<String> bf = BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8), cardinality, fpp);
    try (DataOutputStream out = new DataOutputStream(new FileOutputStream(_bloomFilterFile))) {
      out.writeInt(OnHeapGuavaBloomFilterCreator.TYPE_VALUE);
      out.writeInt(OnHeapGuavaBloomFilterCreator.VERSION);
      bf.writeTo(out);
    }
  }

  /**
   * Writes a v2 Pinot bloom filter file:
   * {@code [TYPE_VALUE_V2=2 (int)][VERSION (int)][FPP (double)][Guava bytes]}.
   */
  private void writeV2BloomFilter(int cardinality, double fpp)
      throws Exception {
    BloomFilter<String> bf = BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8), cardinality, fpp);
    try (DataOutputStream out = new DataOutputStream(new FileOutputStream(_bloomFilterFile))) {
      out.writeInt(OnHeapGuavaBloomFilterCreator.TYPE_VALUE_V2);
      out.writeInt(OnHeapGuavaBloomFilterCreator.VERSION);
      out.writeDouble(fpp);
      bf.writeTo(out);
    }
  }

  private BloomFilterHandler createHandler(String columnName, BloomFilterConfig bloomFilterConfig) {
    FieldIndexConfigs fieldIndexConfigs =
        new FieldIndexConfigs.Builder().add(StandardIndexes.bloomFilter(), bloomFilterConfig).build();

    ColumnMetadata columnMetadata = mock(ColumnMetadata.class);
    when(columnMetadata.getCardinality()).thenReturn(CARDINALITY);
    when(columnMetadata.getTotalNumberOfEntries()).thenReturn(CARDINALITY);

    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    when(segmentMetadata.getName()).thenReturn("testSegment");
    when(segmentMetadata.getColumnMetadataFor(columnName)).thenReturn(columnMetadata);

    SegmentDirectory segmentDirectory = mock(SegmentDirectory.class);
    when(segmentDirectory.getSegmentMetadata()).thenReturn(segmentMetadata);

    return new BloomFilterHandler(segmentDirectory, Map.of(columnName, fieldIndexConfigs),
        mock(TableConfig.class), mock(Schema.class));
  }

  private SegmentDirectory.Reader mockReaderWithBloomFilter(String columnName, File bloomFilterFile)
      throws Exception {
    PinotDataBuffer dataBuffer = PinotDataBuffer.loadBigEndianFile(bloomFilterFile);

    SegmentDirectory segDir = mock(SegmentDirectory.class);
    when(segDir.getColumnsWithIndex(StandardIndexes.bloomFilter())).thenReturn(Set.of(columnName));

    SegmentDirectory.Reader reader = mock(SegmentDirectory.Reader.class);
    when(reader.toSegmentDirectory()).thenReturn(segDir);
    when(reader.getIndexFor(eq(columnName), eq(StandardIndexes.bloomFilter()))).thenReturn(dataBuffer);

    return reader;
  }
}
