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
import java.util.TreeSet;
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
 * <p>Bloom filter file format versions:
 * <ul>
 *   <li><b>v2 (current write format):</b>
 *       {@code [TYPE_VALUE=1 (int)][VERSION_V2=2 (int)][FPP (double, 8 B)][Guava bytes...]} — fpp stored
 *       explicitly at byte offset {@link OnHeapGuavaBloomFilterCreator#FPP_OFFSET}.
 *       Written by {@link OnHeapGuavaBloomFilterCreator#seal()}; fpp change detection is exact.</li>
 *   <li><b>v1 (legacy):</b>
 *       {@code [TYPE_VALUE=1 (int)][VERSION=1 (int)][Guava bytes...]} — fpp not stored.
 *       Fpp change detection is skipped for v1 segments; they upgrade to v2 on the next rebuild.</li>
 * </ul>
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

  // ---------------------------------------------------------------------------
  // V2 (current write format) — fpp stored explicitly in header
  // ---------------------------------------------------------------------------

  /**
   * V2 file with the same fpp as the current config — no rebuild needed.
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
   * V2 file with a different fpp from the current config — rebuild must be triggered.
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

  /**
   * Regression guard for same-k fpp transitions: fpp values that round to the same {@code numHashFunctions}
   * (e.g. 0.05 and 0.045 both yield k=4 for this cardinality) would not have been detected by any
   * numHashFunctions-only comparison. V2 stores the exact fpp, so the rebuild is correctly triggered.
   */
  @Test
  public void testNeedUpdateReturnsTrueForSameKFppChangeV2()
      throws Exception {
    // 0.05 and 0.045 both produce k=4 for large cardinality — same numHashFunctions, different bit-array size
    writeV2BloomFilter(CARDINALITY, 0.05);

    BloomFilterHandler handler = createHandler(COLUMN, new BloomFilterConfig(0.045, 0, false));
    SegmentDirectory.Reader reader = mockReaderWithBloomFilter(COLUMN, _bloomFilterFile);

    assertTrue(handler.needUpdateIndices(reader),
        "Rebuild expected even for same-k fpp transition because v2 stores the exact fpp");
  }

  // ---------------------------------------------------------------------------
  // V1 (legacy) — fpp not stored in header; fpp change detection skipped
  // ---------------------------------------------------------------------------

  /**
   * V1 file with the same fpp as the current config — no rebuild (fpp detection skipped for v1).
   */
  @Test
  public void testNeedUpdateReturnsFalseForV1SameFpp()
      throws Exception {
    writeV1BloomFilter(CARDINALITY, FPP_STORED);

    BloomFilterHandler handler = createHandler(COLUMN, new BloomFilterConfig(FPP_STORED, 0, false));
    SegmentDirectory.Reader reader = mockReaderWithBloomFilter(COLUMN, _bloomFilterFile);

    assertFalse(handler.needUpdateIndices(reader),
        "No rebuild expected for v1 segment — fpp detection is skipped for legacy indexes");
  }

  /**
   * V1 file with a different fpp from the current config — still no rebuild because fpp detection
   * is skipped for v1 segments (no fpp header to compare against).
   * The segment upgrades to v2 the next time it is rebuilt for other reasons.
   */
  @Test
  public void testNeedUpdateReturnsFalseForV1DifferentFpp()
      throws Exception {
    writeV1BloomFilter(CARDINALITY, FPP_STORED);

    BloomFilterHandler handler = createHandler(COLUMN, new BloomFilterConfig(FPP_DIFFERENT, 0, false));
    SegmentDirectory.Reader reader = mockReaderWithBloomFilter(COLUMN, _bloomFilterFile);

    assertFalse(handler.needUpdateIndices(reader),
        "No rebuild expected for v1 segment even when fpp changes — v1 segments cannot detect fpp changes");
  }

  // --- helpers ---

  /**
   * Writes a v1 Pinot bloom filter file: {@code [TYPE_VALUE=1 (int)][VERSION=1 (int)][Guava bytes]}.
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
   * {@code [TYPE_VALUE=1 (int)][VERSION_V2=2 (int)][FPP (double)][Guava bytes]}.
   */
  private void writeV2BloomFilter(int cardinality, double fpp)
      throws Exception {
    BloomFilter<String> bf = BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8), cardinality, fpp);
    try (DataOutputStream out = new DataOutputStream(new FileOutputStream(_bloomFilterFile))) {
      out.writeInt(OnHeapGuavaBloomFilterCreator.TYPE_VALUE);
      out.writeInt(OnHeapGuavaBloomFilterCreator.VERSION_V2);
      out.writeDouble(fpp);
      bf.writeTo(out);
    }
  }

  private BloomFilterHandler createHandler(String columnName, BloomFilterConfig bloomFilterConfig) {
    return createHandlerWithCardinality(columnName, bloomFilterConfig, CARDINALITY);
  }

  private BloomFilterHandler createHandlerWithCardinality(String columnName, BloomFilterConfig bloomFilterConfig,
      int cardinality) {
    FieldIndexConfigs fieldIndexConfigs =
        new FieldIndexConfigs.Builder().add(StandardIndexes.bloomFilter(), bloomFilterConfig).build();

    ColumnMetadata columnMetadata = mock(ColumnMetadata.class);
    when(columnMetadata.getCardinality()).thenReturn(cardinality);
    when(columnMetadata.getTotalNumberOfEntries()).thenReturn(cardinality);

    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    when(segmentMetadata.getName()).thenReturn("testSegment");
    when(segmentMetadata.getTotalDocs()).thenReturn(cardinality);
    when(segmentMetadata.getAllColumns()).thenReturn(new TreeSet<>(Set.of(columnName)));
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
