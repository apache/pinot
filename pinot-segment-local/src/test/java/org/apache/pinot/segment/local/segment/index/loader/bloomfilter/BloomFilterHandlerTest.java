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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Unit tests for {@link BloomFilterHandler} fpp-change detection.
 *
 * <p>The key invariant being tested: when the bloom filter configuration changes, even if the change does NOT
 * alter the number of hash functions (k), it may still change the bit-array size (numLongs). Both dimensions
 * must be compared to correctly detect a rebuild requirement.
 *
 * <p>Concretely, for cardinality=1000:
 * <ul>
 *   <li>fpp=0.05 → k=4, numLongs=98</li>
 *   <li>fpp=0.045 → k=4, numLongs=101</li>
 * </ul>
 * The old check (numHashFunctions only) silently kept the stale index; the new check catches the difference.
 */
public class BloomFilterHandlerTest {
  private static final String COLUMN = "myBloomCol";
  private static final int CARDINALITY = 1000;

  // With cardinality=1000 and fpp=0.05: k=4, numLongs=98
  private static final double FPP_STORED = 0.05;
  // With cardinality=1000 and fpp=0.045: k=4 (same!), numLongs=101 (different!)
  private static final double FPP_NEW_SAME_K = 0.045;
  // With cardinality=1000 and fpp=0.01: k=7 (different from k=4) — sanity check
  private static final double FPP_NEW_DIFF_K = 0.01;

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
   * Regression test: fpp=0.05 → 0.045 both yield k=4 but different numLongs (98 vs 101) for cardinality=1000.
   * The old check (numHashFunctions only) would return false (no rebuild), leaving a stale index.
   * The new check (numHashFunctions + numLongs) must return true (rebuild needed).
   */
  @Test
  public void testNeedUpdateReturnsTrueWhenSameKButDifferentNumLongs()
      throws Exception {
    writeBloomFilter(CARDINALITY, FPP_STORED);

    BloomFilterHandler handler = createHandler(COLUMN, new BloomFilterConfig(FPP_NEW_SAME_K, 0, false));
    SegmentDirectory.Reader reader = mockReaderWithBloomFilter(COLUMN, _bloomFilterFile);

    assertTrue(handler.needUpdateIndices(reader),
        "Rebuild expected: fpp 0.05→0.045 keeps k=4 but changes numLongs (98→101)");
  }

  /**
   * Sanity check: when fpp is unchanged the handler must not trigger a spurious rebuild.
   */
  @Test
  public void testNeedUpdateReturnsFalseWhenFppUnchanged()
      throws Exception {
    writeBloomFilter(CARDINALITY, FPP_STORED);

    BloomFilterHandler handler = createHandler(COLUMN, new BloomFilterConfig(FPP_STORED, 0, false));
    SegmentDirectory.Reader reader = mockReaderWithBloomFilter(COLUMN, _bloomFilterFile);

    assertFalse(handler.needUpdateIndices(reader),
        "No rebuild expected when fpp is unchanged");
  }

  /**
   * Sanity check: when fpp changes enough to alter k the handler must trigger a rebuild (pre-existing behavior).
   */
  @Test
  public void testNeedUpdateReturnsTrueWhenKChanges()
      throws Exception {
    writeBloomFilter(CARDINALITY, FPP_STORED);

    BloomFilterHandler handler = createHandler(COLUMN, new BloomFilterConfig(FPP_NEW_DIFF_K, 0, false));
    SegmentDirectory.Reader reader = mockReaderWithBloomFilter(COLUMN, _bloomFilterFile);

    assertTrue(handler.needUpdateIndices(reader),
        "Rebuild expected: fpp 0.05→0.01 changes k from 4 to 7");
  }

  // --- helpers ---

  /**
   * Writes a Pinot-format bloom filter file: [TYPE_VALUE(int)][VERSION(int)][Guava bytes].
   */
  private void writeBloomFilter(int cardinality, double fpp)
      throws Exception {
    BloomFilter<String> bf = BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8), cardinality, fpp);
    try (DataOutputStream out = new DataOutputStream(new FileOutputStream(_bloomFilterFile))) {
      out.writeInt(OnHeapGuavaBloomFilterCreator.TYPE_VALUE);
      out.writeInt(OnHeapGuavaBloomFilterCreator.VERSION);
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
