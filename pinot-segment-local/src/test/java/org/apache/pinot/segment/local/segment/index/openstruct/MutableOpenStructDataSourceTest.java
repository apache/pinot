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
package org.apache.pinot.segment.local.segment.index.openstruct;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.config.table.OpenStructIndexConfig;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


public class MutableOpenStructDataSourceTest {

  private PinotDataBufferMemoryManager _mm;

  @BeforeMethod
  public void setUp() {
    _mm = new DirectMemoryManager("MutableOpenStructDataSourceTest");
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mm.close();
  }

  private ComplexFieldSpec spec() {
    return new ComplexFieldSpec("metrics", DataType.OPEN_STRUCT, true, Map.of());
  }

  @Test
  public void testGetDataSourcePerKey()
      throws Exception {
    try (MutableOpenStructIndex idx = new MutableOpenStructIndex("metrics", spec(),
        OpenStructIndexConfig.DEFAULT, _mm, 100)) {
      idx.index(0, Map.of("clicks", 5L));
      MutableOpenStructDataSource ds = new MutableOpenStructDataSource(spec(), idx, 1);
      DataSource clicks = ds.getDataSource("clicks");
      assertNotNull(clicks);
      assertTrue(ds.isMaterialized("clicks"));
      assertTrue(ds.isFullyMaterialized()); // mutable always holds everything
    }
  }

  @Test
  public void testGetDataSourceForUnknownKey()
      throws Exception {
    try (MutableOpenStructIndex idx = new MutableOpenStructIndex("metrics", spec(),
        OpenStructIndexConfig.DEFAULT, _mm, 100)) {
      MutableOpenStructDataSource ds = new MutableOpenStructDataSource(spec(), idx, 0);
      assertNull(ds.getDataSource("missing"));
      assertFalse(ds.isMaterialized("missing"));
    }
  }

  @Test
  public void testGetDataSourcesReturnsAllKeys()
      throws Exception {
    try (MutableOpenStructIndex idx = new MutableOpenStructIndex("metrics", spec(),
        OpenStructIndexConfig.DEFAULT, _mm, 100)) {
      idx.index(0, Map.of("clicks", 5L, "country", "US"));
      MutableOpenStructDataSource ds = new MutableOpenStructDataSource(spec(), idx, 1);
      assertEquals(ds.getDataSources().size(), 2);
    }
  }

  /// ProjectionBlock re-resolves the key on every block, so repeated lookups must not rebuild the source.
  @Test
  public void testGetDataSourceIsMemoisedPerKey()
      throws Exception {
    try (MutableOpenStructIndex idx = new MutableOpenStructIndex("metrics", spec(),
        OpenStructIndexConfig.DEFAULT, _mm, 100)) {
      idx.index(0, Map.of("clicks", 5L));
      MutableOpenStructDataSource ds = new MutableOpenStructDataSource(spec(), idx, 1);
      assertSame(ds.getDataSource("clicks"), ds.getDataSource("clicks"));
    }
  }

  /// The memo must not serve a stale source. Ingest past the snapshot boundary — including a type-conflicting value,
  /// which would reallocate the column if the never-replaced invariant regressed — and the cached source must still
  /// report on the snapshotted window only.
  @Test
  public void testMemoisedDataSourceStaysCorrectAsIngestionContinues()
      throws Exception {
    try (MutableOpenStructIndex idx = new MutableOpenStructIndex("metrics", spec(),
        OpenStructIndexConfig.DEFAULT, _mm, 100)) {
      idx.index(0, Map.of("clicks", 5L));
      MutableOpenStructDataSource ds = new MutableOpenStructDataSource(spec(), idx, 1);
      DataSource first = ds.getDataSource("clicks");
      assertNotNull(first);
      assertTrue(first.getNullValueVector().getNullBitmap().isEmpty());

      idx.index(1, Map.of("clicks", 7L));
      idx.index(2, Map.of("clicks", "not-a-long"));

      assertSame(ds.getDataSource("clicks"), first);
      assertTrue(first.getNullValueVector().getNullBitmap().isEmpty());
    }
  }

  /// Exercises a non-empty null bitmap through the memo: the key is absent on doc 0.
  @Test
  public void testMemoisedNullBitmapReportsAbsentDocs()
      throws Exception {
    try (MutableOpenStructIndex idx = new MutableOpenStructIndex("metrics", spec(),
        OpenStructIndexConfig.DEFAULT, _mm, 100)) {
      idx.index(0, Map.of("country", "US"));
      idx.index(1, Map.of("clicks", 5L));
      MutableOpenStructDataSource ds = new MutableOpenStructDataSource(spec(), idx, 2);
      ImmutableRoaringBitmap nulls = ds.getDataSource("clicks").getNullValueVector().getNullBitmap();
      assertTrue(nulls.contains(0));
      assertFalse(nulls.contains(1));
    }
  }

  /// An absent key must not be memoised: ingestion can create it after the first lookup.
  @Test
  public void testKeyCreatedAfterFirstLookupIsPickedUp()
      throws Exception {
    try (MutableOpenStructIndex idx = new MutableOpenStructIndex("metrics", spec(),
        OpenStructIndexConfig.DEFAULT, _mm, 100)) {
      MutableOpenStructDataSource ds = new MutableOpenStructDataSource(spec(), idx, 1);
      assertNull(ds.getDataSource("clicks"));
      idx.index(0, Map.of("clicks", 5L));
      assertNotNull(ds.getDataSource("clicks"));
    }
  }

  @Test
  public void testDictionaryReservesDefaultNullValueAtDictIdZero()
      throws Exception {
    try (MutableOpenStructIndex idx = new MutableOpenStructIndex("metrics", spec(),
        OpenStructIndexConfig.DEFAULT, _mm, 100)) {
      idx.index(0, Map.of("clicks", 5L));
      MutableOpenStructDataSource ds = new MutableOpenStructDataSource(spec(), idx, 1);
      DataSource clicks = ds.getDataSource("clicks");
      assertNotNull(clicks);
      // dictId 0 is the reserved default null value for the inferred LONG type; the first real
      // value lands at dictId 1. Zero-initialized forward-index chunks therefore read absent
      // docs as the default, matching sealed segments which fold the default at build time.
      assertEquals(clicks.getDictionary().get(0), Long.MIN_VALUE);
      assertEquals(clicks.getDictionary().get(1), 5L);
      // The inverted index mirrors the dictionary reservation: slot 0 exists but stays empty
      // (no doc explicitly wrote the default), slot 1 holds the doc that wrote the real value.
      MutableKeyColumn col = idx.getKeyColumn("clicks");
      assertTrue(col.getInvertedIndex().getDocIds(0).isEmpty());
      assertTrue(col.getInvertedIndex().getDocIds(1).contains(0));
    }
  }

  @Test
  public void testLastIndexedDocIdWatermark()
      throws Exception {
    try (MutableOpenStructIndex idx = new MutableOpenStructIndex("metrics", spec(),
        OpenStructIndexConfig.DEFAULT, _mm, 100)) {
      idx.index(0, Map.of("clicks", 5L));
      idx.index(1, Map.of("other", 1L));
      idx.index(2, Map.of("clicks", 7L));
      MutableKeyColumn col = idx.getKeyColumn("clicks");
      assertNotNull(col);
      assertEquals(col.getLastIndexedDocId(), 2);
      assertEquals(idx.getKeyColumn("other").getLastIndexedDocId(), 1);
    }
  }

  @Test
  public void testForwardIndexSafeForAbsentTailDocs()
      throws Exception {
    try (MutableOpenStructIndex idx = new MutableOpenStructIndex("metrics", spec(),
        OpenStructIndexConfig.DEFAULT, _mm, 3000)) {
      idx.index(0, Map.of("clicks", 5L));
      idx.index(3, Map.of("clicks", 9L));
      // Docs 4..2499 never see "clicks": docs in chunk 0 are in-range holes, docs >= 1000 hit
      // forward-index chunks that were never allocated.
      int numDocs = 2500;
      MutableOpenStructDataSource ds = new MutableOpenStructDataSource(spec(), idx, numDocs);
      DataSource clicks = ds.getDataSource("clicks");
      assertNotNull(clicks);
      ForwardIndexReader<?> fwd = clicks.getForwardIndex();

      // Single-doc reads: hole in chunk 0, then past the last allocated chunk.
      assertEquals(fwd.getDictId(1, null), 0);
      assertEquals(fwd.getDictId(2400, null), 0);
      // Present docs unaffected.
      assertEquals(clicks.getDictionary().get(fwd.getDictId(0, null)), 5L);
      assertEquals(clicks.getDictionary().get(fwd.getDictId(3, null)), 9L);

      // Bulk read spanning present docs, holes, and the unallocated tail.
      int[] docIds = {0, 1, 3, 999, 1000, 2499};
      int[] dictIds = new int[docIds.length];
      fwd.readDictIds(docIds, docIds.length, dictIds, null);
      assertEquals(clicks.getDictionary().get(dictIds[0]), 5L);
      assertEquals(dictIds[1], 0);
      assertEquals(clicks.getDictionary().get(dictIds[2]), 9L);
      assertEquals(dictIds[3], 0);
      assertEquals(dictIds[4], 0);
      assertEquals(dictIds[5], 0);

      // Bulk fast path: all docIds at or below the watermark delegate to the raw index.
      int[] presentDocIds = {0, 1, 2, 3};
      int[] presentDictIds = new int[4];
      fwd.readDictIds(presentDocIds, 4, presentDictIds, null);
      assertEquals(clicks.getDictionary().get(presentDictIds[0]), 5L);
      assertEquals(presentDictIds[1], 0);
      assertEquals(presentDictIds[2], 0);
      assertEquals(clicks.getDictionary().get(presentDictIds[3]), 9L);
    }
  }

  @Test
  public void testInvertedIndexFoldsAbsentDocsIntoDefaultPostings()
      throws Exception {
    try (MutableOpenStructIndex idx = new MutableOpenStructIndex("metrics", spec(),
        OpenStructIndexConfig.DEFAULT, _mm, 100)) {
      // Present in docs 0 (5L) and 3 (9L) of 6; docs 1,2,4,5 never see the key.
      idx.index(0, Map.of("clicks", 5L));
      idx.index(3, Map.of("clicks", 9L));
      int numDocs = 6;
      MutableOpenStructDataSource ds = new MutableOpenStructDataSource(spec(), idx, numDocs);
      DataSource clicks = ds.getDataSource("clicks");
      assertNotNull(clicks);

      // Absent docs are folded into the reserved default's (dictId 0) postings, matching what a
      // sealed segment would have built for the same rows.
      assertEquals(clicks.getInvertedIndex().getDocIds(0), MutableRoaringBitmap.bitmapOf(1, 2, 4, 5));
      int dictIdOfFive = clicks.getDictionary().indexOf(5L);
      assertEquals(clicks.getInvertedIndex().getDocIds(dictIdOfFive), MutableRoaringBitmap.bitmapOf(0));
    }
  }

  @Test
  public void testInvertedIndexFoldsAbsentDocsWithExplicitDefaultWrite()
      throws Exception {
    try (MutableOpenStructIndex idx = new MutableOpenStructIndex("metrics", spec(),
        OpenStructIndexConfig.DEFAULT, _mm, 100)) {
      // doc 0 writes a real value; doc 3 explicitly writes the LONG default itself.
      idx.index(0, Map.of("clicks", 5L));
      idx.index(3, Map.of("clicks", Long.MIN_VALUE));
      int numDocs = 6;
      MutableOpenStructDataSource ds = new MutableOpenStructDataSource(spec(), idx, numDocs);
      DataSource clicks = ds.getDataSource("clicks");
      assertNotNull(clicks);

      // dictId 0's postings = {3} (explicit write) folded with the absent complement {1,2,4,5}.
      assertEquals(clicks.getInvertedIndex().getDocIds(0), MutableRoaringBitmap.bitmapOf(1, 2, 3, 4, 5));
    }
  }

  @Test
  public void testKeyDictionaryExactWhenPartiallyPresent()
      throws Exception {
    try (MutableOpenStructIndex idx = new MutableOpenStructIndex("metrics", spec(),
        OpenStructIndexConfig.DEFAULT, _mm, 100)) {
      idx.index(0, Map.of("clicks", 5L));
      MutableOpenStructDataSource ds = new MutableOpenStructDataSource(spec(), idx, 3);
      // numDocs=3 but the key is only present on doc 0: dictId 0 is a legitimate value shared by
      // absent docs 1 and 2, so the dictionary is exact.
      assertTrue(ds.isKeyDictionaryExact("clicks"));
    }
  }

  @Test
  public void testKeyDictionaryNotExactWhenFullyPresentAndDefaultUnobserved()
      throws Exception {
    try (MutableOpenStructIndex idx = new MutableOpenStructIndex("metrics", spec(),
        OpenStructIndexConfig.DEFAULT, _mm, 100)) {
      idx.index(0, Map.of("clicks", 5L));
      idx.index(1, Map.of("clicks", 7L));
      MutableOpenStructDataSource ds = new MutableOpenStructDataSource(spec(), idx, 2);
      // Fully present, and no doc ever wrote the reserved default: dictId 0 is a phantom entry.
      assertFalse(ds.isKeyDictionaryExact("clicks"));
    }
  }

  @Test
  public void testKeyDictionaryExactWhenFullyPresentAndDefaultObserved()
      throws Exception {
    try (MutableOpenStructIndex idx = new MutableOpenStructIndex("metrics", spec(),
        OpenStructIndexConfig.DEFAULT, _mm, 100)) {
      idx.index(0, Map.of("clicks", 5L));
      idx.index(1, Map.of("clicks", Long.MIN_VALUE));
      MutableOpenStructDataSource ds = new MutableOpenStructDataSource(spec(), idx, 2);
      // Fully present, and doc 1 explicitly wrote the reserved default: dictId 0 is real.
      assertTrue(ds.isKeyDictionaryExact("clicks"));
    }
  }

  /// Pins the sealed splitter's default-resolution rule (OpenStructColumnSplitter#writeDenseKeyColumn,
  /// ~line 280): absent-doc defaults come from a throwaway generic DimensionFieldSpec(key, storedType,
  /// true), not from the real child spec's own default. If `allocateKeyColumn` reverted to preferring
  /// `childSpec.getDefaultNullValue()`, this test would fail because dictId 0 would hold 0L instead of
  /// Long.MIN_VALUE.
  @Test
  public void testAllocationUsesGenericDefaultNotChildSpecCustomDefault()
      throws Exception {
    Map<String, FieldSpec> children = new HashMap<>();
    children.put("clicks", new DimensionFieldSpec("clicks", DataType.LONG, true, 0L));
    ComplexFieldSpec specWithCustomDefault =
        new ComplexFieldSpec("metrics", DataType.OPEN_STRUCT, true, children);
    try (MutableOpenStructIndex idx = new MutableOpenStructIndex("metrics", specWithCustomDefault,
        OpenStructIndexConfig.DEFAULT, _mm, 100)) {
      idx.index(0, Map.of("clicks", 5L));
      MutableKeyColumn col = idx.getKeyColumn("clicks");
      assertNotNull(col);
      assertEquals(col.getDictionary().get(0), Long.MIN_VALUE);
    }
  }

  @DataProvider(name = "storedTypes")
  public Object[][] storedTypes() {
    return new Object[][] {
        {DataType.INT, 42},
        {DataType.LONG, 5L},
        {DataType.FLOAT, 1.5f},
        {DataType.DOUBLE, 2.5d},
        {DataType.STRING, "hello"},
        {DataType.BYTES, new byte[] {1, 2, 3}},
    };
  }

  /// Every inferable stored type reserves its own generic dimension default at dictId 0, not just LONG.
  @Test(dataProvider = "storedTypes")
  public void testDefaultReservationPerStoredType(DataType storedType, Object value)
      throws Exception {
    try (MutableOpenStructIndex idx = new MutableOpenStructIndex("metrics", spec(),
        OpenStructIndexConfig.DEFAULT, _mm, 100)) {
      idx.index(0, Map.of("k", value));
      MutableKeyColumn col = idx.getKeyColumn("k");
      assertNotNull(col);
      assertEquals(col.getStoredType(), storedType);
      Object expectedDefault = FieldSpec.getDefaultNullValue(FieldSpec.FieldType.DIMENSION, storedType, null);
      Object actualDefault = col.getDictionary().get(0);
      if (storedType == DataType.BYTES) {
        assertEquals((byte[]) actualDefault, (byte[]) expectedDefault);
      } else {
        assertEquals(actualDefault, expectedDefault);
      }
    }
  }
}
