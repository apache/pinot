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
package org.apache.pinot.segment.local.upsert;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class BasePartitionUpsertMetadataManagerTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "BasePartitionUpsertMetadataManagerTest");

  @BeforeClass
  public void setUp()
      throws IOException {
    FileUtils.forceMkdir(TEMP_DIR);
    ServerMetrics.register(mock(ServerMetrics.class));
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    FileUtils.forceDelete(TEMP_DIR);
  }

  @Test
  public void testTakeSnapshotInOrder()
      throws IOException {
    DummyPartitionUpsertMetadataManager upsertMetadataManager =
        new DummyPartitionUpsertMetadataManager("myTable", 0, mock(UpsertContext.class));

    List<String> segmentsTakenSnapshot = new ArrayList<>();

    File segDir01 = new File(TEMP_DIR, "seg01");
    ImmutableSegmentImpl seg01 = createImmutableSegment("seg01", segDir01, segmentsTakenSnapshot);
    seg01.enableUpsert(upsertMetadataManager, createValidDocIds(0, 1, 2, 3), null);
    upsertMetadataManager.trackSegment(seg01);
    // seg01 has a tmp snapshot file, but no snapshot file
    FileUtils.touch(new File(segDir01, V1Constants.VALID_DOC_IDS_SNAPSHOT_FILE_NAME + "_tmp"));

    File segDir02 = new File(TEMP_DIR, "seg02");
    ImmutableSegmentImpl seg02 = createImmutableSegment("seg02", segDir02, segmentsTakenSnapshot);
    seg02.enableUpsert(upsertMetadataManager, createValidDocIds(0, 1, 2, 3, 4, 5), null);
    upsertMetadataManager.trackSegment(seg02);
    // seg02 has snapshot file, so its snapshot is taken first.
    FileUtils.touch(new File(segDir02, V1Constants.VALID_DOC_IDS_SNAPSHOT_FILE_NAME));

    File segDir03 = new File(TEMP_DIR, "seg03");
    ImmutableSegmentImpl seg03 = createImmutableSegment("seg03", segDir03, segmentsTakenSnapshot);
    seg03.enableUpsert(upsertMetadataManager, createValidDocIds(3, 4, 7), null);
    upsertMetadataManager.trackSegment(seg03);

    // The mutable segments will be skipped.
    upsertMetadataManager.trackSegment(mock(MutableSegmentImpl.class));

    upsertMetadataManager.doTakeSnapshot();
    assertEquals(segmentsTakenSnapshot.size(), 3);
    // The snapshot of seg02 was taken firstly, as it's the only segment with existing snapshot.
    assertEquals(segmentsTakenSnapshot.get(0), "seg02");
    // Set is used to track segments internally, so we can't assert the order of the other segments deterministically,
    // but all 3 segments should have taken their snapshots.
    assertTrue(segmentsTakenSnapshot.containsAll(Arrays.asList("seg01", "seg02", "seg03")));

    assertEquals(TEMP_DIR.list().length, 3);
    assertTrue(segDir01.exists());
    assertEquals(seg01.loadValidDocIdsFromSnapshot().getCardinality(), 4);
    assertTrue(segDir02.exists());
    assertEquals(seg02.loadValidDocIdsFromSnapshot().getCardinality(), 6);
    assertTrue(segDir03.exists());
    assertEquals(seg03.loadValidDocIdsFromSnapshot().getCardinality(), 3);
  }

  private static ThreadSafeMutableRoaringBitmap createValidDocIds(int... docIds) {
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(docIds);
    return new ThreadSafeMutableRoaringBitmap(bitmap);
  }

  private static ImmutableSegmentImpl createImmutableSegment(String segName, File segDir,
      List<String> segmentsTakenSnapshot)
      throws IOException {
    FileUtils.forceMkdir(segDir);
    SegmentMetadataImpl meta = mock(SegmentMetadataImpl.class);
    when(meta.getName()).thenReturn(segName);
    when(meta.getIndexDir()).thenReturn(segDir);
    return new ImmutableSegmentImpl(mock(SegmentDirectory.class), meta, new HashMap<>(), null) {
      public void persistValidDocIdsSnapshot() {
        segmentsTakenSnapshot.add(segName);
        super.persistValidDocIdsSnapshot();
      }
    };
  }

  private static class DummyPartitionUpsertMetadataManager extends BasePartitionUpsertMetadataManager {

    protected DummyPartitionUpsertMetadataManager(String tableNameWithType, int partitionId, UpsertContext context) {
      super(tableNameWithType, partitionId, context);
    }

    public void trackSegment(IndexSegment seg) {
      _trackedSegments.add(seg);
    }

    @Override
    protected long getNumPrimaryKeys() {
      return 0;
    }

    @Override
    protected void addOrReplaceSegment(ImmutableSegmentImpl segment, ThreadSafeMutableRoaringBitmap validDocIds,
        @Nullable ThreadSafeMutableRoaringBitmap queryableDocIds, Iterator<RecordInfo> recordInfoIterator,
        @Nullable IndexSegment oldSegment, @Nullable MutableRoaringBitmap validDocIdsForOldSegment) {

    }

    @Override
    protected boolean doAddRecord(MutableSegment segment, RecordInfo recordInfo) {
      return false;
    }

    @Override
    protected void removeSegment(IndexSegment segment, MutableRoaringBitmap validDocIds) {

    }

    @Override
    protected GenericRow doUpdateRecord(GenericRow record, RecordInfo recordInfo) {
      return null;
    }

    @Override
    protected void doRemoveExpiredPrimaryKeys() {

    }
  }
}
