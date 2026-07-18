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
import java.util.List;
import java.util.Map;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;


/**
 * Covers the mode-dispatch branch that a prior review pass regressed: NONE/skipUpsertView must take the direct,
 * unlocked read; SYNC/SNAPSHOT must delegate to {@link UpsertViewManager}, which is the only path that respects
 * segment tracking.
 */
public class ConcurrentMapTableUpsertMetadataManagerTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .setSchemaName(RAW_TABLE_NAME)
      .addSingleValueDimension("myCol", DataType.STRING)
      .addDateTimeField("timeCol", DataType.LONG, "TIMESTAMP", "1:MILLISECONDS")
      .setPrimaryKeyColumns(List.of("myCol"))
      .build();

  private ConcurrentMapTableUpsertMetadataManager createManager(UpsertConfig.ConsistencyMode consistencyMode) {
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setConsistencyMode(consistencyMode);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(RAW_TABLE_NAME)
        .setTimeColumnName("timeCol")
        .setUpsertConfig(upsertConfig)
        .build();
    TableDataManager tableDataManager = mock(TableDataManager.class);
    when(tableDataManager.getTableDataDir()).thenReturn(new File(RAW_TABLE_NAME));
    return (ConcurrentMapTableUpsertMetadataManager) TableUpsertMetadataManagerFactory.create(new PinotConfiguration(),
        tableConfig, SCHEMA, tableDataManager, null);
  }

  private IndexSegment mockSegmentWithDistinctBitmaps() {
    IndexSegment segment = mock(MutableSegment.class);
    ThreadSafeMutableRoaringBitmap validBitmap = mock(ThreadSafeMutableRoaringBitmap.class);
    ThreadSafeMutableRoaringBitmap queryableBitmap = mock(ThreadSafeMutableRoaringBitmap.class);
    when(validBitmap.getMutableRoaringBitmap()).thenReturn(new MutableRoaringBitmap());
    when(queryableBitmap.getMutableRoaringBitmap()).thenReturn(new MutableRoaringBitmap());
    when(segment.getValidDocIds()).thenReturn(validBitmap);
    when(segment.getQueryableDocIds()).thenReturn(queryableBitmap);
    when(segment.getSegmentName()).thenReturn("seg1");
    return segment;
  }

  @Test
  public void testNoneModeTakesDirectReadEvenWhenUntracked() {
    ConcurrentMapTableUpsertMetadataManager manager = createManager(UpsertConfig.ConsistencyMode.NONE);
    IndexSegment segment = mockSegmentWithDistinctBitmaps();
    SegmentContext segmentContext = new SegmentContext(segment);

    // Never tracked via any UpsertViewManager, yet a direct read still populates the snapshot.
    manager.setSegmentContexts(List.of(segmentContext), Map.of(QueryOptionKey.SKIP_UPSERT_DELETE, "true"));
    assertSame(segmentContext.getDocIdsSnapshot(), segment.getValidDocIds().getMutableRoaringBitmap());
  }

  @Test
  public void testSkipUpsertViewTakesDirectReadUnderSyncMode() {
    ConcurrentMapTableUpsertMetadataManager manager = createManager(UpsertConfig.ConsistencyMode.SYNC);
    IndexSegment segment = mockSegmentWithDistinctBitmaps();
    SegmentContext segmentContext = new SegmentContext(segment);

    manager.setSegmentContexts(List.of(segmentContext),
        Map.of(QueryOptionKey.SKIP_UPSERT_VIEW, "true", QueryOptionKey.SKIP_UPSERT_DELETE, "true"));
    assertSame(segmentContext.getDocIdsSnapshot(), segment.getValidDocIds().getMutableRoaringBitmap());
  }

  /// ConcurrentMapTableUpsertMetadataManager's dispatch only special-cases NONE (see the other tests above);
  /// SYNC and SNAPSHOT both fall into this same delegate branch, so testing one mode here is sufficient — their
  /// differing internal behavior is UpsertViewManager's concern, covered by UpsertViewManagerTest instead.
  @Test
  public void testDelegatesToUpsertViewManagerAndRespectsTracking() {
    ConcurrentMapTableUpsertMetadataManager manager = createManager(UpsertConfig.ConsistencyMode.SYNC);
    IndexSegment segment = mockSegmentWithDistinctBitmaps();
    SegmentContext segmentContext = new SegmentContext(segment);

    // Force the partition manager (and its UpsertViewManager) to exist, but deliberately don't track the segment:
    // a correct delegation to UpsertViewManager leaves the snapshot unset, since UpsertViewManager only serves
    // tracked segments. A regression back to a direct-read bypass would populate it regardless of tracking, as
    // testNoneModeTakesDirectReadEvenWhenUntracked demonstrates. (Without forcing the partition manager to exist
    // first, this assertion would pass vacuously: setSegmentContexts would iterate zero partition managers.)
    manager.getOrCreatePartitionManager(0);
    manager.setSegmentContexts(List.of(segmentContext), Map.of(QueryOptionKey.SKIP_UPSERT_DELETE, "true"));
    assertNull(segmentContext.getDocIdsSnapshot());

    manager.getOrCreatePartitionManager(0).getUpsertViewManager().trackSegment(segment);
    manager.setSegmentContexts(List.of(segmentContext), Map.of(QueryOptionKey.SKIP_UPSERT_DELETE, "true"));
    assertSame(segmentContext.getDocIdsSnapshot(), segment.getValidDocIds().getMutableRoaringBitmap());
  }
}
