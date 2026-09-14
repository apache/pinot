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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.utils.ConsumingSegmentConsistencyModeListener;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;


/// Exercises replacement reporting independently of metadata ownership and upsert/query consistency modes.
public class UpsertReplacementReportingTest {
  private static final List<String> PRIMARY_KEYS = List.of("pk");
  private static final List<String> COMPARISON_COLUMNS = List.of("time");
  private static final Schema SCHEMA = new Schema.SchemaBuilder().addSingleValueDimension("pk", DataType.INT)
      .addMetric("time", DataType.INT).addMetric("total", DataType.INT).setPrimaryKeyColumns(PRIMARY_KEYS).build();
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.REALTIME).setTableName("test").build();

  @BeforeClass
  public void setUp() {
    ServerMetrics.register(mock(ServerMetrics.class));
  }

  @DataProvider
  public Object[][] replacements() {
    List<Object[]> cases = new ArrayList<>();
    for (String type : List.of("full", "partial", "drop", "mark")) {
      for (UpsertConfig.ConsistencyMode view : UpsertConfig.ConsistencyMode.values()) {
        for (String committed : List.of("matching", "different", "missing", "older", "newer", "duplicate")) {
          for (boolean moveBefore : new boolean[]{true, false}) {
            cases.add(new Object[]{type, view, committed, moveBefore, false});
            cases.add(new Object[]{type, view, committed, moveBefore, true});
          }
        }
      }
    }
    return cases.toArray(Object[][]::new);
  }

  @Test(dataProvider = "replacements")
  public void testReplacementReporting(String type, UpsertConfig.ConsistencyMode view, String committed,
      boolean moveBefore, boolean consistentDeletes)
      throws IOException {
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.PARTIAL);
    upsertConfig.setPartialUpsertStrategies(Map.of("total", UpsertConfig.Strategy.INCREMENT));
    PartialUpsertHandler handler = new PartialUpsertHandler(TABLE_CONFIG, SCHEMA, COMPARISON_COLUMNS, upsertConfig);
    TableDataManager tableDataManager = mock(TableDataManager.class);
    when(tableDataManager.getTableDataDir()).thenReturn(new File(System.getProperty("java.io.tmpdir")));
    UpsertContext context = new UpsertContext.Builder().setSchema(SCHEMA).setTableConfig(TABLE_CONFIG)
        .setTableDataManager(tableDataManager).setPrimaryKeyColumns(PRIMARY_KEYS)
        .setComparisonColumns(COMPARISON_COLUMNS)
        .setConsistencyMode(view).setPartialUpsertHandlerSupplier(type.equals("partial") ? () -> handler : null)
        .setDropOutOfOrderRecord(type.equals("drop")).setOutOfOrderRecordColumn(type.equals("mark") ? "marked" : null)
        .build();
    MutableSegment oldSegment = mock(MutableSegment.class);
    configureSegment(oldSegment, "test__0__0__0", new int[]{100}, new int[]{10});
    ImmutableSegmentImpl replacement = mock(ImmutableSegmentImpl.class);
    int[] comparisons = committed.equals("missing") ? new int[0]
        : committed.equals("duplicate") ? new int[]{100, 100}
        : new int[]{committed.equals("older") ? 90 : committed.equals("newer") ? 105 : 100};
    int[] totals = committed.equals("missing") ? new int[0]
        : committed.equals("duplicate") ? new int[]{10, 8} : new int[]{committed.equals("matching") ? 10 : 8};
    configureSegment(replacement, "test__0__0__0", comparisons, totals);
    MutableSegment nextSegment = mock(MutableSegment.class);
    configureSegment(nextSegment, "test__0__1__0", new int[]{110}, new int[]{0});
    List<RecordInfo> records = new ArrayList<>();
    for (int i = 0; i < comparisons.length; i++) {
      records.add(new RecordInfo(new PrimaryKey(new Object[]{1}), i, comparisons[i], false));
    }
    boolean replacementFailed = committed.equals("missing") || committed.equals("older");
    int expected = moveBefore ? (committed.equals("matching") ? 0 : 1) : (replacementFailed ? 1 : 0);
    int[] reported = new int[1];
    Consumer<BasePartitionUpsertMetadataManager> consumeNext = manager -> {
      GenericRow row = new GenericRow();
      row.putValue("pk", 1);
      row.putValue("time", 110);
      row.putValue("total", 1);
      RecordInfo next = new RecordInfo(new PrimaryKey(new Object[]{1}), 0, 110, false);
      manager.updateRecord(row, next);
      if (type.equals("partial")) {
        int baseline = moveBefore || replacementFailed || committed.equals("matching") ? 10 : 8;
        assertEquals(row.getValue("total"), baseline + 1);
      }
      manager.addRecord(nextSegment, next);
    };
    try (BasePartitionUpsertMetadataManager manager =
        createManager(consistentDeletes, context, consumeNext, moveBefore, reported)) {
      try {
        ConsumingSegmentConsistencyModeListener.getInstance()
            .setMode(ConsumingSegmentConsistencyModeListener.Mode.UNSAFE);
        manager.addRecord(oldSegment, new RecordInfo(new PrimaryKey(new Object[]{1}), 0, 100, false));
        manager.replaceSegment(replacement, replacement.getValidDocIds(), null, records.iterator(), oldSegment);
        assertEquals(reported[0], expected, "Reporting must follow the committed baseline, not current ownership");
        PrimaryKey key = new PrimaryKey(new Object[]{1});
        if (manager instanceof ConcurrentMapPartitionUpsertMetadataManagerForConsistentDeletes deletesManager) {
          var location = deletesManager._primaryKeyToRecordLocationMap.get(key);
          assertSame(location.getSegment(), nextSegment);
          assertEquals(location.getDistinctSegmentCount(), committed.equals("missing") ? 1 : 2);
        } else {
          var mapManager = (ConcurrentMapPartitionUpsertMetadataManager) manager;
          assertSame(mapManager._primaryKeyToRecordLocationMap.get(key).getSegment(), nextSegment);
        }
        assertEquals(nextSegment.getValidDocIds().getMutableRoaringBitmap().getCardinality(), 1);
      } finally {
        ConsumingSegmentConsistencyModeListener.getInstance().reset();
        manager.stop();
      }
    }
  }

  @Test
  public void testProtectedReplacementPreservesRevertCandidates()
      throws IOException {
    for (UpsertConfig.ConsistencyMode view : UpsertConfig.ConsistencyMode.values()) {
      TableDataManager tableDataManager = mock(TableDataManager.class);
      when(tableDataManager.getTableDataDir()).thenReturn(new File(System.getProperty("java.io.tmpdir")));
      UpsertContext context = new UpsertContext.Builder().setSchema(SCHEMA).setTableConfig(TABLE_CONFIG)
          .setTableDataManager(tableDataManager).setPrimaryKeyColumns(PRIMARY_KEYS)
          .setComparisonColumns(COMPARISON_COLUMNS).setConsistencyMode(view)
          .setPartialUpsertHandlerSupplier(() -> mock(PartialUpsertHandler.class)).build();
      MutableSegment oldSegment = mock(MutableSegment.class);
      configureSegment(oldSegment, "test__0__0__0", new int[]{100}, new int[]{10});
      ImmutableSegmentImpl replacement = mock(ImmutableSegmentImpl.class);
      configureSegment(replacement, "test__0__0__0", new int[0], new int[0]);
      MutableSegment nextSegment = mock(MutableSegment.class);
      configureSegment(nextSegment, "test__0__1__0", new int[]{110}, new int[]{11});
      int[] revertedCandidates = new int[1];
      try (BasePartitionUpsertMetadataManager manager =
          new ConcurrentMapPartitionUpsertMetadataManager("test_REALTIME", 0, context) {
            @Override
            protected void doAddOrReplaceSegment(ImmutableSegmentImpl segment,
                ThreadSafeMutableRoaringBitmap validDocIds, @Nullable ThreadSafeMutableRoaringBitmap queryableDocIds,
                Iterator<RecordInfo> iterator, @Nullable IndexSegment oldSegment,
                @Nullable MutableRoaringBitmap candidates) {
              addRecord(nextSegment, new RecordInfo(new PrimaryKey(new Object[]{1}), 0, 110, false));
              super.doAddOrReplaceSegment(segment, validDocIds, queryableDocIds, iterator, oldSegment, candidates);
            }

            @Override
            protected void revertSegmentUpsertMetadata(IndexSegment segment, String name,
                MutableRoaringBitmap candidates) {
              revertedCandidates[0] = candidates.getCardinality();
            }
          }) {
        try {
          ConsumingSegmentConsistencyModeListener.getInstance()
              .setMode(ConsumingSegmentConsistencyModeListener.Mode.PROTECTED);
          manager.addRecord(oldSegment, new RecordInfo(new PrimaryKey(new Object[]{1}), 0, 100, false));
          manager.replaceSegment(replacement, replacement.getValidDocIds(), null, List.<RecordInfo>of().iterator(),
              oldSegment);
          assertEquals(revertedCandidates[0], view == UpsertConfig.ConsistencyMode.NONE ? 1 : 0,
              "Protected mode must retain its existing snapshot/live-bitmap revert inputs");
        } finally {
          ConsumingSegmentConsistencyModeListener.getInstance().reset();
          manager.stop();
        }
      }
    }
  }

  @Test
  public void testNullAndUnreadableReplacementValues()
      throws IOException {
    UpsertContext context = new UpsertContext.Builder().setSchema(SCHEMA).setTableConfig(TABLE_CONFIG)
        .setPrimaryKeyColumns(PRIMARY_KEYS).setComparisonColumns(COMPARISON_COLUMNS)
        .setTableIndexDir(new File(System.getProperty("java.io.tmpdir"))).build();
    MutableSegment oldSegment = mock(MutableSegment.class);
    ImmutableSegmentImpl replacement = mock(ImmutableSegmentImpl.class);
    configureSegment(oldSegment, "test__0__0__0", new int[]{100}, new int[]{0});
    configureSegment(replacement, "test__0__0__0", new int[]{100}, new int[]{0});
    MutableRoaringBitmap candidates = MutableRoaringBitmap.bitmapOf(0);
    try (BasePartitionUpsertMetadataManager manager = createManager(false, context, ignored -> { }, true, new int[1])) {
      try {
        NullValueVectorReader nulls = mock(NullValueVectorReader.class);
        when(nulls.isNull(0)).thenReturn(true);
        when(replacement.getDataSource("total").getNullValueVector()).thenReturn(nulls);
        assertEquals(manager.findUnreplacedDocIds(oldSegment, replacement, candidates).getCardinality(), 1,
            "A null and its stored default are different values");
        when(oldSegment.getDataSource("total").getNullValueVector()).thenReturn(nulls);
        assertEquals(manager.findUnreplacedDocIds(oldSegment, replacement, candidates).getCardinality(), 0);
        when(replacement.getDataSource("total").getForwardIndex()).thenReturn(null);
        assertEquals(manager.findUnreplacedDocIds(oldSegment, replacement, candidates).getCardinality(), 1,
            "An unreadable value cannot establish equivalence");
        assertEquals(candidates.getCardinality(), 1, "Validation must preserve candidates for cleanup");
      } finally {
        manager.stop();
      }
    }
  }

  private static BasePartitionUpsertMetadataManager createManager(boolean consistentDeletes, UpsertContext context,
      Consumer<BasePartitionUpsertMetadataManager> consumeNext, boolean moveBefore, int[] reported) {
    if (consistentDeletes) {
      return new ConcurrentMapPartitionUpsertMetadataManagerForConsistentDeletes("test_REALTIME", 0, context) {
        @Override
        protected void doAddOrReplaceSegment(ImmutableSegmentImpl segment,
            ThreadSafeMutableRoaringBitmap validDocIds, @Nullable ThreadSafeMutableRoaringBitmap queryableDocIds,
            Iterator<RecordInfo> iterator, @Nullable IndexSegment oldSegment,
            @Nullable MutableRoaringBitmap candidates) {
          if (moveBefore) {
            consumeNext.accept(this);
          }
          super.doAddOrReplaceSegment(segment, validDocIds, queryableDocIds, iterator, oldSegment, candidates);
          if (!moveBefore) {
            consumeNext.accept(this);
          }
        }

        @Override
        protected void updateInconsistentRowsMetric(String name, int count) {
          reported[0] += count;
        }
      };
    }
    return new ConcurrentMapPartitionUpsertMetadataManager("test_REALTIME", 0, context) {
      @Override
      protected void doAddOrReplaceSegment(ImmutableSegmentImpl segment,
          ThreadSafeMutableRoaringBitmap validDocIds, @Nullable ThreadSafeMutableRoaringBitmap queryableDocIds,
          Iterator<RecordInfo> iterator, @Nullable IndexSegment oldSegment,
          @Nullable MutableRoaringBitmap candidates) {
        if (moveBefore) {
          consumeNext.accept(this);
        }
        super.doAddOrReplaceSegment(segment, validDocIds, queryableDocIds, iterator, oldSegment, candidates);
        if (!moveBefore) {
          consumeNext.accept(this);
        }
      }

      @Override
      protected void updateInconsistentRowsMetric(String name, int count) {
        reported[0] += count;
      }
    };
  }

  private static void configureSegment(IndexSegment segment, String name, int[] comparisons, int[] totals) {
    when(segment.getSegmentName()).thenReturn(name);
    when(segment.getValidDocIds()).thenReturn(new ThreadSafeMutableRoaringBitmap());
    when(segment.getColumnNames()).thenReturn(Set.of("pk", "time", "total"));
    when(segment.getPhysicalColumnNames()).thenReturn(Set.of("pk", "time", "total"));
    SegmentMetadataImpl metadata = mock(SegmentMetadataImpl.class);
    when(metadata.getTotalDocs()).thenReturn(comparisons.length);
    when(segment.getSegmentMetadata()).thenReturn(metadata);
    for (String column : List.of("pk", "time", "total")) {
      DataSource source = mock(DataSource.class);
      ForwardIndexReader reader = mock(ForwardIndexReader.class);
      when(reader.isSingleValue()).thenReturn(true);
      when(reader.getStoredType()).thenReturn(DataType.INT);
      when(reader.getInt(anyInt(), any())).thenAnswer(call -> {
        int docId = call.getArgument(0);
        return column.equals("pk") ? 1 : column.equals("time") ? comparisons[docId] : totals[docId];
      });
      when(source.getForwardIndex()).thenReturn(reader);
      when(segment.getDataSource(column)).thenReturn(source);
    }
  }
}
