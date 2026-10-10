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
package org.apache.pinot.queries;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import org.apache.commons.io.FileUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.config.SchemaSerDeUtils;
import org.apache.pinot.common.utils.config.TableConfigSerDeUtils;
import org.apache.pinot.core.data.manager.offline.DimensionTableDataManager;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.utils.SegmentLocks;
import org.apache.pinot.segment.local.utils.SegmentOperationsThrottler;
import org.apache.pinot.segment.local.utils.SegmentOperationsThrottlerSet;
import org.apache.pinot.segment.local.utils.SegmentReloadSemaphore;
import org.apache.pinot.segment.local.utils.ServerReloadJobStatusCache;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.DimensionTableConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


/// Query-level regression test for the single-stage `lookUp(...)` UDF against a dimension table with a BYTES
/// primary key, using a *real* [DimensionTableDataManager] in preload mode (i.e. a real [FastLookupDimensionTable]
/// built by [DimensionTableDataManager#createFastLookupDimensionTable]) rather than a mock.
///
/// This exists because two separate things needed a BYTES primary key value to be represented consistently as
/// [org.apache.pinot.spi.utils.ByteArray] rather than a raw `byte[]`:
///   1. The value stored as the [org.apache.pinot.spi.data.readers.PrimaryKey] component when the dimension table
///      is loaded (`DimensionTableDataManager#createFastLookupDimensionTable`, via
///      `PinotSegmentRecordReader#getPrimaryKeys`).
///   2. The value returned by `FastLookupDimensionTable#getValue` when the *primary key column itself* is the
///      column being looked up (it returns the raw `PrimaryKey` component, unlike a non-key value column, which
///      is read from the separate `values` array).
///
public class LookupTransformFunctionBytesPrimaryKeyQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), "LookupTransformFunctionBytesPrimaryKeyQueriesTest");

  private static final String FACT_RAW_TABLE_NAME = "factTable";
  private static final String FACT_SEGMENT_NAME = "factTableSegment";
  private static final String FACT_ID_COLUMN = "id";
  private static final String FACT_BYTES_KEY_COLUMN = "factBytesKey";

  private static final String DIM_RAW_TABLE_NAME = "dimTable";
  private static final String DIM_SEGMENT_NAME = "dimTableSegment";
  private static final String DIM_BYTES_PK_COLUMN = "bytesPk";
  private static final String DIM_LABEL_COLUMN = "label";
  private static final String DIM_OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(DIM_RAW_TABLE_NAME);

  // Rows that have a matching dimension row, plus one (id = 3) that legitimately does not match anything, so a
  // "miss" can be told apart from the bug: both a real miss and the bug under test return empty bytes, but only
  // the bug does so for ids 1 and 2, which do have a matching row.
  private static final byte[] BYTES_ALPHA = {1, 2, 3};
  private static final byte[] BYTES_BETA = {4, 5, 6};
  private static final byte[] BYTES_NO_MATCH = {9, 9, 9};

  private static final SegmentOperationsThrottlerSet SEGMENT_OPERATIONS_THROTTLER = new SegmentOperationsThrottlerSet(
      new SegmentOperationsThrottler(1, 2, true),
      new SegmentOperationsThrottler(1, 2, true),
      new SegmentOperationsThrottler(1, 2, true),
      new SegmentOperationsThrottler(1, 2, true));

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;
  private DimensionTableDataManager _dimensionTableDataManager;

  @Override
  protected String getFilter() {
    return "";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _indexSegments;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);
    ServerMetrics.register(mock(ServerMetrics.class));

    ImmutableSegment factSegment = buildFactSegment();
    _indexSegment = factSegment;
    _indexSegments = Arrays.asList(factSegment);
  }

  @AfterClass
  public void tearDown() {
    if (_dimensionTableDataManager != null) {
      _dimensionTableDataManager.shutDown();
    }
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  private ImmutableSegment buildFactSegment()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(FACT_RAW_TABLE_NAME)
        .addSingleValueDimension(FACT_ID_COLUMN, DataType.INT)
        .addSingleValueDimension(FACT_BYTES_KEY_COLUMN, DataType.BYTES)
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(FACT_RAW_TABLE_NAME).build();

    List<GenericRow> records = new ArrayList<>();
    records.add(makeRow(FACT_ID_COLUMN, 1, FACT_BYTES_KEY_COLUMN, BYTES_ALPHA));
    records.add(makeRow(FACT_ID_COLUMN, 2, FACT_BYTES_KEY_COLUMN, BYTES_BETA));
    records.add(makeRow(FACT_ID_COLUMN, 3, FACT_BYTES_KEY_COLUMN, BYTES_NO_MATCH));

    return buildSegment(tableConfig, schema, FACT_SEGMENT_NAME, records);
  }

  private ImmutableSegment buildDimSegment()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(DIM_RAW_TABLE_NAME)
        .addSingleValueDimension(DIM_BYTES_PK_COLUMN, DataType.BYTES)
        .addSingleValueDimension(DIM_LABEL_COLUMN, DataType.STRING)
        .setPrimaryKeyColumns(List.of(DIM_BYTES_PK_COLUMN))
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(DIM_RAW_TABLE_NAME)
        .setDimensionTableConfig(new DimensionTableConfig(false /* disablePreload */, false))
        .build();

    List<GenericRow> records = new ArrayList<>();
    records.add(makeRow(DIM_BYTES_PK_COLUMN, BYTES_ALPHA, DIM_LABEL_COLUMN, "alpha"));
    records.add(makeRow(DIM_BYTES_PK_COLUMN, BYTES_BETA, DIM_LABEL_COLUMN, "beta"));

    return buildSegment(tableConfig, schema, DIM_SEGMENT_NAME, records);
  }

  private static GenericRow makeRow(Object... columnValuePairs) {
    GenericRow row = new GenericRow();
    for (int i = 0; i < columnValuePairs.length; i += 2) {
      row.putValue((String) columnValuePairs[i], columnValuePairs[i + 1]);
    }
    return row;
  }

  private ImmutableSegment buildSegment(TableConfig tableConfig, Schema schema, String segmentName,
      List<GenericRow> records)
      throws Exception {
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setTableName(schema.getSchemaName());
    segmentGeneratorConfig.setSegmentName(segmentName);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
    driver.build();

    return ImmutableSegmentLoader.load(new File(INDEX_DIR, segmentName), ReadMode.mmap);
  }

  private void registerRealDimensionTable(ImmutableSegment dimSegment, boolean disablePreload)
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(DIM_RAW_TABLE_NAME)
        .addSingleValueDimension(DIM_BYTES_PK_COLUMN, DataType.BYTES)
        .addSingleValueDimension(DIM_LABEL_COLUMN, DataType.STRING)
        .setPrimaryKeyColumns(List.of(DIM_BYTES_PK_COLUMN))
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(DIM_RAW_TABLE_NAME)
        .setDimensionTableConfig(new DimensionTableConfig(disablePreload, false))
        .build();

    ZkHelixPropertyStore<ZNRecord> propertyStoreMock = mock(ZkHelixPropertyStore.class);
    HelixManager helixManager = mock(HelixManager.class);
    when(propertyStoreMock.get("/CONFIGS/TABLE/" + DIM_OFFLINE_TABLE_NAME, null, AccessOption.PERSISTENT))
        .thenReturn(TableConfigSerDeUtils.toZNRecord(tableConfig));
    when(propertyStoreMock.get("/SCHEMAS/" + DIM_RAW_TABLE_NAME, null, AccessOption.PERSISTENT))
        .thenReturn(SchemaSerDeUtils.toZNRecord(schema));
    when(helixManager.getHelixPropertyStore()).thenReturn(propertyStoreMock);

    InstanceDataManagerConfig instanceDataManagerConfig = mock(InstanceDataManagerConfig.class);
    when(instanceDataManagerConfig.getInstanceDataDir()).thenReturn(INDEX_DIR.getAbsolutePath());

    _dimensionTableDataManager = DimensionTableDataManager.createInstanceByTableName(DIM_OFFLINE_TABLE_NAME);
    _dimensionTableDataManager.init(instanceDataManagerConfig, helixManager, new SegmentLocks(), tableConfig, schema,
        new SegmentReloadSemaphore(1), Executors.newSingleThreadExecutor(), null, null, SEGMENT_OPERATIONS_THROTTLER,
        false, mock(ServerReloadJobStatusCache.class));
    _dimensionTableDataManager.start();
    // This is what actually triggers loadLookupTable() -> createFastLookupDimensionTable(), i.e. the real
    // production code path, using real PinotSegmentRecordReader#getPrimaryKeys against the real segment.
    _dimensionTableDataManager.addSegment(dimSegment, null);
  }

  @Test
  public void testLookupReturnsPrimaryKeyBytesColumnUnwrappedWithFastLookupDimensionTable()
      throws Exception {
    ImmutableSegment dimSegment = buildDimSegment();
    registerRealDimensionTable(dimSegment, false);
    String query = "SELECT id, lookUp('" + DIM_RAW_TABLE_NAME + "', '" + DIM_BYTES_PK_COLUMN + "', '"
        + DIM_BYTES_PK_COLUMN + "', " + FACT_BYTES_KEY_COLUMN + ") FROM " + FACT_RAW_TABLE_NAME + " ORDER BY id";
    List<Object[]> rows = getBrokerResponse(query).getResultTable().getRows();
    // BaseQueriesTest#getBrokerResponse always simulates one OFFLINE and one REALTIME server response from the
    // same underlying segment(s) (see its dataTableMap construction), so with a single fact segment each logical
    // row appears twice -- 3 fact rows -> 6 returned rows. This is expected harness behavior, not a bug in the
    // query or the lookup logic, so assert per-id rather than on fixed row indices.
    assertEquals(rows.size(), 6);
    assertAllRowsForId(rows, 1, BYTES_ALPHA, "matching row (id=1) must return its own primary key bytes");
    assertAllRowsForId(rows, 2, BYTES_BETA, "matching row (id=2) must return its own primary key bytes");
    assertAllRowsForId(rows, 3, new byte[0], "non-matching row (id=3) has no dimension row to return");
  }

  @Test
  public void testLookupReturnsNonPrimaryKeyValueColumnFastLookupDimensionTable()
      throws Exception {
    ImmutableSegment dimSegment = buildDimSegment();
    registerRealDimensionTable(dimSegment, false);
    String query = "SELECT id, lookUp('" + DIM_RAW_TABLE_NAME + "', '" + DIM_LABEL_COLUMN + "', '"
        + DIM_BYTES_PK_COLUMN + "', " + FACT_BYTES_KEY_COLUMN + ") FROM " + FACT_RAW_TABLE_NAME + " ORDER BY id";
    List<Object[]> rows = getBrokerResponse(query).getResultTable().getRows();
    assertEquals(rows.size(), 6); // see comment in testLookupReturnsPrimaryKeyBytesColumnUnwrapped
    for (Object[] row : rows) {
      int id = (Integer) row[0];
      if (id == 1) {
        assertEquals(row[1], "alpha");
      } else if (id == 2) {
        assertEquals(row[1], "beta");
      }
    }
  }

  @Test
  public void testLookupReturnsPrimaryKeyBytesColumnUnwrappedWithMemOptimisedDimensionTable()
      throws Exception {
    ImmutableSegment dimSegment = buildDimSegment();
    registerRealDimensionTable(dimSegment, true);
    String query = "SELECT id, lookUp('" + DIM_RAW_TABLE_NAME + "', '" + DIM_BYTES_PK_COLUMN + "', '"
        + DIM_BYTES_PK_COLUMN + "', " + FACT_BYTES_KEY_COLUMN + ") FROM " + FACT_RAW_TABLE_NAME + " ORDER BY id";
    List<Object[]> rows = getBrokerResponse(query).getResultTable().getRows();
    // BaseQueriesTest#getBrokerResponse always simulates one OFFLINE and one REALTIME server response from the
    // same underlying segment(s) (see its dataTableMap construction), so with a single fact segment each logical
    // row appears twice -- 3 fact rows -> 6 returned rows. This is expected harness behavior, not a bug in the
    // query or the lookup logic, so assert per-id rather than on fixed row indices.
    assertEquals(rows.size(), 6);
    assertAllRowsForId(rows, 1, BYTES_ALPHA, "matching row (id=1) must return its own primary key bytes");
    assertAllRowsForId(rows, 2, BYTES_BETA, "matching row (id=2) must return its own primary key bytes");
    assertAllRowsForId(rows, 3, new byte[0], "non-matching row (id=3) has no dimension row to return");
  }

  @Test
  public void testLookupReturnsNonPrimaryKeyValueColumnMemOptimisedDimensionTable()
      throws Exception {
    ImmutableSegment dimSegment = buildDimSegment();
    registerRealDimensionTable(dimSegment, true);
    String query = "SELECT id, lookUp('" + DIM_RAW_TABLE_NAME + "', '" + DIM_LABEL_COLUMN + "', '"
        + DIM_BYTES_PK_COLUMN + "', " + FACT_BYTES_KEY_COLUMN + ") FROM " + FACT_RAW_TABLE_NAME + " ORDER BY id";
    List<Object[]> rows = getBrokerResponse(query).getResultTable().getRows();
    assertEquals(rows.size(), 6); // see comment in testLookupReturnsPrimaryKeyBytesColumnUnwrapped
    for (Object[] row : rows) {
      int id = (Integer) row[0];
      if (id == 1) {
        assertEquals(row[1], "alpha");
      } else if (id == 2) {
        assertEquals(row[1], "beta");
      }
    }
  }

  private static void assertAllRowsForId(List<Object[]> rows, int id, byte[] expectedBytes, String message) {
    int matchCount = 0;
    for (Object[] row : rows) {
      if (((Integer) row[0]) == id) {
        assertEquals(BytesUtils.toBytes((String) row[1]), expectedBytes, message);
        matchCount++;
      }
    }
    assertEquals(matchCount, 2, "expected exactly 2 rows for id=" + id + " (OFFLINE + REALTIME duplication)");
  }
}
