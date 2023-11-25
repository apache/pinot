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
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTableFactory;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.ExplainPlanRows;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.offline.TableDataManagerProvider;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.executor.ServerQueryExecutorV1Impl;
import org.apache.pinot.core.query.reduce.BrokerReduceService;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManagerConfig;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.CommonsConfigurationUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class ExplainPlanQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "ExplainPlanQueriesTest");
  private static final String QUERY_EXECUTOR_CONFIG_PATH = "conf/query-executor.properties";
  private static final ExecutorService QUERY_RUNNERS = Executors.newFixedThreadPool(20);

  private static final String RAW_TABLE_NAME = "testTable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);
  private static final String SEGMENT_NAME_1 = "testSegment1";
  private static final String SEGMENT_NAME_2 = "testSegment2";
  private static final String SEGMENT_NAME_3 = "testSegment3";
  private static final String SEGMENT_NAME_4 = "testSegment4";
  private static final int NUM_RECORDS = 10;

  private final static String COL1_RAW = "rawCol1";
  private final static String COL1_NO_INDEX = "noIndexCol1";
  private final static String COL2_NO_INDEX = "noIndexCol2";
  private final static String COL3_NO_INDEX = "noIndexCol3";
  private final static String COL4_NO_INDEX = "noIndexCol4";
  private final static String COL1_INVERTED_INDEX = "invertedIndexCol1";
  private final static String COL2_INVERTED_INDEX = "invertedIndexCol2";
  private final static String COL3_INVERTED_INDEX = "invertedIndexCol3";
  private final static String COL1_RANGE_INDEX = "rangeIndexCol1";
  private final static String COL2_RANGE_INDEX = "rangeIndexCol2";
  private final static String COL3_RANGE_INDEX = "rangeIndexCol3";
  private final static String COL1_SORTED_INDEX = "sortedIndexCol1";
  private final static String COL1_JSON_INDEX = "jsonIndexCol1";
  private final static String COL1_TEXT_INDEX = "textIndexCol1";
  private final static String MV_COL1_RAW = "mvRawCol1";
  private final static String MV_COL1_NO_INDEX = "mvNoIndexCol1";

  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
      .addSingleValueDimension(COL1_RAW, FieldSpec.DataType.INT)
      .addSingleValueDimension(COL1_NO_INDEX, FieldSpec.DataType.INT)
      .addSingleValueDimension(COL2_NO_INDEX, FieldSpec.DataType.INT)
      .addSingleValueDimension(COL3_NO_INDEX, FieldSpec.DataType.INT)
      .addSingleValueDimension(COL4_NO_INDEX, FieldSpec.DataType.BOOLEAN)
      .addSingleValueDimension(COL1_INVERTED_INDEX, FieldSpec.DataType.DOUBLE)
      .addSingleValueDimension(COL2_INVERTED_INDEX, FieldSpec.DataType.INT)
      .addSingleValueDimension(COL3_INVERTED_INDEX, FieldSpec.DataType.STRING)
      .addSingleValueDimension(COL1_RANGE_INDEX, FieldSpec.DataType.DOUBLE)
      .addSingleValueDimension(COL2_RANGE_INDEX, FieldSpec.DataType.INT)
      .addSingleValueDimension(COL3_RANGE_INDEX, FieldSpec.DataType.INT)
      .addSingleValueDimension(COL1_SORTED_INDEX, FieldSpec.DataType.DOUBLE)
      .addSingleValueDimension(COL1_JSON_INDEX, FieldSpec.DataType.JSON)
      .addSingleValueDimension(COL1_TEXT_INDEX, FieldSpec.DataType.STRING)
      .addMultiValueDimension(MV_COL1_RAW, FieldSpec.DataType.INT)
      .addMultiValueDimension(MV_COL1_NO_INDEX, FieldSpec.DataType.INT).build();

  private static final DataSchema DATA_SCHEMA = new DataSchema(new String[]{"Operator", "Operator_Id", "Parent_Id"},
      new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT,
          DataSchema.ColumnDataType.INT});

  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setNoDictionaryColumns(Arrays.asList(COL1_RAW, MV_COL1_RAW))
          .setTableName(RAW_TABLE_NAME).build();
  public static final String PROJEC_TNII_3 = "PROJECT(noIndexCol1, invertedIndexCol1, invertedIndexCol3)";
  public static final String INVERTED_INDEX_COL_1 = "PROJECT(noIndexCol1, invertedIndexCol1)";


  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;
  private List<String> _segmentNames;

  private ServerMetrics _serverMetrics;
  private QueryExecutor _queryExecutor;
  private QueryExecutor _queryExecutorWithPrefetchEnabled;
  private BrokerReduceService _brokerReduceService;

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

  GenericRow createMockRecord(int noIndexCol1, int noIndexCol2, int noIndexCol3, boolean noIndexCol4,
      double invertedIndexCol1, int invertedIndexCol2, String invertedIndexCol3, double rangeIndexCol1,
      int rangeIndexCol2, int rangeIndexCol3, double sortedIndexCol1, String jsonIndexCol1, String textIndexCol1,
      int rawCol1, Object[] mvRawCol1, Object[] mvNoIndexCol1) {

    GenericRow record = new GenericRow();
    record.putValue(COL1_RAW, rawCol1);

    record.putValue(COL1_NO_INDEX, noIndexCol1);
    record.putValue(COL2_NO_INDEX, noIndexCol2);
    record.putValue(COL3_NO_INDEX, noIndexCol3);
    record.putValue(COL4_NO_INDEX, noIndexCol4);

    record.putValue(COL1_INVERTED_INDEX, invertedIndexCol1);
    record.putValue(COL2_INVERTED_INDEX, invertedIndexCol2);
    record.putValue(COL3_INVERTED_INDEX, invertedIndexCol3);

    record.putValue(COL1_RANGE_INDEX, rangeIndexCol1);
    record.putValue(COL2_RANGE_INDEX, rangeIndexCol2);
    record.putValue(COL3_RANGE_INDEX, rangeIndexCol3);

    record.putValue(COL1_SORTED_INDEX, sortedIndexCol1);

    record.putValue(COL1_JSON_INDEX, jsonIndexCol1);
    record.putValue(COL1_TEXT_INDEX, textIndexCol1);

    record.putValue(MV_COL1_RAW, mvRawCol1);
    record.putValue(MV_COL1_NO_INDEX, mvNoIndexCol1);

    return record;
  }

  ImmutableSegment createImmutableSegment(List<GenericRow> records, String segmentName)
      throws Exception {
    IndexingConfig indexingConfig = TABLE_CONFIG.getIndexingConfig();

    List<String> invertedIndexColumns = Arrays.asList(COL1_INVERTED_INDEX, COL2_INVERTED_INDEX, COL3_INVERTED_INDEX);
    indexingConfig.setInvertedIndexColumns(invertedIndexColumns);

    List<String> rangeIndexColumns = Arrays.asList(COL1_RANGE_INDEX, COL2_RANGE_INDEX, COL3_RANGE_INDEX);
    indexingConfig.setRangeIndexColumns(rangeIndexColumns);

    List<String> sortedIndexColumns = Collections.singletonList(COL1_SORTED_INDEX);
    indexingConfig.setSortedColumn(sortedIndexColumns);

    List<String> jsonIndexColumns = Arrays.asList(COL1_JSON_INDEX);
    indexingConfig.setJsonIndexColumns(jsonIndexColumns);

    List<String> textIndexColumns = Arrays.asList(COL1_TEXT_INDEX);

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    segmentGeneratorConfig.setSegmentName(segmentName);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
    driver.build();

    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    indexLoadingConfig.setTableConfig(TABLE_CONFIG);
    indexLoadingConfig.setInvertedIndexColumns(new HashSet<>(invertedIndexColumns));
    indexLoadingConfig.setRangeIndexColumns(new HashSet<>(rangeIndexColumns));
    indexLoadingConfig.setJsonIndexColumns(new HashSet<>(jsonIndexColumns));
    indexLoadingConfig.setTextIndexColumns(new HashSet<>(textIndexColumns));
    indexLoadingConfig.setReadMode(ReadMode.mmap);

    _segmentNames.add(segmentName);

    return ImmutableSegmentLoader.load(new File(INDEX_DIR, segmentName), indexLoadingConfig);
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);
    _segmentNames = new ArrayList<>();

    List<GenericRow> records = new ArrayList<>(NUM_RECORDS);
    records.add(createMockRecord(1, 2, 3, true, 1.1, 2, "daffy", 10.1, 20, 30, 100.1,
        "{\"first\": \"daffy\", \"last\": \"duck\"}", "daffy", 1, new Object[]{1, 2, 3}, new Object[]{1, 2, 3}));
    records.add(createMockRecord(0, 1, 2, false, 0.1, 1, "mickey", 0.1, 10, 20, 100.2,
        "{\"first\": \"mickey\", \"last\": \"mouse\"}", "mickey", 0, new Object[]{2, 3, 4}, new Object[]{2, 3, 4}));
    records.add(createMockRecord(3, 4, 5, true, 2.1, 3, "mickey", 20.1, 30, 40, 100.3,
        "{\"first\": \"mickey\", \"last\": \"mouse\"}", "mickey", 3, new Object[]{3, 4, 5}, new Object[]{3, 4, 5}));
    ImmutableSegment immutableSegment1 = createImmutableSegment(records, SEGMENT_NAME_1);

    List<GenericRow> records2 = new ArrayList<>(NUM_RECORDS);
    records2.add(createMockRecord(5, 2, 3, true, 1.1, 2, "pluto", 10.1, 20, 30, 100.1,
        "{\"first\": \"pluto\", \"last\": \"dog\"}", "pluto", 5, new Object[]{100, 200, 300},
        new Object[]{100, 200, 300}));
    records2.add(createMockRecord(6, 1, 2, false, 0.1, 1, "pluto", 0.1, 10, 20, 100.2,
        "{\"first\": \"pluto\", \"last\": \"dog\"}", "pluto", 6, new Object[]{200, 300, 400},
        new Object[]{200, 300, 400}));
    records2.add(createMockRecord(8, 4, 5, true, 2.1, 3, "pluto", 20.1, 30, 40, 100.3,
        "{\"first\": \"pluto\", \"last\": \"dog\"}", "pluto", 8, new Object[]{300, 400, 500},
        new Object[]{300, 400, 500}));
    ImmutableSegment immutableSegment2 = createImmutableSegment(records2, SEGMENT_NAME_2);

    List<GenericRow> records3 = new ArrayList<>(NUM_RECORDS);
    records3.add(createMockRecord(5, 2, 3, true, 1.5, 2, "donald", 10.1, 20, 30, 100.1,
        "{\"first\": \"donald\", \"last\": \"duck\"}", "donald", 1, new Object[]{100, 200, 300},
        new Object[]{100, 200, 300}));
    records3.add(createMockRecord(6, 1, 2, false, 0.1, 1, "goofy", 0.1, 10, 20, 100.2,
        "{\"first\": \"goofy\", \"last\": \"dog\"}", "goofy", 1, new Object[]{100, 200, 300},
        new Object[]{100, 200, 300}));
    records3.add(createMockRecord(7, 4, 5, true, 2.1, 3, "minnie", 20.1, 30, 40, 100.3,
        "{\"first\": \"minnie\", \"last\": \"mouse\"}", "minnie", 1, new Object[]{1000, 2000, 3000},
        new Object[]{1000, 2000, 3000}));
    ImmutableSegment immutableSegment3 = createImmutableSegment(records3, SEGMENT_NAME_3);

    List<GenericRow> records4 = new ArrayList<>(NUM_RECORDS);
    records4.add(createMockRecord(5, 2, 3, true, 1.1, 2, "tweety", 10.1, 20, 30, 100.1,
        "{\"first\": \"tweety\", \"last\": \"bird\"}", "tweety", 5, new Object[]{100, 200, 300},
        new Object[]{100, 200, 300}));
    records4.add(createMockRecord(6, 1, 2, false, 0.1, 1, "bugs", 0.1, 10, 20, 100.2,
        "{\"first\": \"bugs\", \"last\": \"bunny\"}", "bugs", 6, new Object[]{100, 200, 300},
        new Object[]{100, 200, 300}));
    records4.add(createMockRecord(7, 4, 5, true, 2.1, 3, "sylvester", 20.1, 30, 40, 100.3,
        "{\"first\": \"sylvester\", \"last\": \"cat\"}", "sylvester", 7, new Object[]{1000, 2000, 3000},
        new Object[]{1000, 2000, 3000}));
    ImmutableSegment immutableSegment4 = createImmutableSegment(records4, SEGMENT_NAME_4);

    _indexSegment = immutableSegment1;
    _indexSegments = Arrays.asList(immutableSegment1, immutableSegment2, immutableSegment3, immutableSegment4);

    // Mock the instance data manager
    _serverMetrics = new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    TableDataManagerConfig tableDataManagerConfig = mock(TableDataManagerConfig.class);
    when(tableDataManagerConfig.getTableName()).thenReturn(OFFLINE_TABLE_NAME);
    when(tableDataManagerConfig.getTableType()).thenReturn(TableType.OFFLINE);
    when(tableDataManagerConfig.getDataDir()).thenReturn(FileUtils.getTempDirectoryPath());
    InstanceDataManagerConfig instanceDataManagerConfig = mock(InstanceDataManagerConfig.class);
    when(instanceDataManagerConfig.getMaxParallelSegmentBuilds()).thenReturn(4);
    when(instanceDataManagerConfig.getStreamSegmentDownloadUntarRateLimit()).thenReturn(-1L);
    when(instanceDataManagerConfig.getMaxParallelSegmentDownloads()).thenReturn(-1);
    when(instanceDataManagerConfig.isStreamSegmentDownloadUntar()).thenReturn(false);
    TableDataManagerProvider.init(instanceDataManagerConfig);
    @SuppressWarnings("unchecked")
    TableDataManager tableDataManager =
        TableDataManagerProvider.getTableDataManager(tableDataManagerConfig, "testInstance",
            mock(ZkHelixPropertyStore.class), mock(ServerMetrics.class), mock(HelixManager.class), null);
    tableDataManager.start();
    for (IndexSegment indexSegment : _indexSegments) {
      tableDataManager.addSegment((ImmutableSegment) indexSegment);
    }
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    when(instanceDataManager.getTableDataManager(OFFLINE_TABLE_NAME)).thenReturn(tableDataManager);

    // Set up the query executor
    URL resourceUrl = getClass().getClassLoader().getResource(QUERY_EXECUTOR_CONFIG_PATH);
    Assert.assertNotNull(resourceUrl);
    PropertiesConfiguration queryExecutorConfig =
        CommonsConfigurationUtils.loadFromFile(new File(resourceUrl.getFile()));
    _queryExecutor = new ServerQueryExecutorV1Impl();
    _queryExecutor.init(new PinotConfiguration(queryExecutorConfig), instanceDataManager, _serverMetrics);

    PinotConfiguration prefetchEnabledConf = new PinotConfiguration(queryExecutorConfig);
    prefetchEnabledConf.setProperty(ServerQueryExecutorV1Impl.ENABLE_PREFETCH, "true");
    _queryExecutorWithPrefetchEnabled = new ServerQueryExecutorV1Impl();
    _queryExecutorWithPrefetchEnabled.init(prefetchEnabledConf, instanceDataManager, _serverMetrics);

    // Create the BrokerReduceService
    _brokerReduceService = new BrokerReduceService(new PinotConfiguration(
        Collections.singletonMap(CommonConstants.Broker.CONFIG_OF_MAX_REDUCE_THREADS_PER_QUERY, 2)));
  }

  private ResultTable getPrefetchEnabledResulTable(ResultTable resultTable) {
    // TODO does not work for cases where different segments have different plans (more than 1 PLAN_START rows)
    List<Object[]> newRows = new ArrayList<>();
    int acquireOpParentId = -1;

    Iterator<Object[]> it = resultTable.getRows().iterator();
    // After each PLAN_START, we need to add ACQUIRE_RELEASE_COLUMNS_SEGMENT (unles ALL_SEGMENTS_PRUNED_ON_SERVER),
    // and all following op should have their ids incremented

    while (it.hasNext()) {
      Object[] row = it.next();
      String op = row[0].toString();
      newRows.add(row);
      acquireOpParentId = Math.max(acquireOpParentId, (int) row[1]);
      if (op.startsWith("PLAN_START")) {
        newRows.add(new Object[]{"ACQUIRE_RELEASE_COLUMNS_SEGMENT", acquireOpParentId + 1, acquireOpParentId});
        break;
      }
    }

    while (it.hasNext()) {
      Object[] row = it.next();
      newRows.add(new Object[]{row[0].toString(), ((int) row[1] + 1), ((int) row[2]) + 1});
    }

    return new ResultTable(resultTable.getDataSchema(), newRows);
  }

  /** Checks the correctness of EXPLAIN PLAN output. */
  private void check(String query, ResultTable expected) {
    check(query, expected, false);
  }

  private void check(String query, ResultTable expected, boolean checkPrefetchEnabled) {
    checkWithQueryExecutor(query, expected, _queryExecutor);
    if (checkPrefetchEnabled) {
      checkWithQueryExecutor(query, getPrefetchEnabledResulTable(expected), _queryExecutorWithPrefetchEnabled);
    }
  }

  private void checkWithQueryExecutor(String query, ResultTable expected, QueryExecutor queryExecutor) {
    BrokerRequest brokerRequest = CalciteSqlCompiler.compileToBrokerRequest(query);

    int segmentsForServer1 = _segmentNames.size() / 2;
    List<String> indexSegmentsForServer1 = new ArrayList<>();
    List<String> indexSegmentsForServer2 = new ArrayList<>();
    for (int i = 0; i < getIndexSegments().size(); i++) {
      if (i < segmentsForServer1) {
        indexSegmentsForServer1.add(_segmentNames.get(i));
      } else {
        indexSegmentsForServer2.add(_segmentNames.get(i));
      }
    }

    // Target OFFLINE table on the server side
    brokerRequest.getPinotQuery().getDataSource().setTableName(OFFLINE_TABLE_NAME);
    InstanceRequest instanceRequest1 = new InstanceRequest(0L, brokerRequest);
    instanceRequest1.setSearchSegments(indexSegmentsForServer1);
    InstanceResponseBlock instanceResponse1 = queryExecutor.execute(getQueryRequest(instanceRequest1), QUERY_RUNNERS);
    InstanceRequest instanceRequest2 = new InstanceRequest(0L, brokerRequest);
    instanceRequest2.setSearchSegments(indexSegmentsForServer2);
    InstanceResponseBlock instanceResponse2 = queryExecutor.execute(getQueryRequest(instanceRequest2), QUERY_RUNNERS);

    // Broker side
    // Use 2 Threads for 2 data-tables
    // Different segments are assigned to each set of DataTables. This is necessary to simulate scenarios where
    // certain segments may completely be pruned on one server but not the other.
    Map<ServerRoutingInstance, DataTable> dataTableMap = new HashMap<>();
    try {
      // For multi-threaded BrokerReduceService, we cannot reuse the same data-table
      byte[] serializedResponse1 = instanceResponse1.toDataTable().toBytes();
      dataTableMap.put(new ServerRoutingInstance("localhost", 1234, TableType.OFFLINE),
          DataTableFactory.getDataTable(serializedResponse1));
      byte[] serializedResponse2 = instanceResponse2.toDataTable().toBytes();
      dataTableMap.put(new ServerRoutingInstance("localhost", 1234, TableType.REALTIME),
          DataTableFactory.getDataTable(serializedResponse2));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    // Target raw table on the broker side
    brokerRequest.getPinotQuery().getDataSource().setTableName(RAW_TABLE_NAME);
    BrokerResponseNative brokerResponse =
        _brokerReduceService.reduceOnDataTable(brokerRequest, brokerRequest, dataTableMap,
            CommonConstants.Broker.DEFAULT_BROKER_TIMEOUT_MS, null);

    QueriesTestUtils.testExplainSegmentsResult(brokerResponse, expected);
  }

  private ServerQueryRequest getQueryRequest(InstanceRequest instanceRequest) {
    return new ServerQueryRequest(instanceRequest, _serverMetrics, System.currentTimeMillis());
  }

  @Test
  public void testSelect() {
    // All segment plans for these queries generate a plan using the MatchAllFilterOperator as these queries select
    // columns without filtering
    String query1 = "EXPLAIN PLAN FOR SELECT * FROM testTable";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result1.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result1.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result1.add(new Object[]{
        "SELECT(selectList:invertedIndexCol1, invertedIndexCol2, invertedIndexCol3, jsonIndexCol1, mvNoIndexCol1, "
            + "mvRawCol1, noIndexCol1, noIndexCol2, noIndexCol3, noIndexCol4, rangeIndexCol1, rangeIndexCol2, "
            + "rangeIndexCol3, rawCol1, sortedIndexCol1, textIndexCol1)", 3, 2
    });
    result1.add(new Object[]{
        "PROJECT(invertedIndexCol1, invertedIndexCol2, invertedIndexCol3, jsonIndexCol1, mvNoIndexCol1"
            + ", mvRawCol1, noIndexCol1, noIndexCol2, noIndexCol3, noIndexCol4, rangeIndexCol1, rangeIndexCol2, "
            + "rangeIndexCol3, rawCol1, sortedIndexCol1, textIndexCol1)", 4, 3
    });
    result1.add(new Object[]{"DOC_ID_SET", 5, 4});
    result1.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 6, 5});
    check(query1, new ResultTable(DATA_SCHEMA, result1), true);

    String query2 = "EXPLAIN PLAN FOR SELECT 'mickey' FROM testTable";
    List<Object[]> result2 = new ArrayList<>();
    result2.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result2.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result2.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result2.add(new Object[]{"SELECT(selectList:'mickey')", 3, 2});
    result2.add(new Object[]{"TRANSFORM('mickey')", 4, 3});
    result2.add(new Object[]{"PROJECT()", 5, 4});
    result2.add(new Object[]{"DOC_ID_SET", 6, 5});
    result2.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 7, 6});
    check(query2, new ResultTable(DATA_SCHEMA, result2), true);

    String query3 = "EXPLAIN PLAN FOR SELECT invertedIndexCol1, noIndexCol1 FROM testTable LIMIT 100";
    List<Object[]> result3 = new ArrayList<>();
    result3.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result3.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result3.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result3.add(new Object[]{"SELECT(selectList:invertedIndexCol1, noIndexCol1)", 3, 2});
    result3.add(new Object[]{"PROJECT(invertedIndexCol1, noIndexCol1)", 4, 3});
    result3.add(new Object[]{"DOC_ID_SET", 5, 4});
    result3.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 6, 5});
    check(query3, new ResultTable(DATA_SCHEMA, result3), true);

    String query4 = "EXPLAIN PLAN FOR SELECT DISTINCT invertedIndexCol1, noIndexCol1 FROM testTable LIMIT 100";
    List<Object[]> result4 = new ArrayList<>();
    result4.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result4.add(new Object[]{"COMBINE_DISTINCT", 2, 1});
    result4.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result4.add(new Object[]{"DISTINCT(keyColumns:invertedIndexCol1, noIndexCol1)", 3, 2});
    result4.add(new Object[]{"PROJECT(invertedIndexCol1, noIndexCol1)", 4, 3});
    result4.add(new Object[]{"DOC_ID_SET", 5, 4});
    result4.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 6, 5});
    check(query4, new ResultTable(DATA_SCHEMA, result4), true);
  }

  @Test
  public void testSelectVerbose() {
    // All segment plans for these queries generate a plan using the MatchAllFilterOperator as these queries select
    // columns without filtering
    String query1 = "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT * FROM testTable";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result1.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result1.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result1.add(new Object[]{
        "SELECT(selectList:invertedIndexCol1, invertedIndexCol2, invertedIndexCol3, jsonIndexCol1, mvNoIndexCol1, "
            + "mvRawCol1, noIndexCol1, noIndexCol2, noIndexCol3, noIndexCol4, rangeIndexCol1, rangeIndexCol2, "
            + "rangeIndexCol3, rawCol1, sortedIndexCol1, textIndexCol1)", 3, 2
    });
    result1.add(new Object[]{
        "PROJECT(invertedIndexCol1, invertedIndexCol2, invertedIndexCol3, jsonIndexCol1, mvNoIndexCol1, "
            + "mvRawCol1, noIndexCol1, noIndexCol2, noIndexCol3, noIndexCol4, rangeIndexCol1, rangeIndexCol2, "
            + "rangeIndexCol3, rawCol1, sortedIndexCol1, textIndexCol1)", 4, 3
    });
    result1.add(new Object[]{"DOC_ID_SET", 5, 4});
    result1.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 6, 5});
    check(query1, new ResultTable(DATA_SCHEMA, result1));

    String query2 = "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT 'mickey' FROM testTable";
    List<Object[]> result2 = new ArrayList<>();
    result2.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result2.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result2.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result2.add(new Object[]{"SELECT(selectList:'mickey')", 3, 2});
    result2.add(new Object[]{"TRANSFORM('mickey')", 4, 3});
    result2.add(new Object[]{"PROJECT()", 5, 4});
    result2.add(new Object[]{"DOC_ID_SET", 6, 5});
    result2.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 7, 6});
    check(query2, new ResultTable(DATA_SCHEMA, result2));

    String query3 = "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT invertedIndexCol1, noIndexCol1 FROM "
        + "testTable LIMIT 100";
    List<Object[]> result3 = new ArrayList<>();
    result3.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result3.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result3.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result3.add(new Object[]{"SELECT(selectList:invertedIndexCol1, noIndexCol1)", 3, 2});
    result3.add(new Object[]{"PROJECT(invertedIndexCol1, noIndexCol1)", 4, 3});
    result3.add(new Object[]{"DOC_ID_SET", 5, 4});
    result3.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 6, 5});
    check(query3, new ResultTable(DATA_SCHEMA, result3));

    String query4 = "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT DISTINCT invertedIndexCol1, noIndexCol1 "
        + "FROM testTable LIMIT 100";
    List<Object[]> result4 = new ArrayList<>();
    result4.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result4.add(new Object[]{"COMBINE_DISTINCT", 2, 1});
    result4.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result4.add(new Object[]{"DISTINCT(keyColumns:invertedIndexCol1, noIndexCol1)", 3, 2});
    result4.add(new Object[]{"PROJECT(invertedIndexCol1, noIndexCol1)", 4, 3});
    result4.add(new Object[]{"DOC_ID_SET", 5, 4});
    result4.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 6, 5});
    check(query4, new ResultTable(DATA_SCHEMA, result4));
  }

  @Test
  public void testSelectTransformFunction() {
    // All segment plans for these queries generate a plan using the MatchAllFilterOperator as these queries select
    // columns without filtering and apply transforms
    String query1 = "EXPLAIN PLAN FOR SELECT CASE WHEN noIndexCol1 < 10 THEN 'less' ELSE 'more' END  FROM testTable";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result1.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result1.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result1.add(new Object[]{"SELECT(selectList:case(less_than(noIndexCol1,'10'),'less','more'))", 3, 2});
    result1.add(new Object[]{"TRANSFORM(case(less_than(noIndexCol1,'10'),'less','more'))", 4, 3});
    result1.add(new Object[]{"PROJECT(noIndexCol1)", 5, 4});
    result1.add(new Object[]{"DOC_ID_SET", 6, 5});
    result1.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 7, 6});
    check(query1, new ResultTable(DATA_SCHEMA, result1));

    String query2 = "EXPLAIN PLAN FOR SELECT CONCAT(textIndexCol1, textIndexCol1, ':') FROM testTable";
    List<Object[]> result2 = new ArrayList<>();
    result2.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result2.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result2.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result2.add(new Object[]{"SELECT(selectList:concat(textIndexCol1,textIndexCol1,':'))", 3, 2});
    result2.add(new Object[]{"TRANSFORM(concat(textIndexCol1,textIndexCol1,':'))", 4, 3});
    result2.add(new Object[]{"PROJECT(textIndexCol1)", 5, 4});
    result2.add(new Object[]{"DOC_ID_SET", 6, 5});
    result2.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 7, 6});
    check(query2, new ResultTable(DATA_SCHEMA, result2));
  }

  @Test
  public void testSelectTransformFunctionVerbose() {
    // All segment plans for these queries generate a plan using the MatchAllFilterOperator as these queries select
    // columns without filtering and apply transforms
    String query1 = "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT CASE WHEN noIndexCol1 < 10 THEN 'less' "
        + "ELSE 'more' END  FROM testTable";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result1.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result1.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result1.add(new Object[]{"SELECT(selectList:case(less_than(noIndexCol1,'10'),'less','more'))", 3, 2});
    result1.add(new Object[]{"TRANSFORM(case(less_than(noIndexCol1,'10'),'less','more'))", 4, 3});
    result1.add(new Object[]{"PROJECT(noIndexCol1)", 5, 4});
    result1.add(new Object[]{"DOC_ID_SET", 6, 5});
    result1.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 7, 6});
    check(query1, new ResultTable(DATA_SCHEMA, result1));

    String query2 = "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT CONCAT(textIndexCol1, textIndexCol1, ':') "
        + "FROM testTable";
    List<Object[]> result2 = new ArrayList<>();
    result2.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result2.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result2.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result2.add(new Object[]{"SELECT(selectList:concat(textIndexCol1,textIndexCol1,':'))", 3, 2});
    result2.add(new Object[]{"TRANSFORM(concat(textIndexCol1,textIndexCol1,':'))", 4, 3});
    result2.add(new Object[]{"PROJECT(textIndexCol1)", 5, 4});
    result2.add(new Object[]{"DOC_ID_SET", 6, 5});
    result2.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 7, 6});
    check(query2, new ResultTable(DATA_SCHEMA, result2));
  }

  @Test
  public void testSelectOrderBy() {
    // All segment plans for these queries generate a plan using the MatchAllFilterOperator as these queries select
    // columns without filtering and apply transforms along with order by
    String query1 = "EXPLAIN PLAN FOR SELECT CASE WHEN noIndexCol1 < 10 THEN 'less' ELSE 'more' END  FROM testTable "
        + "ORDER BY 1";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(
        new Object[]{"BROKER_REDUCE(sort:[case(less_than(noIndexCol1,'10'),'less','more') ASC],limit:10)", 1, 0});
    result1.add(new Object[]{"COMBINE_SELECT_ORDERBY", 2, 1});
    result1.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result1.add(new Object[]{"SELECT_ORDERBY(selectList:case(less_than(noIndexCol1,'10'),'less','more'))", 3, 2});
    result1.add(new Object[]{"TRANSFORM(case(less_than(noIndexCol1,'10'),'less','more'))", 4, 3});
    result1.add(new Object[]{"PROJECT(noIndexCol1)", 5, 4});
    result1.add(new Object[]{"DOC_ID_SET", 6, 5});
    result1.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 7, 6});
    check(query1, new ResultTable(DATA_SCHEMA, result1));

    String query2 = "EXPLAIN PLAN FOR SELECT CONCAT(textIndexCol1, textIndexCol1, ':') FROM testTable ORDER BY 1 DESC";
    List<Object[]> result2 = new ArrayList<>();
    result2.add(new Object[]{"BROKER_REDUCE(sort:[concat(textIndexCol1,textIndexCol1,':') DESC],limit:10)", 1, 0});
    result2.add(new Object[]{"COMBINE_SELECT_ORDERBY", 2, 1});
    result2.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result2.add(new Object[]{"SELECT_ORDERBY(selectList:concat(textIndexCol1,textIndexCol1,':'))", 3, 2});
    result2.add(new Object[]{"TRANSFORM(concat(textIndexCol1,textIndexCol1,':'))", 4, 3});
    result2.add(new Object[]{"PROJECT(textIndexCol1)", 5, 4});
    result2.add(new Object[]{"DOC_ID_SET", 6, 5});
    result2.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 7, 6});
    check(query2, new ResultTable(DATA_SCHEMA, result2));
  }

  @Test
  public void testSelectOrderByVerbose() {
    // All segment plans for these queries generate a plan using the MatchAllFilterOperator as these queries select
    // columns without filtering and apply transforms along with order by
    String query1 = "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT CASE WHEN noIndexCol1 < 10 THEN 'less' ELSE "
        + "'more' END  FROM testTable ORDER BY 1";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(
        new Object[]{"BROKER_REDUCE(sort:[case(less_than(noIndexCol1,'10'),'less','more') ASC],limit:10)", 1, 0});
    result1.add(new Object[]{"COMBINE_SELECT_ORDERBY", 2, 1});
    result1.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result1.add(new Object[]{"SELECT_ORDERBY(selectList:case(less_than(noIndexCol1,'10'),'less','more'))", 3, 2});
    result1.add(new Object[]{"TRANSFORM(case(less_than(noIndexCol1,'10'),'less','more'))", 4, 3});
    result1.add(new Object[]{"PROJECT(noIndexCol1)", 5, 4});
    result1.add(new Object[]{"DOC_ID_SET", 6, 5});
    result1.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 7, 6});
    check(query1, new ResultTable(DATA_SCHEMA, result1));

    String query2 = "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT CONCAT(textIndexCol1, textIndexCol1, ':') "
        + "FROM testTable ORDER BY 1 DESC ";
    List<Object[]> result2 = new ArrayList<>();
    result2.add(new Object[]{"BROKER_REDUCE(sort:[concat(textIndexCol1,textIndexCol1,':') DESC],limit:10)", 1, 0});
    result2.add(new Object[]{"COMBINE_SELECT_ORDERBY", 2, 1});
    result2.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result2.add(new Object[]{"SELECT_ORDERBY(selectList:concat(textIndexCol1,textIndexCol1,':'))", 3, 2});
    result2.add(new Object[]{"TRANSFORM(concat(textIndexCol1,textIndexCol1,':'))", 4, 3});
    result2.add(new Object[]{"PROJECT(textIndexCol1)", 5, 4});
    result2.add(new Object[]{"DOC_ID_SET", 6, 5});
    result2.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 7, 6});
    check(query2, new ResultTable(DATA_SCHEMA, result2));
  }

  /** Test case for SQL statements with filter that doesn't involve index access. */
  @Test
  public void testSelectColumnsUsingFilter() {
    // MatchAllFilterOperator is returned for all segments because the predicate `sortedIndexCol1 != 5` is true for all
    // values across all segments. The FILTER_OR doesn't show up in the plan because the other two OR predicates are
    // not true and removed from the query plan.
    String query1 =
        "EXPLAIN PLAN FOR SELECT noIndexCol1, noIndexCol2, sortedIndexCol1 FROM testTable WHERE sortedIndexCol1 = 1.5"
            + " OR sortedIndexCol1 != 5 OR sortedIndexCol1 IN (10, 20, 30) LIMIT 100";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result1.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result1.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result1.add(new Object[]{"SELECT(selectList:noIndexCol1, noIndexCol2, sortedIndexCol1)", 3, 2});
    result1.add(new Object[]{"PROJECT(noIndexCol1, noIndexCol2, sortedIndexCol1)", 4, 3});
    result1.add(new Object[]{"DOC_ID_SET", 5, 4});
    result1.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 6, 5});
    check(query1, new ResultTable(DATA_SCHEMA, result1));

    // The FILTER_AND is part of the query plan for all segments as both predicates are expressions
    String query2 =
        "EXPLAIN PLAN FOR SELECT noIndexCol1, noIndexCol2 FROM testTable WHERE DIV(noIndexCol1, noIndexCol2) BETWEEN "
            + "10 AND 20 AND invertedIndexCol1 * 5 < 1000";
    List<Object[]> result2 = new ArrayList<>();
    result2.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result2.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result2.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result2.add(new Object[]{"SELECT(selectList:noIndexCol1, noIndexCol2)", 3, 2});
    result2.add(new Object[]{"PROJECT(noIndexCol1, noIndexCol2)", 4, 3});
    result2.add(new Object[]{"DOC_ID_SET", 5, 4});
    result2.add(new Object[]{"FILTER_AND", 6, 5});
    result2.add(new Object[]{
        "FILTER_EXPRESSION(operator:RANGE,predicate:div(noIndexCol1,noIndexCol2) BETWEEN '10' AND '20')", 7, 6
    });
    result2.add(
        new Object[]{"FILTER_EXPRESSION(operator:RANGE,predicate:times(invertedIndexCol1,'5') < '1000')", 8, 6});
    check(query2, new ResultTable(DATA_SCHEMA, result2));

    // All segments have a match for noIndexCol2 'between 2 and 101'
    // Segments 2, 3, 4 don't have a single value of noIndexCol1 as less than 1, whereas segment 1 has a 0 as
    // one of the rows for noIndexCol1. Due to this the three segments for which noIndexCol1 > 1 is true for all rows,
    // a MatchAllFilterOperator plan is returned. For the other segment, the actual FILTER_OR query plan is returned.
    // Since verbose mode is turned off, the deepest plan is returned which is the FILTER_OR query plan in this case.
    String query3 =
        "EXPLAIN PLAN FOR SELECT noIndexCol1, invertedIndexCol1 FROM testTable WHERE noIndexCol1 > 1 OR noIndexCol2"
            + " BETWEEN 2 AND 101 LIMIT 100";
    List<Object[]> result3 = new ArrayList<>();
    result3.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result3.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result3.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result3.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1)", 3, 2});
    result3.add(new Object[]{INVERTED_INDEX_COL_1, 4, 3});
    result3.add(new Object[]{"DOC_ID_SET", 5, 4});
    result3.add(new Object[]{"FILTER_OR", 6, 5});
    result3.add(new Object[]{"FILTER_FULL_SCAN(operator:RANGE,predicate:noIndexCol1 > '1')", 7, 6});
    result3.add(new Object[]{"FILTER_FULL_SCAN(operator:RANGE,predicate:noIndexCol2 BETWEEN '2' AND '101')", 8, 6});
    check(query3, new ResultTable(DATA_SCHEMA, result3));

    // All segments have a match for noIndexCol2 'between 2 and 101'
    // Segments 2, 3, 4 don't have a single value of noIndexCol1 as less than 1, whereas segment 1 has a 0 as
    // one of the rows for noIndexCol1. Due to this the three segments for which noIndexCol1 > 1 is true for all rows,
    // a MatchAllFilterOperator plan is returned. For the other segment, the actual FILTER_OR query plan is returned.
    // Since verbose mode is turned off, the deepest plan is returned which is the FILTER_OR query plan in this case.
    String query4 = "EXPLAIN PLAN FOR SELECT invertedIndexCol1, noIndexCol1 FROM testTable WHERE noIndexCol1 > 1 OR "
        + "contains(textIndexCol1, 'daff') OR noIndexCol2 BETWEEN 2 AND 101 LIMIT 100";
    List<Object[]> result4 = new ArrayList<>();
    result4.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result4.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result4.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result4.add(new Object[]{"SELECT(selectList:invertedIndexCol1, noIndexCol1)", 3, 2});
    result4.add(new Object[]{"PROJECT(invertedIndexCol1, noIndexCol1)", 4, 3});
    result4.add(new Object[]{"DOC_ID_SET", 5, 4});
    result4.add(new Object[]{"FILTER_OR", 6, 5});
    result4.add(new Object[]{"FILTER_FULL_SCAN(operator:RANGE,predicate:noIndexCol1 > '1')", 7, 6});
    result4.add(new Object[]{"FILTER_EXPRESSION(operator:EQ,predicate:contains(textIndexCol1,'daff') = 'true')", 8, 6});
    result4.add(new Object[]{"FILTER_FULL_SCAN(operator:RANGE,predicate:noIndexCol2 BETWEEN '2' AND '101')", 9, 6});
    check(query4, new ResultTable(DATA_SCHEMA, result4));

    // All segments match for a full scan since noIndexCol4 has at least one row value set to 'true' across all segments
    String query5 = "EXPLAIN PLAN FOR SELECT invertedIndexCol1, noIndexCol1 FROM testTable WHERE noIndexCol4 LIMIT 100";
    List<Object[]> result5 = new ArrayList<>();
    result5.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result5.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result5.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result5.add(new Object[]{"SELECT(selectList:invertedIndexCol1, noIndexCol1)", 3, 2});
    result5.add(new Object[]{"PROJECT(invertedIndexCol1, noIndexCol1)", 4, 3});
    result5.add(new Object[]{"DOC_ID_SET", 5, 4});
    result5.add(new Object[]{"FILTER_FULL_SCAN(operator:EQ,predicate:noIndexCol4 = 'true')", 6, 5});
    check(query5, new ResultTable(DATA_SCHEMA, result5));

    // The FILTER_AND is part of the query plan for all segments as the first predicate is an expression and the second
    // has at least one matching row in each segment
    String query6 = "EXPLAIN PLAN FOR SELECT invertedIndexCol1, noIndexCol1 FROM testTable WHERE startsWith "
        + "(textIndexCol1, 'daff') AND noIndexCol4";
    List<Object[]> result6 = new ArrayList<>();
    result6.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result6.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result6.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result6.add(new Object[]{"SELECT(selectList:invertedIndexCol1, noIndexCol1)", 3, 2});
    result6.add(new Object[]{"PROJECT(invertedIndexCol1, noIndexCol1)", 4, 3});
    result6.add(new Object[]{"DOC_ID_SET", 5, 4});
    result6.add(new Object[]{"FILTER_AND", 6, 5});
    result6.add(new Object[]{"FILTER_FULL_SCAN(operator:EQ,predicate:noIndexCol4 = 'true')", 7, 6});
    result6.add(new Object[]{
        "FILTER_EXPRESSION(operator:EQ,predicate:startswith(textIndexCol1,'daff') = 'true')", 8, 6
    });
    check(query6, new ResultTable(DATA_SCHEMA, result6));
  }

  /** Test case for SQL statements with filter that doesn't involve index access. */
  @Test
  public void testSelectColumnsUsingFilterVerbose() {
    // MatchAllFilterOperator is returned for all segments because the predicate `sortedIndexCol1 != 5` is true for all
    // values across all segments. The FILTER_OR doesn't show up in the plan because the other two OR predicates are
    // not true and removed from the query plan.
    String query1 =
        "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT noIndexCol1, noIndexCol2, sortedIndexCol1 FROM "
            + "testTable WHERE sortedIndexCol1 = 1.5 OR sortedIndexCol1 != 5 OR sortedIndexCol1 IN (10, 20, 30) "
            + "LIMIT 100";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result1.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result1.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result1.add(new Object[]{"SELECT(selectList:noIndexCol1, noIndexCol2, sortedIndexCol1)", 3, 2});
    result1.add(new Object[]{"PROJECT(noIndexCol1, noIndexCol2, sortedIndexCol1)", 4, 3});
    result1.add(new Object[]{"DOC_ID_SET", 5, 4});
    result1.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 6, 5});
    check(query1, new ResultTable(DATA_SCHEMA, result1));

    // The FILTER_AND is part of the query plan for all segments as both predicates are expressions
    String query2 =
        "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT noIndexCol1, noIndexCol2 FROM testTable WHERE "
            + "DIV(noIndexCol1, noIndexCol2) BETWEEN 10 AND 20 AND invertedIndexCol1 * 5 < 1000";
    List<Object[]> result2 = new ArrayList<>();
    result2.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result2.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result2.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result2.add(new Object[]{"SELECT(selectList:noIndexCol1, noIndexCol2)", 3, 2});
    result2.add(new Object[]{"PROJECT(noIndexCol1, noIndexCol2)", 4, 3});
    result2.add(new Object[]{"DOC_ID_SET", 5, 4});
    result2.add(new Object[]{"FILTER_AND", 6, 5});
    result2.add(new Object[]{
        "FILTER_EXPRESSION(operator:RANGE,predicate:div(noIndexCol1,noIndexCol2) BETWEEN '10' AND '20')", 7, 6
    });
    result2.add(
        new Object[]{"FILTER_EXPRESSION(operator:RANGE,predicate:times(invertedIndexCol1,'5') < '1000')", 8, 6});
    check(query2, new ResultTable(DATA_SCHEMA, result2));

    // All segments have a match for noIndexCol2 'between 2 and 101'
    // Segments 2, 3, 4 don't have a single value of noIndexCol1 as less than 1, whereas segment 1 has a 0 as
    // one of the rows for noIndexCol1. Due to this the three segments for which noIndexCol1 > 1 is true for all rows,
    // a MatchAllFilterOperator plan is returned. For the other segment, the actual FILTER_OR query plan is returned.
    String query3 =
        "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT noIndexCol1, invertedIndexCol1 FROM testTable "
            + "WHERE noIndexCol1 > 1 OR noIndexCol2 BETWEEN 2 AND 101 LIMIT 100";
    List<Object[]> result3 = new ArrayList<>();
    result3.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result3.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result3.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result3.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1)", 3, 2});
    result3.add(new Object[]{INVERTED_INDEX_COL_1, 4, 3});
    result3.add(new Object[]{"DOC_ID_SET", 5, 4});
    result3.add(new Object[]{"FILTER_OR", 6, 5});
    result3.add(new Object[]{"FILTER_FULL_SCAN(operator:RANGE,predicate:noIndexCol1 > '1')", 7, 6});
    result3.add(new Object[]{"FILTER_FULL_SCAN(operator:RANGE,predicate:noIndexCol2 BETWEEN '2' AND '101')", 8, 6});
    result3.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:3)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result3.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1)", 3, 2});
    result3.add(new Object[]{INVERTED_INDEX_COL_1, 4, 3});
    result3.add(new Object[]{"DOC_ID_SET", 5, 4});
    result3.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 6, 5});
    check(query3, new ResultTable(DATA_SCHEMA, result3));

    // All segments have a match for noIndexCol2 'between 2 and 101'
    // Segments 2, 3, 4 don't have a single value of noIndexCol1 as less than 1, whereas segment 1 has a 0 as
    // one of the rows for noIndexCol1. Due to this the three segments for which noIndexCol1 > 1 is true for all rows,
    // a MatchAllFilterOperator plan is returned. For the other segment, the actual FILTER_OR query plan is returned.
    String query4 = "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT invertedIndexCol1, noIndexCol1 FROM "
        + "testTable WHERE noIndexCol1 > 1 OR contains(textIndexCol1, 'daff') OR noIndexCol2 BETWEEN 2 AND 101 "
        + "LIMIT 100";
    List<Object[]> result4 = new ArrayList<>();
    result4.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result4.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result4.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:3)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result4.add(new Object[]{"SELECT(selectList:invertedIndexCol1, noIndexCol1)", 3, 2});
    result4.add(new Object[]{"PROJECT(invertedIndexCol1, noIndexCol1)", 4, 3});
    result4.add(new Object[]{"DOC_ID_SET", 5, 4});
    result4.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 6, 5});
    result4.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result4.add(new Object[]{"SELECT(selectList:invertedIndexCol1, noIndexCol1)", 3, 2});
    result4.add(new Object[]{"PROJECT(invertedIndexCol1, noIndexCol1)", 4, 3});
    result4.add(new Object[]{"DOC_ID_SET", 5, 4});
    result4.add(new Object[]{"FILTER_OR", 6, 5});
    result4.add(new Object[]{"FILTER_FULL_SCAN(operator:RANGE,predicate:noIndexCol1 > '1')", 7, 6});
    result4.add(new Object[]{"FILTER_EXPRESSION(operator:EQ,predicate:contains(textIndexCol1,'daff') = 'true')", 8, 6});
    result4.add(new Object[]{"FILTER_FULL_SCAN(operator:RANGE,predicate:noIndexCol2 BETWEEN '2' AND '101')", 9, 6});
    check(query4, new ResultTable(DATA_SCHEMA, result4));

    // All segments match since noIndexCol4 has at least one row value set to 'true' across all segments
    String query5 = "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT invertedIndexCol1, noIndexCol1 FROM "
        + "testTable WHERE noIndexCol4 LIMIT 100 ";
    List<Object[]> result5 = new ArrayList<>();
    result5.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result5.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result5.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result5.add(new Object[]{"SELECT(selectList:invertedIndexCol1, noIndexCol1)", 3, 2});
    result5.add(new Object[]{"PROJECT(invertedIndexCol1, noIndexCol1)", 4, 3});
    result5.add(new Object[]{"DOC_ID_SET", 5, 4});
    result5.add(new Object[]{"FILTER_FULL_SCAN(operator:EQ,predicate:noIndexCol4 = 'true')", 6, 5});
    check(query5, new ResultTable(DATA_SCHEMA, result5));

    // The FILTER_AND is part of the query plan for all segments as the first predicate is an expression and the second
    // has at least one matching row in each segment
    String query6 = "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT invertedIndexCol1, noIndexCol1 FROM "
        + "testTable WHERE startsWith (textIndexCol1, 'daff') AND noIndexCol4";
    List<Object[]> result6 = new ArrayList<>();
    result6.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result6.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result6.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result6.add(new Object[]{"SELECT(selectList:invertedIndexCol1, noIndexCol1)", 3, 2});
    result6.add(new Object[]{"PROJECT(invertedIndexCol1, noIndexCol1)", 4, 3});
    result6.add(new Object[]{"DOC_ID_SET", 5, 4});
    result6.add(new Object[]{"FILTER_AND", 6, 5});
    result6.add(new Object[]{"FILTER_FULL_SCAN(operator:EQ,predicate:noIndexCol4 = 'true')", 7, 6});
    result6.add(new Object[]{
        "FILTER_EXPRESSION(operator:EQ,predicate:startswith(textIndexCol1,'daff') = 'true')", 8, 6
    });
    check(query6, new ResultTable(DATA_SCHEMA, result6));
  }

  /** Test case for SQL statements with filter that involves inverted or sorted index access. */
  @Test
  public void testSelectColumnsUsingFilterOnInvertedIndexColumn() {
    // Segments 1, 2, 4 result in both the AND predicates getting evaluated as all three have some rows that match.
    // Segment 3 results in an EmptyFilterOperator as the invertedIndexCol1 doesn't have the value 1.1 in it
    // and this is an AND predicate, but the values are within range
    // Since verbose mode is disabled, the non-EmptyFilterOperator plan is returned.
    String query1 = "EXPLAIN PLAN FOR SELECT noIndexCol1, invertedIndexCol1, sortedIndexCol1 FROM testTable WHERE "
        + "invertedIndexCol1 = 1.1 AND sortedIndexCol1 = 100.1 LIMIT 100";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result1.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result1.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:3)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result1.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1, sortedIndexCol1)", 3, 2});
    result1.add(new Object[]{"PROJECT(noIndexCol1, invertedIndexCol1, sortedIndexCol1)", 4, 3});
    result1.add(new Object[]{"DOC_ID_SET", 5, 4});
    result1.add(new Object[]{"FILTER_AND", 6, 5});
    result1.add(new Object[]{
        "FILTER_SORTED_INDEX(indexLookUp:sorted_index,operator:EQ,predicate:sortedIndexCol1 = '100.1')", 7, 6
    });
    result1.add(new Object[]{
        "FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:EQ,predicate:invertedIndexCol1 = '1.1')", 8, 6
    });
    check(query1, new ResultTable(DATA_SCHEMA, result1));

    // Segments 1, 2, 4 result in a FILTER_OR plan which matches all the segments and all four predicates.
    // Segment 3 removes the OR clause for 'invertedIndexCol1 = 1.1' since that segment doesn't have this value for any
    // of its rows.
    // The deepest plan is returned which matches the former which matches three segments and has four OR predicates
    String query2 = "EXPLAIN PLAN FOR SELECT noIndexCol1, invertedIndexCol1, sortedIndexCol1  FROM testTable WHERE "
        + "(invertedIndexCol1 = 1.1 OR sortedIndexCol1 = 100.2) OR (invertedIndexCol1 BETWEEN 0.2 AND 5 OR "
        + "rangeIndexCol1 > 20)";
    List<Object[]> result2 = new ArrayList<>();
    result2.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result2.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result2.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:3)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result2.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1, sortedIndexCol1)", 3, 2});
    result2.add(new Object[]{"PROJECT(noIndexCol1, invertedIndexCol1, sortedIndexCol1)", 4, 3});
    result2.add(new Object[]{"DOC_ID_SET", 5, 4});
    result2.add(new Object[]{"FILTER_OR", 6, 5});
    result2.add(new Object[]{
        "FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:EQ,predicate:invertedIndexCol1 = '1.1')", 7, 6
    });
    result2.add(new Object[]{
        "FILTER_SORTED_INDEX(indexLookUp:sorted_index,operator:EQ,predicate:sortedIndexCol1 = '100.2')", 8, 6
    });
    result2.add(
        new Object[]{"FILTER_FULL_SCAN(operator:RANGE,predicate:invertedIndexCol1 BETWEEN '0.2' AND '5')", 9, 6});
    result2.add(new Object[]{
        "FILTER_RANGE_INDEX(indexLookUp:range_index,operator:RANGE,predicate:rangeIndexCol1 > '20')", 10, 6
    });
    check(query2, new ResultTable(DATA_SCHEMA, result2));

    // Segments 2, 3, 4 result in a MatchAllOperator because 'invertedIndexCol3 NOT IN ('foo', 'mickey')' matches all
    // the rows in these segments.
    // Segment 1 does contain 'mickey' and two predicates of the OR are part of the query plan.
    // Since verbose mode is disabled The FILTER_OR plan is returned as it's the deepest.
    String query3 =
        "EXPLAIN PLAN FOR SELECT noIndexCol1, invertedIndexCol1 FROM testTable WHERE invertedIndexCol1 = 1.5 OR "
            + "invertedIndexCol2 IN (1, 2, 30) OR invertedIndexCol3 NOT IN ('foo', 'mickey') LIMIT 100";
    List<Object[]> result3 = new ArrayList<>();
    result3.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result3.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result3.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result3.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1)", 3, 2});
    result3.add(new Object[]{INVERTED_INDEX_COL_1, 4, 3});
    result3.add(new Object[]{"DOC_ID_SET", 5, 4});
    result3.add(new Object[]{"FILTER_OR", 6, 5});
    result3.add(new Object[]{
        "FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:IN,predicate:invertedIndexCol2 IN ('1','2','30'))",
        7, 6
    });
    result3.add(new Object[]{
        "FILTER_SORTED_INDEX(indexLookUp:sorted_index,operator:NOT_IN,predicate:invertedIndexCol3 NOT IN "
            + "('foo','mickey'))", 8, 6
    });
    check(query3, new ResultTable(DATA_SCHEMA, result3));
  }

  /** Test case for SQL statements with filter that involves inverted or sorted index access. */
  @Test
  public void testSelectColumnsUsingFilterOnInvertedIndexColumnVerbose() {
    // Segments 1, 2, 4 result in both the AND predicates getting evaluated as all three have some rows that match.
    // Segment 3 results in an EmptyFilterOperator as the invertedIndexCol1 doesn't have the value 1.1 in it
    // and this is an AND predicate, but the values are within range
    String query1 = "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT noIndexCol1, invertedIndexCol1, "
        + "sortedIndexCol1 FROM testTable WHERE invertedIndexCol1 = 1.1 AND sortedIndexCol1 = 100.1 LIMIT 100";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result1.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result1.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:3)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result1.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1, sortedIndexCol1)", 3, 2});
    result1.add(new Object[]{"PROJECT(noIndexCol1, invertedIndexCol1, sortedIndexCol1)", 4, 3});
    result1.add(new Object[]{"DOC_ID_SET", 5, 4});
    result1.add(new Object[]{"FILTER_AND", 6, 5});
    result1.add(new Object[]{
        "FILTER_SORTED_INDEX(indexLookUp:sorted_index,operator:EQ,predicate:sortedIndexCol1 = '100.1')", 7, 6
    });
    result1.add(new Object[]{
        "FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:EQ,predicate:invertedIndexCol1 = '1.1')", 8, 6
    });
    result1.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result1.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1, sortedIndexCol1)", 3, 2});
    result1.add(new Object[]{"PROJECT(noIndexCol1, invertedIndexCol1, sortedIndexCol1)", 4, 3});
    result1.add(new Object[]{"DOC_ID_SET", 5, 4});
    result1.add(new Object[]{"FILTER_EMPTY", 6, 5});
    check(query1, new ResultTable(DATA_SCHEMA, result1));

    // Segments 1, 2, 4 result in a FILTER_OR plan which matches all the segments and all four predicates.
    // Segment 3 removes the OR clause for 'invertedIndexCol1 = 1.1' since that segment doesn't have this value for any
    // of its rows.
    String query2 = "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT noIndexCol1, invertedIndexCol1, "
        + "sortedIndexCol1  FROM testTable WHERE (invertedIndexCol1 = 1.1 OR sortedIndexCol1 = 100.2) OR "
        + "(invertedIndexCol1 BETWEEN 0.2 AND 5 OR rangeIndexCol1 > 20)";
    List<Object[]> result2 = new ArrayList<>();
    result2.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result2.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result2.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result2.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1, sortedIndexCol1)", 3, 2});
    result2.add(new Object[]{"PROJECT(noIndexCol1, invertedIndexCol1, sortedIndexCol1)", 4, 3});
    result2.add(new Object[]{"DOC_ID_SET", 5, 4});
    result2.add(new Object[]{"FILTER_OR", 6, 5});
    result2.add(new Object[]{
        "FILTER_SORTED_INDEX(indexLookUp:sorted_index,operator:EQ,predicate:sortedIndexCol1 = '100.2')", 7, 6
    });
    result2.add(new Object[]{
        "FILTER_FULL_SCAN(operator:RANGE,predicate:invertedIndexCol1 BETWEEN '0.2' AND '5')", 8, 6
    });
    result2.add(new Object[]{
        "FILTER_RANGE_INDEX(indexLookUp:range_index,operator:RANGE,predicate:rangeIndexCol1 > '20')", 9, 6
    });
    result2.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:3)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result2.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1, sortedIndexCol1)", 3, 2});
    result2.add(new Object[]{"PROJECT(noIndexCol1, invertedIndexCol1, sortedIndexCol1)", 4, 3});
    result2.add(new Object[]{"DOC_ID_SET", 5, 4});
    result2.add(new Object[]{"FILTER_OR", 6, 5});
    result2.add(new Object[]{
        "FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:EQ,predicate:invertedIndexCol1 = '1.1')", 7, 6
    });
    result2.add(new Object[]{
        "FILTER_SORTED_INDEX(indexLookUp:sorted_index,operator:EQ,predicate:sortedIndexCol1 = '100.2')", 8, 6
    });
    result2.add(
        new Object[]{"FILTER_FULL_SCAN(operator:RANGE,predicate:invertedIndexCol1 BETWEEN '0.2' AND '5')", 9, 6});
    result2.add(new Object[]{
        "FILTER_RANGE_INDEX(indexLookUp:range_index,operator:RANGE,predicate:rangeIndexCol1 > '20')", 10, 6
    });
    check(query2, new ResultTable(DATA_SCHEMA, result2));

    // Segments 2, 3, 4 result in a MatchAllOperator because 'invertedIndexCol3 NOT IN ('foo', 'mickey')' matches all
    // the rows in these segments.
    // Segment 1 does contain 'mickey' and two predicates of the OR are part of the query plan.
    String query3 =
        "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT noIndexCol1, invertedIndexCol1 FROM testTable WHERE "
            + "invertedIndexCol1 = 1.5 OR invertedIndexCol2 IN (1, 2, 30) OR invertedIndexCol3 NOT IN "
            + "('foo', 'mickey') LIMIT 100";
    List<Object[]> result3 = new ArrayList<>();
    result3.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result3.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result3.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result3.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1)", 3, 2});
    result3.add(new Object[]{INVERTED_INDEX_COL_1, 4, 3});
    result3.add(new Object[]{"DOC_ID_SET", 5, 4});
    result3.add(new Object[]{"FILTER_OR", 6, 5});
    result3.add(new Object[]{
        "FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:IN,predicate:invertedIndexCol2 IN ('1','2','30'))",
        7, 6
    });
    result3.add(new Object[]{
        "FILTER_SORTED_INDEX(indexLookUp:sorted_index,operator:NOT_IN,predicate:invertedIndexCol3 NOT IN "
            + "('foo','mickey'))", 8, 6
    });
    result3.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:3)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result3.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1)", 3, 2});
    result3.add(new Object[]{INVERTED_INDEX_COL_1, 4, 3});
    result3.add(new Object[]{"DOC_ID_SET", 5, 4});
    result3.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 6, 5});
    check(query3, new ResultTable(DATA_SCHEMA, result3));
  }

  /** Test case for SQL statements with filter that involves range index access. */
  @Test
  public void testSelectColumnUsingFilterOnRangeIndexColumn() {
    // select * query triggering range index
    // checks using RANGE (>, >=, <, <=, BETWEEN ..) on a column with range index should use RANGE_INDEX_SCAN
    String query1 =
        "EXPLAIN PLAN FOR SELECT invertedIndexCol1, noIndexCol1, rangeIndexCol1 FROM testTable WHERE rangeIndexCol1 >"
            + " 10.1 AND rangeIndexCol2 >= 15 OR rangeIndexCol3 BETWEEN 21 AND 45 LIMIT 100";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result1.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result1.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result1.add(new Object[]{"SELECT(selectList:invertedIndexCol1, noIndexCol1, rangeIndexCol1)", 3, 2});
    result1.add(new Object[]{"PROJECT(invertedIndexCol1, noIndexCol1, rangeIndexCol1)", 4, 3});
    result1.add(new Object[]{"DOC_ID_SET", 5, 4});
    result1.add(new Object[]{"FILTER_OR", 6, 5});
    result1.add(new Object[]{"FILTER_AND", 7, 6});
    result1.add(new Object[]{
        "FILTER_RANGE_INDEX(indexLookUp:range_index,operator:RANGE,predicate:rangeIndexCol1 > '10.1')", 8, 7
    });
    result1.add(new Object[]{
        "FILTER_RANGE_INDEX(indexLookUp:range_index,operator:RANGE,predicate:rangeIndexCol2 >= '15')", 9, 7
    });
    result1.add(new Object[]{
        "FILTER_RANGE_INDEX(indexLookUp:range_index,operator:RANGE,predicate:rangeIndexCol3 "
            + "BETWEEN '21' AND '45')", 10, 6
    });
    check(query1, new ResultTable(DATA_SCHEMA, result1));
  }

  /** Test case for SQL statements with filter that involves range index access. */
  @Test
  public void testSelectColumnUsingFilterOnRangeIndexColumnVerbose() {
    // select * query triggering range index
    // checks using RANGE (>, >=, <, <=, BETWEEN ..) on a column with range index should use RANGE_INDEX_SCAN
    String query1 =
        "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT invertedIndexCol1, noIndexCol1, rangeIndexCol1 FROM "
            + "testTable WHERE rangeIndexCol1 > 10.1 AND rangeIndexCol2 >= 15 OR rangeIndexCol3 BETWEEN 21 AND 45 "
            + "LIMIT 100";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result1.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result1.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result1.add(new Object[]{"SELECT(selectList:invertedIndexCol1, noIndexCol1, rangeIndexCol1)", 3, 2});
    result1.add(new Object[]{"PROJECT(invertedIndexCol1, noIndexCol1, rangeIndexCol1)", 4, 3});
    result1.add(new Object[]{"DOC_ID_SET", 5, 4});
    result1.add(new Object[]{"FILTER_OR", 6, 5});
    result1.add(new Object[]{"FILTER_AND", 7, 6});
    result1.add(new Object[]{
        "FILTER_RANGE_INDEX(indexLookUp:range_index,operator:RANGE,predicate:rangeIndexCol1 > '10.1')", 8, 7
    });
    result1.add(new Object[]{
        "FILTER_RANGE_INDEX(indexLookUp:range_index,operator:RANGE,predicate:rangeIndexCol2 >= '15')", 9, 7
    });
    result1.add(new Object[]{
        "FILTER_RANGE_INDEX(indexLookUp:range_index,operator:RANGE,predicate:rangeIndexCol3 "
            + "BETWEEN '21' AND '45')", 10, 6
    });
    check(query1, new ResultTable(DATA_SCHEMA, result1));
  }

  @Test
  public void testSelectAggregateUsingFilterOnTextIndexColumn() {
    // All segments match the same plan for these queries
    String query1 =
        "EXPLAIN PLAN FOR SELECT noIndexCol1, noIndexCol2, max(noIndexCol2), min(noIndexCol3) FROM testTable WHERE "
            + "TEXT_MATCH(textIndexCol1, 'foo') GROUP BY noIndexCol1, noIndexCol2";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result1.add(new Object[]{"COMBINE_GROUP_BY", 2, 1});
    result1.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result1.add(new Object[]{
        "GROUP_BY(groupKeys:noIndexCol1, noIndexCol2, aggregations:max(noIndexCol2), min(noIndexCol3))", 3, 2
    });
    result1.add(new Object[]{"PROJECT(noIndexCol2, noIndexCol3, noIndexCol1)", 4, 3});
    result1.add(new Object[]{"DOC_ID_SET", 5, 4});
    result1.add(new Object[]{
        "FILTER_TEXT_INDEX(indexLookUp:text_index,operator:TEXT_MATCH,predicate:text_match(textIndexCol1,'foo'))", 6, 5
    });
    check(query1, new ResultTable(DATA_SCHEMA, result1));

    String query2 =
        "EXPLAIN PLAN FOR SELECT noIndexCol1, max(noIndexCol2) AS mymax, min(noIndexCol3) AS mymin FROM testTable "
            + "WHERE TEXT_MATCH (textIndexCol1, 'foo') GROUP BY noIndexCol1, noIndexCol2 ORDER BY noIndexCol1, max"
            + "(noIndexCol2)";
    List<Object[]> result2 = new ArrayList<>();
    result2.add(new Object[]{"BROKER_REDUCE(sort:[noIndexCol1 ASC, max(noIndexCol2) ASC],limit:10)", 1, 0});
    result2.add(new Object[]{"COMBINE_GROUP_BY", 2, 1});
    result2.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result2.add(new Object[]{
        "GROUP_BY(groupKeys:noIndexCol1, noIndexCol2, aggregations:max(noIndexCol2), min(noIndexCol3))", 3, 2
    });
    result2.add(new Object[]{"PROJECT(noIndexCol2, noIndexCol3, noIndexCol1)", 4, 3});
    result2.add(new Object[]{"DOC_ID_SET", 5, 4});
    result2.add(new Object[]{
        "FILTER_TEXT_INDEX(indexLookUp:text_index,operator:TEXT_MATCH,predicate:text_match(textIndexCol1,'foo'))", 6, 5
    });
    check(query2, new ResultTable(DATA_SCHEMA, result2));
  }

  @Test
  public void testSelectAggregateUsingFilterOnTextIndexColumnVerbose() {
    // All segments match the same plan for these queries
    String query1 = "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT noIndexCol1, noIndexCol2, max(noIndexCol2), "
        + "min(noIndexCol3) FROM testTable WHERE TEXT_MATCH(textIndexCol1, 'foo') GROUP BY noIndexCol1, "
        + "noIndexCol2";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result1.add(new Object[]{"COMBINE_GROUP_BY", 2, 1});
    result1.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result1.add(new Object[]{
        "GROUP_BY(groupKeys:noIndexCol1, noIndexCol2, aggregations:max(noIndexCol2), min(noIndexCol3))", 3, 2
    });
    result1.add(new Object[]{"PROJECT(noIndexCol2, noIndexCol3, noIndexCol1)", 4, 3});
    result1.add(new Object[]{"DOC_ID_SET", 5, 4});
    result1.add(new Object[]{
        "FILTER_TEXT_INDEX(indexLookUp:text_index,operator:TEXT_MATCH,predicate:text_match(textIndexCol1,'foo'))", 6, 5
    });
    check(query1, new ResultTable(DATA_SCHEMA, result1));

    String query2 = "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT noIndexCol1, max(noIndexCol2) AS mymax, "
        + "min(noIndexCol3) AS mymin FROM testTable WHERE TEXT_MATCH (textIndexCol1, 'foo') GROUP BY "
        + "noIndexCol1, noIndexCol2 ORDER BY noIndexCol1, max(noIndexCol2)";
    List<Object[]> result2 = new ArrayList<>();
    result2.add(new Object[]{"BROKER_REDUCE(sort:[noIndexCol1 ASC, max(noIndexCol2) ASC],limit:10)", 1, 0});
    result2.add(new Object[]{"COMBINE_GROUP_BY", 2, 1});
    result2.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result2.add(new Object[]{
        "GROUP_BY(groupKeys:noIndexCol1, noIndexCol2, aggregations:max(noIndexCol2), min(noIndexCol3))", 3, 2
    });
    result2.add(new Object[]{"PROJECT(noIndexCol2, noIndexCol3, noIndexCol1)", 4, 3});
    result2.add(new Object[]{"DOC_ID_SET", 5, 4});
    result2.add(new Object[]{
        "FILTER_TEXT_INDEX(indexLookUp:text_index,operator:TEXT_MATCH,predicate:text_match(textIndexCol1,'foo'))", 6, 5
    });
    check(query2, new ResultTable(DATA_SCHEMA, result2));
  }

  @Test
  public void testSelectColumnUsingFilterOnJsonIndexColumn() {
    // Segment 2, 3, 4 don't match 'noIndexCol1 NOT IN (1, 20, 30)' so they return a plan without this OR predicate
    // Segments 1 matches all three predicates so returns a plan with all three
    // The plan with the deepest tree is returned which matches segment 1 as verbose mode is disabled
    String query =
        "EXPLAIN PLAN FOR SELECT noIndexCol1, invertedIndexCol1 FROM testTable WHERE (invertedIndexCol1 IN (10, 20, "
            + "30) AND sortedIndexCol1 != 100) OR (noIndexCol1 NOT IN (1, 20, 30) AND rangeIndexCol1 != 20 AND "
            + "JSON_MATCH(jsonIndexCol1, 'key=1') AND TEXT_MATCH(textIndexCol1, 'foo'))";
    List<Object[]> result = new ArrayList<>();
    result.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1)", 3, 2});
    result.add(new Object[]{INVERTED_INDEX_COL_1, 4, 3});
    result.add(new Object[]{"DOC_ID_SET", 5, 4});
    result.add(new Object[]{"FILTER_AND", 6, 5});
    result.add(new Object[]{
        "FILTER_JSON_INDEX(indexLookUp:json_index,operator:JSON_MATCH,predicate:json_match(jsonIndexCol1,'key=1'))",
        7, 6
    });
    result.add(new Object[]{
        "FILTER_TEXT_INDEX(indexLookUp:text_index,operator:TEXT_MATCH,predicate:text_match(textIndexCol1,'foo'))", 8, 6
    });
    result.add(new Object[]{"FILTER_FULL_SCAN(operator:NOT_IN,predicate:noIndexCol1 NOT IN ('1','20','30'))", 9, 6});
    check(query, new ResultTable(DATA_SCHEMA, result));
  }

  @Test
  public void testSelectColumnUsingFilterOnJsonIndexColumnVerbose() {
    // Segment 2, 3, 4 don't match 'noIndexCol1 NOT IN (1, 20, 30)' so they return a plan without this OR predicate
    // Segments 1 matches all three predicates so returns a plan with all three
    String query = "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT noIndexCol1, invertedIndexCol1 FROM testTable "
        + "WHERE (invertedIndexCol1 IN (10, 20, 30) AND sortedIndexCol1 != 100) OR (noIndexCol1 NOT IN (1, 20, "
        + "30) AND rangeIndexCol1 != 20 AND JSON_MATCH(jsonIndexCol1, 'key=1') AND TEXT_MATCH(textIndexCol1, "
        + "'foo'))";
    List<Object[]> result = new ArrayList<>();
    result.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1)", 3, 2});
    result.add(new Object[]{INVERTED_INDEX_COL_1, 4, 3});
    result.add(new Object[]{"DOC_ID_SET", 5, 4});
    result.add(new Object[]{"FILTER_AND", 6, 5});
    result.add(new Object[]{
        "FILTER_JSON_INDEX(indexLookUp:json_index,operator:JSON_MATCH,predicate:json_match(jsonIndexCol1,'key=1'))",
        7, 6
    });
    result.add(new Object[]{
        "FILTER_TEXT_INDEX(indexLookUp:text_index,operator:TEXT_MATCH,predicate:text_match(textIndexCol1,'foo'))", 8, 6
    });
    result.add(new Object[]{"FILTER_FULL_SCAN(operator:NOT_IN,predicate:noIndexCol1 NOT IN ('1','20','30'))", 9, 6});
    result.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:3)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1)", 3, 2});
    result.add(new Object[]{INVERTED_INDEX_COL_1, 4, 3});
    result.add(new Object[]{"DOC_ID_SET", 5, 4});
    result.add(new Object[]{"FILTER_AND", 6, 5});
    result.add(new Object[]{
        "FILTER_JSON_INDEX(indexLookUp:json_index,operator:JSON_MATCH,predicate:json_match(jsonIndexCol1,'key=1'))",
        7, 6
    });
    result.add(new Object[]{
        "FILTER_TEXT_INDEX(indexLookUp:text_index,operator:TEXT_MATCH,predicate:text_match(textIndexCol1,'foo'))", 8, 6
    });
    check(query, new ResultTable(DATA_SCHEMA, result));
  }

  @Test
  public void testSelectColumnsVariationsOfOrOperators() {
    // Segment 2 returns match all as 'pluto' matches all rows even though '1.5' isn't present
    // Segment 3 matches a row for '1.5' even though 'pluto' doesn't exist
    // Segment 1, 4 don't contain either '1.5' or 'pluto' but are within range so they return an EmptyFilter
    // The plan for segment 3 is returned as it has precedence and verbose mode is disabled
    String query1 = "EXPLAIN PLAN FOR SELECT noIndexCol1, invertedIndexCol1, invertedIndexCol3 FROM testTable WHERE "
        + "invertedIndexCol1 = 1.5 OR invertedIndexCol3 = 'pluto' LIMIT 100";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result1.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result1.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result1.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1, invertedIndexCol3)", 3, 2});
    result1.add(new Object[]{PROJEC_TNII_3, 4, 3});
    result1.add(new Object[]{"DOC_ID_SET", 5, 4});
    result1.add(new Object[]{
        "FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:EQ,predicate:invertedIndexCol1 = '1.5')", 6, 5
    });
    check(query1, new ResultTable(DATA_SCHEMA, result1));

    // Segment 1 matches both OR predicates so returns a FILTER_OR tree
    // Segment 3 doesn't contain 'mickey' or '1.1' and returns an EmptyFilter as '1.1' is within range
    // Segment 2, 4 do contain 1.1 but don't contain 'mickey' so part of the OR predicate is removed
    // The deepest tree with FILTER_OR is returned as it has precedence and verbose mode is disabled
    String query2 = "EXPLAIN PLAN FOR SELECT noIndexCol1, invertedIndexCol1, invertedIndexCol3 FROM testTable WHERE "
        + "invertedIndexCol1 = 1.1 OR invertedIndexCol3 = 'mickey' LIMIT 100";
    List<Object[]> result2 = new ArrayList<>();
    result2.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result2.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result2.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result2.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1, invertedIndexCol3)", 3, 2});
    result2.add(new Object[]{PROJEC_TNII_3, 4, 3});
    result2.add(new Object[]{"DOC_ID_SET", 5, 4});
    result2.add(new Object[]{"FILTER_OR", 6, 5});
    result2.add(new Object[]{
        "FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:EQ,predicate:invertedIndexCol1 = '1.1')", 7, 6
    });
    result2.add(new Object[]{
        "FILTER_SORTED_INDEX(indexLookUp:sorted_index,operator:EQ,predicate:invertedIndexCol3 = 'mickey')", 8, 6
    });
    check(query2, new ResultTable(DATA_SCHEMA, result2));

    // An OR query that matches all predicates on all segments
    String query3 = "EXPLAIN PLAN FOR SELECT noIndexCol1, invertedIndexCol1, invertedIndexCol2 FROM testTable WHERE "
        + "invertedIndexCol1 = 0.1 OR invertedIndexCol2 = 2 LIMIT 100";
    List<Object[]> result3 = new ArrayList<>();
    result3.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result3.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result3.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result3.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1, invertedIndexCol2)", 3, 2});
    result3.add(new Object[]{"PROJECT(noIndexCol1, invertedIndexCol1, invertedIndexCol2)", 4, 3});
    result3.add(new Object[]{"DOC_ID_SET", 5, 4});
    result3.add(new Object[]{"FILTER_OR", 6, 5});
    result3.add(new Object[]{
        "FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:EQ,predicate:invertedIndexCol1 = '0.1')", 7, 6
    });
    result3.add(new Object[]{
        "FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:EQ,predicate:invertedIndexCol2 = '2')", 8, 6
    });
    check(query3, new ResultTable(DATA_SCHEMA, result3));

    // Segment 2 matches all on the 'pluto' predicate
    // Segments 1, 3 get pruned as '8' and 'pluto' are out of range
    // Segment 4 returns EmptyFilterOperator as though '8' is out of range, 'pluto' is within range but not present
    // The MatchAll plan is returned as it has precedence and verbose mode is disabled
    String query4 = "EXPLAIN PLAN FOR SELECT noIndexCol1, invertedIndexCol3 FROM testTable WHERE "
        + "invertedIndexCol3 = 'pluto' OR noIndexCol1 = 8 LIMIT 100";
    List<Object[]> result4 = new ArrayList<>();
    result4.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result4.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result4.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result4.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol3)", 3, 2});
    result4.add(new Object[]{"PROJECT(noIndexCol1, invertedIndexCol3)", 4, 3});
    result4.add(new Object[]{"DOC_ID_SET", 5, 4});
    result4.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 6, 5});
    check(query4, new ResultTable(DATA_SCHEMA, result4));
  }

  @Test
  public void testSelectColumnsVariationsOfOrOperatorsVerbose() {
    // Segment 2 returns match all as 'pluto' matches all rows even though '1.5' isn't present
    // Segment 3 matches a row for '1.5' even though 'pluto' doesn't exist
    // Segment 1, 4 don't contain either '1.5' or 'pluto' but are within range so they return an EmptyFilter
    String query1 = "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT noIndexCol1, invertedIndexCol1, "
        + "invertedIndexCol3 FROM testTable WHERE invertedIndexCol1 = 1.5 OR invertedIndexCol3 = 'pluto' LIMIT 100";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result1.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result1.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result1.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1, invertedIndexCol3)", 3, 2});
    result1.add(new Object[]{PROJEC_TNII_3, 4, 3});
    result1.add(new Object[]{"DOC_ID_SET", 5, 4});
    result1.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 6, 5});
    result1.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:2)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result1.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1, invertedIndexCol3)", 3, 2});
    result1.add(new Object[]{PROJEC_TNII_3, 4, 3});
    result1.add(new Object[]{"DOC_ID_SET", 5, 4});
    result1.add(new Object[]{"FILTER_EMPTY", 6, 5});
    result1.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result1.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1, invertedIndexCol3)", 3, 2});
    result1.add(new Object[]{PROJEC_TNII_3, 4, 3});
    result1.add(new Object[]{"DOC_ID_SET", 5, 4});
    result1.add(new Object[]{
        "FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:EQ,predicate:invertedIndexCol1 = '1.5')", 6, 5
    });

    check(query1, new ResultTable(DATA_SCHEMA, result1));

    // Segment 1 matches both OR predicates so returns a FILTER_OR tree
    // Segment 3 doesn't contain 'mickey' or '1.1' and returns an EmptyFilter as '1.1' is within range
    // Segment 2, 4 do contain 1.1 but don't contain 'mickey' so part of the OR predicate is removed
    String query2 = "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT noIndexCol1, invertedIndexCol1, "
        + "invertedIndexCol3 FROM testTable WHERE invertedIndexCol1 = 1.1 OR invertedIndexCol3 = 'mickey' LIMIT 100";
    List<Object[]> result2 = new ArrayList<>();
    result2.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result2.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result2.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result2.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1, invertedIndexCol3)", 3, 2});
    result2.add(new Object[]{PROJEC_TNII_3, 4, 3});
    result2.add(new Object[]{"DOC_ID_SET", 5, 4});
    result2.add(new Object[]{"FILTER_OR", 6, 5});
    result2.add(new Object[]{
        "FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:EQ,predicate:invertedIndexCol1 = '1.1')", 7, 6
    });
    result2.add(new Object[]{
        "FILTER_SORTED_INDEX(indexLookUp:sorted_index,operator:EQ,predicate:invertedIndexCol3 = 'mickey')", 8, 6
    });
    result2.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:2)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result2.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1, invertedIndexCol3)", 3, 2});
    result2.add(new Object[]{PROJEC_TNII_3, 4, 3});
    result2.add(new Object[]{"DOC_ID_SET", 5, 4});
    result2.add(new Object[]{
        "FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:EQ,predicate:invertedIndexCol1 = '1.1')", 6, 5
    });
    result2.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result2.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1, invertedIndexCol3)", 3, 2});
    result2.add(new Object[]{PROJEC_TNII_3, 4, 3});
    result2.add(new Object[]{"DOC_ID_SET", 5, 4});
    result2.add(new Object[]{"FILTER_EMPTY", 6, 5});
    check(query2, new ResultTable(DATA_SCHEMA, result2));

    // An OR query that matches all predicates on all segments
    String query3 = "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT noIndexCol1, invertedIndexCol1, "
        + "invertedIndexCol2 FROM testTable WHERE invertedIndexCol1 = 0.1 OR invertedIndexCol2 = 2 LIMIT 100";
    List<Object[]> result3 = new ArrayList<>();
    result3.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result3.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result3.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result3.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1, invertedIndexCol2)", 3, 2});
    result3.add(new Object[]{"PROJECT(noIndexCol1, invertedIndexCol1, invertedIndexCol2)", 4, 3});
    result3.add(new Object[]{"DOC_ID_SET", 5, 4});
    result3.add(new Object[]{"FILTER_OR", 6, 5});
    result3.add(new Object[]{
        "FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:EQ,predicate:invertedIndexCol1 = '0.1')", 7, 6
    });
    result3.add(new Object[]{
        "FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:EQ,predicate:invertedIndexCol2 = '2')", 8, 6
    });
    check(query3, new ResultTable(DATA_SCHEMA, result3));

    // Segment 2 matches all on the 'pluto' predicate
    // Segments 1, 3 get pruned as '8' and 'pluto' are out of range
    // Segment 4 returns EmptyFilterOperator as though '8' is out of range, 'pluto' is within range but not present
    String query4 = "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT noIndexCol1, invertedIndexCol3 FROM "
        + "testTable WHERE invertedIndexCol3 = 'pluto' OR noIndexCol1 = 8 LIMIT 100";
    List<Object[]> result4 = new ArrayList<>();
    result4.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result4.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result4.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result4.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol3)", 3, 2});
    result4.add(new Object[]{"PROJECT(noIndexCol1, invertedIndexCol3)", 4, 3});
    result4.add(new Object[]{"DOC_ID_SET", 5, 4});
    result4.add(new Object[]{"FILTER_EMPTY", 6, 5});
    result4.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result4.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol3)", 3, 2});
    result4.add(new Object[]{"PROJECT(noIndexCol1, invertedIndexCol3)", 4, 3});
    result4.add(new Object[]{"DOC_ID_SET", 5, 4});
    result4.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 6, 5});
    check(query4, new ResultTable(DATA_SCHEMA, result4));
  }

  @Test
  public void testSelectColumnsVariationsOfAndOperators() {
    // Segments 1 and 4 are pruned as 'pluto' isn't in range and nor is '1.5'
    // Segment 2 has 'pluto' but doesn't have '1.5' so it returns EmptyFilterOperator
    // Segment 3 has '1.5' but doesn't have pluto so it also returns EmptyFilterOperator
    // Return the EmptyFilterOperator plan
    String query1 = "EXPLAIN PLAN FOR SELECT noIndexCol1, invertedIndexCol1, invertedIndexCol3 FROM testTable WHERE "
        + "invertedIndexCol1 = 1.5 AND invertedIndexCol3 = 'pluto' LIMIT 100";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result1.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result1.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:2)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result1.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1, invertedIndexCol3)", 3, 2});
    result1.add(new Object[]{PROJEC_TNII_3, 4, 3});
    result1.add(new Object[]{"DOC_ID_SET", 5, 4});
    result1.add(new Object[]{"FILTER_EMPTY", 6, 5});
    check(query1, new ResultTable(DATA_SCHEMA, result1));

    // Segment 1 has a match for both predicates so it return a FILTER_AND plan
    // Segment 2 is pruned as 'mickey' isn't in range (all values are 'pluto')
    // Segment 3, 4 return an EmptyFilterOperator plan as they contain '1.1' but don't contain 'mickey'
    // Return the FILTER_AND plan as it has precedence and verbose mode is disabled
    String query2 = "EXPLAIN PLAN FOR SELECT noIndexCol1, invertedIndexCol1, invertedIndexCol3 FROM testTable WHERE "
        + "invertedIndexCol1 = 1.1 AND invertedIndexCol3 = 'mickey' LIMIT 100";
    List<Object[]> result2 = new ArrayList<>();
    result2.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result2.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result2.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result2.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1, invertedIndexCol3)", 3, 2});
    result2.add(new Object[]{PROJEC_TNII_3, 4, 3});
    result2.add(new Object[]{"DOC_ID_SET", 5, 4});
    result2.add(new Object[]{"FILTER_AND", 6, 5});
    result2.add(new Object[]{
        "FILTER_SORTED_INDEX(indexLookUp:sorted_index,operator:EQ,predicate:invertedIndexCol3 = 'mickey')", 7, 6
    });
    result2.add(new Object[]{
        "FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:EQ,predicate:invertedIndexCol1 = '1.1')", 8, 6
    });
    check(query2, new ResultTable(DATA_SCHEMA, result2));

    // An AND query that matches all predicates on all segments
    String query3 = "EXPLAIN PLAN FOR SELECT noIndexCol1, invertedIndexCol1, invertedIndexCol2 FROM testTable WHERE "
        + "invertedIndexCol1 = 0.1 AND invertedIndexCol2 = 1 LIMIT 100";
    List<Object[]> result3 = new ArrayList<>();
    result3.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result3.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result3.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result3.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1, invertedIndexCol2)", 3, 2});
    result3.add(new Object[]{"PROJECT(noIndexCol1, invertedIndexCol1, invertedIndexCol2)", 4, 3});
    result3.add(new Object[]{"DOC_ID_SET", 5, 4});
    result3.add(new Object[]{"FILTER_AND", 6, 5});
    result3.add(new Object[]{
        "FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:EQ,predicate:invertedIndexCol1 = '0.1')", 7, 6
    });
    result3.add(new Object[]{
        "FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:EQ,predicate:invertedIndexCol2 = '1')", 8, 6
    });
    check(query3, new ResultTable(DATA_SCHEMA, result3));

    // Segment 2 matches all on the first predicate 'pluto' which is removed from the AND, and matches '8' for the
    // second predicate on one row.
    // Segments 1, 3, 4 are all pruned as '8' is out of range for all and 'pluto' doesn't match either
    // The plan for segment 2 is returned as it has precedence over the others and verbose mode is disabled
    String query4 = "EXPLAIN PLAN FOR SELECT noIndexCol1, invertedIndexCol3 FROM testTable WHERE "
        + "invertedIndexCol3 = 'pluto' AND noIndexCol1 = 8 LIMIT 100";
    List<Object[]> result4 = new ArrayList<>();
    result4.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result4.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result4.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result4.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol3)", 3, 2});
    result4.add(new Object[]{"PROJECT(noIndexCol1, invertedIndexCol3)", 4, 3});
    result4.add(new Object[]{"DOC_ID_SET", 5, 4});
    result4.add(new Object[]{
        "FILTER_SORTED_INDEX(indexLookUp:sorted_index,operator:EQ,predicate:noIndexCol1 = '8')", 6, 5
    });
    check(query4, new ResultTable(DATA_SCHEMA, result4));
  }

  @Test
  public void testSelectColumnsVariationsOfAndOperatorsVerbose() {
    // Segments 1 and 4 are pruned as 'pluto' isn't in range and nor is '1.5'
    // Segment 2 has 'pluto' but doesn't have '1.5' so it returns EmptyFilterOperator
    // Segment 3 has '1.5' but doesn't have pluto so it also returns EmptyFilterOperator
    String query1 = "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT noIndexCol1, invertedIndexCol1, "
        + "invertedIndexCol3 FROM testTable WHERE invertedIndexCol1 = 1.5 AND invertedIndexCol3 = 'pluto' LIMIT 100";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result1.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result1.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:2)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result1.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1, invertedIndexCol3)", 3, 2});
    result1.add(new Object[]{PROJEC_TNII_3, 4, 3});
    result1.add(new Object[]{"DOC_ID_SET", 5, 4});
    result1.add(new Object[]{"FILTER_EMPTY", 6, 5});
    check(query1, new ResultTable(DATA_SCHEMA, result1));

    // Segment 1 has a match for both predicates so it return a FILTER_AND plan
    // Segment 2 is pruned as 'mickey' isn't in range (all values are 'pluto')
    // Segment 3, 4 return an EmptyFilterOperator plan as they contain '1.1' but don't contain 'mickey'
    String query2 = "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT noIndexCol1, invertedIndexCol1, "
        + "invertedIndexCol3 FROM testTable WHERE invertedIndexCol1 = 1.1 AND invertedIndexCol3 = 'mickey' LIMIT 100";
    List<Object[]> result2 = new ArrayList<>();
    result2.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result2.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result2.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result2.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1, invertedIndexCol3)", 3, 2});
    result2.add(new Object[]{PROJEC_TNII_3, 4, 3});
    result2.add(new Object[]{"DOC_ID_SET", 5, 4});
    result2.add(new Object[]{"FILTER_AND", 6, 5});
    result2.add(new Object[]{
        "FILTER_SORTED_INDEX(indexLookUp:sorted_index,operator:EQ,predicate:invertedIndexCol3 = 'mickey')", 7, 6
    });
    result2.add(new Object[]{
        "FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:EQ,predicate:invertedIndexCol1 = '1.1')", 8, 6
    });
    result2.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:2)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result2.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1, invertedIndexCol3)", 3, 2});
    result2.add(new Object[]{PROJEC_TNII_3, 4, 3});
    result2.add(new Object[]{"DOC_ID_SET", 5, 4});
    result2.add(new Object[]{"FILTER_EMPTY", 6, 5});
    check(query2, new ResultTable(DATA_SCHEMA, result2));

    // An AND query that matches all predicates on all segments
    String query3 = "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT noIndexCol1, invertedIndexCol1, "
        + "invertedIndexCol2 FROM testTable WHERE invertedIndexCol1 = 0.1 AND invertedIndexCol2 = 1 LIMIT 100";
    List<Object[]> result3 = new ArrayList<>();
    result3.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result3.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result3.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result3.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1, invertedIndexCol2)", 3, 2});
    result3.add(new Object[]{"PROJECT(noIndexCol1, invertedIndexCol1, invertedIndexCol2)", 4, 3});
    result3.add(new Object[]{"DOC_ID_SET", 5, 4});
    result3.add(new Object[]{"FILTER_AND", 6, 5});
    result3.add(new Object[]{
        "FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:EQ,predicate:invertedIndexCol1 = '0.1')", 7, 6
    });
    result3.add(new Object[]{
        "FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:EQ,predicate:invertedIndexCol2 = '1')", 8, 6
    });
    check(query3, new ResultTable(DATA_SCHEMA, result3));

    // Segment 2 matches all on the first predicate 'pluto' which is removed from the AND, and matches '8' for the
    // second predicate on one row.
    // Segments 1, 3, 4 are all pruned as '8' is out of range for all and 'pluto' doesn't match either
    String query4 = "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT noIndexCol1, invertedIndexCol3 FROM "
        + "testTable WHERE invertedIndexCol3 = 'pluto' AND noIndexCol1 = 8 LIMIT 100";
    List<Object[]> result4 = new ArrayList<>();
    result4.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result4.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result4.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result4.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol3)", 3, 2});
    result4.add(new Object[]{"PROJECT(noIndexCol1, invertedIndexCol3)", 4, 3});
    result4.add(new Object[]{"DOC_ID_SET", 5, 4});
    result4.add(new Object[]{
        "FILTER_SORTED_INDEX(indexLookUp:sorted_index,operator:EQ,predicate:noIndexCol1 = '8')", 6, 5
    });
    result4.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:2)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result4.add(new Object[]{"ALL_SEGMENTS_PRUNED_ON_SERVER", 3, 2});
    check(query4, new ResultTable(DATA_SCHEMA, result4));
  }

  @Test
  public void testSelectAggregate() {
    // All segment plans for this queries generate a plan using the MatchAllFilterOperator as it selects and aggregates
    // columns without filtering
    String query1 = "EXPLAIN PLAN FOR SELECT count(*) FROM testTable";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result1.add(new Object[]{"COMBINE_AGGREGATE", 2, 1});
    result1.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result1.add(new Object[]{"FAST_FILTERED_COUNT", 3, 2});
    result1.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 4, 3});
    check(query1, new ResultTable(DATA_SCHEMA, result1));

    // No scan required as metadata is sufficient to answer teh query for all segments
    String query2 = "EXPLAIN PLAN FOR SELECT min(invertedIndexCol1) FROM testTable";
    List<Object[]> result2 = new ArrayList<>();
    result2.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result2.add(new Object[]{"COMBINE_AGGREGATE", 2, 1});
    result2.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result2.add(new Object[]{"AGGREGATE_NO_SCAN", 3, 2});
    check(query2, new ResultTable(DATA_SCHEMA, result2));

    // All segment plans for this queries generate a plan using the MatchAllFilterOperator as it selects and aggregates
    // columns without filtering
    String query3 =
        "EXPLAIN PLAN FOR SELECT count(*), max(noIndexCol1), sum(noIndexCol2), avg(noIndexCol2) FROM testTable";
    List<Object[]> result3 = new ArrayList<>();
    result3.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result3.add(new Object[]{"COMBINE_AGGREGATE", 2, 1});
    result3.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result3.add(
        new Object[]{"AGGREGATE(aggregations:count(*), max(noIndexCol1), sum(noIndexCol2), avg(noIndexCol2))", 3, 2});
    result3.add(new Object[]{"PROJECT(noIndexCol1, noIndexCol2)", 4, 3});
    result3.add(new Object[]{"DOC_ID_SET", 5, 4});
    result3.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 6, 5});
    check(query3, new ResultTable(DATA_SCHEMA, result3));

    // All segment plans for this queries generate a plan using the MatchAllFilterOperator as it selects and aggregates
    // columns without filtering
    String query4 = "EXPLAIN PLAN FOR SELECT sum(add(noIndexCol1, noIndexCol2)), MIN(ADD(DIV(noIndexCol1,noIndexCol2),"
        + "noIndexCol3)) FROM testTable";
    List<Object[]> result4 = new ArrayList<>();
    result4.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result4.add(new Object[]{"COMBINE_AGGREGATE", 2, 1});
    result4.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result4.add(new Object[]{
        "AGGREGATE(aggregations:sum(add(noIndexCol1,noIndexCol2)), "
            + "min(add(div(noIndexCol1,noIndexCol2),noIndexCol3)))", 3, 2
    });
    result4.add(
        new Object[]{"TRANSFORM(add(div(noIndexCol1,noIndexCol2),noIndexCol3), add(noIndexCol1,noIndexCol2))", 4, 3});
    result4.add(new Object[]{"PROJECT(noIndexCol1, noIndexCol2, noIndexCol3)", 5, 4});
    result4.add(new Object[]{"DOC_ID_SET", 6, 5});
    result4.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 7, 6});
    check(query4, new ResultTable(DATA_SCHEMA, result4));

    // No scan required as metadata is sufficient to answer the query for all segments
    String query5 = "EXPLAIN PLAN FOR SELECT DISTINCTSUM(invertedIndexCol1) FROM testTable";
    List<Object[]> result5 = new ArrayList<>();
    result5.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result5.add(new Object[]{"COMBINE_AGGREGATE", 2, 1});
    result5.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result5.add(new Object[]{"AGGREGATE_NO_SCAN", 3, 2});
    check(query5, new ResultTable(DATA_SCHEMA, result5));

    String query6 = "EXPLAIN PLAN FOR SELECT DISTINCTSUMMV(mvNoIndexCol1) FROM testTable";
    List<Object[]> result6 = new ArrayList<>();
    result6.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result6.add(new Object[]{"COMBINE_AGGREGATE", 2, 1});
    result6.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result6.add(new Object[]{"AGGREGATE_NO_SCAN", 3, 2});
    check(query6, new ResultTable(DATA_SCHEMA, result6));

    // Full scan required for distinctavg as the column does not have a dictionary.
    String query7 = "EXPLAIN PLAN FOR SELECT DISTINCTAVG(rawCol1) FROM testTable";
    List<Object[]> result7 = new ArrayList<>();
    result7.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result7.add(new Object[]{"COMBINE_AGGREGATE", 2, 1});
    result7.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result7.add(new Object[]{"AGGREGATE(aggregations:distinctAvg(rawCol1))", 3, 2});
    result7.add(new Object[]{"PROJECT(rawCol1)", 4, 3});
    result7.add(new Object[]{"DOC_ID_SET", 5, 4});
    result7.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 6, 5});
    check(query7, new ResultTable(DATA_SCHEMA, result7));

    String query8 = "EXPLAIN PLAN FOR SELECT DISTINCTAVGMV(mvRawCol1) FROM testTable";
    List<Object[]> result8 = new ArrayList<>();
    result8.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result8.add(new Object[]{"COMBINE_AGGREGATE", 2, 1});
    result8.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result8.add(new Object[]{"AGGREGATE(aggregations:distinctAvgMV(mvRawCol1))", 3, 2});
    result8.add(new Object[]{"PROJECT(mvRawCol1)", 4, 3});
    result8.add(new Object[]{"DOC_ID_SET", 5, 4});
    result8.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 6, 5});
    check(query8, new ResultTable(DATA_SCHEMA, result8));
  }

  @Test
  public void testSelectAggregateVerbose() {
    // All segment plans for this queries generate a plan using the MatchAllFilterOperator as it selects and aggregates
    // columns without filtering
    String query1 = "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT count(*) FROM testTable";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result1.add(new Object[]{"COMBINE_AGGREGATE", 2, 1});
    result1.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result1.add(new Object[]{"FAST_FILTERED_COUNT", 3, 2});
    result1.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 4, 3});
    check(query1, new ResultTable(DATA_SCHEMA, result1));

    // No scan required as metadata is sufficient to answer teh query for all segments
    String query2 = "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT min(invertedIndexCol1) FROM testTable";
    List<Object[]> result2 = new ArrayList<>();
    result2.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result2.add(new Object[]{"COMBINE_AGGREGATE", 2, 1});
    result2.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result2.add(new Object[]{"AGGREGATE_NO_SCAN", 3, 2});
    check(query2, new ResultTable(DATA_SCHEMA, result2));

    // All segment plans for this queries generate a plan using the MatchAllFilterOperator as it selects and aggregates
    // columns without filtering
    String query3 =
        "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT count(*), max(noIndexCol1), sum(noIndexCol2), "
            + "avg(noIndexCol2) FROM testTable";
    List<Object[]> result3 = new ArrayList<>();
    result3.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result3.add(new Object[]{"COMBINE_AGGREGATE", 2, 1});
    result3.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result3.add(
        new Object[]{"AGGREGATE(aggregations:count(*), max(noIndexCol1), sum(noIndexCol2), avg(noIndexCol2))", 3, 2});
    result3.add(new Object[]{"PROJECT(noIndexCol1, noIndexCol2)", 4, 3});
    result3.add(new Object[]{"DOC_ID_SET", 5, 4});
    result3.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 6, 5});
    check(query3, new ResultTable(DATA_SCHEMA, result3));

    // All segment plans for this queries generate a plan using the MatchAllFilterOperator as it selects and aggregates
    // columns without filtering
    String query4 = "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT sum(add(noIndexCol1, noIndexCol2)), "
        + "MIN(ADD(DIV(noIndexCol1, noIndexCol2), noIndexCol3)) FROM testTable";
    List<Object[]> result4 = new ArrayList<>();
    result4.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result4.add(new Object[]{"COMBINE_AGGREGATE", 2, 1});
    result4.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result4.add(new Object[]{
        "AGGREGATE(aggregations:sum(add(noIndexCol1,noIndexCol2)), "
            + "min(add(div(noIndexCol1,noIndexCol2),noIndexCol3)))", 3, 2
    });
    result4.add(
        new Object[]{"TRANSFORM(add(div(noIndexCol1,noIndexCol2),noIndexCol3), add(noIndexCol1,noIndexCol2))", 4, 3});
    result4.add(new Object[]{"PROJECT(noIndexCol1, noIndexCol2, noIndexCol3)", 5, 4});
    result4.add(new Object[]{"DOC_ID_SET", 6, 5});
    result4.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 7, 6});
    check(query4, new ResultTable(DATA_SCHEMA, result4));
  }

  @Test
  public void testSelectAggregateUsingFilterGroupBy() {
    // Segments 2, 3, 4 are pruned as noIndexCol1's values are all > '3'
    // Segment 1 has values which are < '3' so a FILTER_FULL_SCAN is returned
    // The plan for FILTER_FULL_SCAN is returned as it has precedence over NoMatch and verbose mode is disabled
    String query1 =
        "EXPLAIN PLAN FOR SELECT noIndexCol2, sum(add(noIndexCol1, noIndexCol2)), min(noIndexCol3) FROM testTable "
            + "WHERE noIndexCol1 < 3 GROUP BY noIndexCol2";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result1.add(new Object[]{"COMBINE_GROUP_BY", 2, 1});
    result1.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result1.add(new Object[]{
        "GROUP_BY(groupKeys:noIndexCol2, aggregations:sum(add(noIndexCol1,noIndexCol2)), min(noIndexCol3))", 3, 2
    });
    result1.add(new Object[]{"TRANSFORM(add(noIndexCol1,noIndexCol2), noIndexCol2, noIndexCol3)", 4, 3});
    result1.add(new Object[]{"PROJECT(noIndexCol1, noIndexCol2, noIndexCol3)", 5, 4});
    result1.add(new Object[]{"DOC_ID_SET", 6, 5});
    result1.add(new Object[]{"FILTER_FULL_SCAN(operator:RANGE,predicate:noIndexCol1 < '3')", 7, 6});
    check(query1, new ResultTable(DATA_SCHEMA, result1));
  }

  @Test
  public void testSelectAggregateUsingFilterGroupByVerbose() {
    // Segments 2, 3, 4 are pruned as noIndexCol1's values are all > '3'
    // Segment 1 has values which are < '3' so a FILTER_FULL_SCAN is returned
    String query1 =
        "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT noIndexCol2, sum(add(noIndexCol1, noIndexCol2)), "
            + "min(noIndexCol3) FROM testTable WHERE noIndexCol1 < 3 GROUP BY noIndexCol2";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result1.add(new Object[]{"COMBINE_GROUP_BY", 2, 1});
    result1.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:2)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result1.add(new Object[]{"ALL_SEGMENTS_PRUNED_ON_SERVER", 3, 2});
    result1.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result1.add(new Object[]{
        "GROUP_BY(groupKeys:noIndexCol2, aggregations:sum(add(noIndexCol1,noIndexCol2)), min(noIndexCol3))", 3, 2
    });
    result1.add(new Object[]{"TRANSFORM(add(noIndexCol1,noIndexCol2), noIndexCol2, noIndexCol3)", 4, 3});
    result1.add(new Object[]{"PROJECT(noIndexCol1, noIndexCol2, noIndexCol3)", 5, 4});
    result1.add(new Object[]{"DOC_ID_SET", 6, 5});
    result1.add(new Object[]{"FILTER_FULL_SCAN(operator:RANGE,predicate:noIndexCol1 < '3')", 7, 6});
    check(query1, new ResultTable(DATA_SCHEMA, result1));
  }

  @Test
  public void testSelectAggregateUsingFilterIndex() {
    // Segment 2 is pruned because 'mickey' is not within range (and all values in that segment are 'pluto')
    // Segments 3 and 4 don't contain 'mickey' but 'mickey' is within range so they return EmptyFilterOperator
    // Segment 1 contains 'mickey' so the FILTER_SORTED_INDEX plan is returned for it
    // The non-EmptyFilterOperator plan is returned as it has precedence and verbose mode is disabled
    String query1 = "EXPLAIN PLAN FOR SELECT count(*) FROM testTable WHERE invertedIndexCol3 = 'mickey'";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result1.add(new Object[]{"COMBINE_AGGREGATE", 2, 1});
    result1.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result1.add(new Object[]{"FAST_FILTERED_COUNT", 3, 2});
    result1.add(new Object[]{
        "FILTER_SORTED_INDEX(indexLookUp:sorted_index,operator:EQ,predicate:invertedIndexCol3 = 'mickey')", 4, 3
    });
    check(query1, new ResultTable(DATA_SCHEMA, result1));

    // Segment 2 is pruned because 'mickey' is not within range (and all values in that segment are 'pluto')
    // Segments 3 and 4 don't contain 'mickey' but 'mickey' is within range so they return EmptyFilterOperator
    // Segment 1 contains 'mickey' so the FILTER_SORTED_INDEX plan is returned for it
    // The non-EmptyFilterOperator plan is returned as it has precedence and verbose mode is disabled
    String query2 = "EXPLAIN PLAN FOR SELECT sum(noIndexCol2) FROM testTable WHERE invertedIndexCol3 = 'mickey'";
    List<Object[]> result2 = new ArrayList<>();
    result2.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result2.add(new Object[]{"COMBINE_AGGREGATE", 2, 1});
    result2.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result2.add(new Object[]{"AGGREGATE(aggregations:sum(noIndexCol2))", 3, 2});
    result2.add(new Object[]{"PROJECT(noIndexCol2)", 4, 3});
    result2.add(new Object[]{"DOC_ID_SET", 5, 4});
    result2.add(new Object[]{
        "FILTER_SORTED_INDEX(indexLookUp:sorted_index,operator:EQ,predicate:invertedIndexCol3 = 'mickey')", 6, 5
    });
    check(query2, new ResultTable(DATA_SCHEMA, result2));

    // None of the segments have a value of '20' for the noIndexCol1 so this part of the OR predicate is removed for
    // all segments.
    // Segment 3 doesn't have the value '1.1' for the invertedIndexCol1 due to which one plan shows EmptyFilterOperator
    // The non-EmptyFilterOperator plan is returned as it has precedence and verbose mode is disabled
    String query3 =
        "EXPLAIN PLAN FOR SELECT count(*), max(noIndexCol1), sum(noIndexCol2), avg(noIndexCol3) FROM testTable WHERE "
            + "invertedIndexCol1 = 1.1 OR noIndexCol1 = 20";
    List<Object[]> result3 = new ArrayList<>();
    result3.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result3.add(new Object[]{"COMBINE_AGGREGATE", 2, 1});
    result3.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:3)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result3.add(
        new Object[]{"AGGREGATE(aggregations:count(*), max(noIndexCol1), sum(noIndexCol2), avg(noIndexCol3))", 3, 2});
    result3.add(new Object[]{"PROJECT(noIndexCol1, noIndexCol2, noIndexCol3)", 4, 3});
    result3.add(new Object[]{"DOC_ID_SET", 5, 4});
    result3.add(new Object[]{
        "FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:EQ,predicate:invertedIndexCol1 = '1.1')", 6, 5
    });
    check(query3, new ResultTable(DATA_SCHEMA, result3));

    // Use a Transform function in filter on an indexed column.
    // Segment 3 doesn't have the value '1.1' for the invertedIndexCol1 due to which one plan doesn't show the
    // FILTER_OR as that predicate is removed.
    // The deepest tree plan is returned which happens to be the plan with FILTER_OR
    String query4 = "EXPLAIN PLAN FOR SELECT invertedIndexCol3 FROM testTable WHERE concat (invertedIndexCol3, 'test',"
        + "'-') = 'mickey-test' OR invertedIndexCol1 = 1.1";
    List<Object[]> result4 = new ArrayList<>();
    result4.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result4.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result4.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:3)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result4.add(new Object[]{"SELECT(selectList:invertedIndexCol3)", 3, 2});
    result4.add(new Object[]{"PROJECT(invertedIndexCol3)", 4, 3});
    result4.add(new Object[]{"DOC_ID_SET", 5, 4});
    result4.add(new Object[]{"FILTER_OR", 6, 5});
    result4.add(new Object[]{
        "FILTER_EXPRESSION(operator:EQ,predicate:concat(invertedIndexCol3,'test','-') = 'mickey-test')", 7, 6
    });
    result4.add(new Object[]{
        "FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:EQ,predicate:invertedIndexCol1 = '1.1')", 8, 6
    });
    check(query4, new ResultTable(DATA_SCHEMA, result4));

    // Segments 1, 2, 4 have an EmptyFilterOperator plan for this query as '1.5' is within the min-max range but
    // doesn't exist as a value in any row
    // Segment 3 contains a row with the value as '1.5' so a FILTERED_INVERTED_INDEX is returned for 1 segment
    String query5 = "EXPLAIN PLAN FOR SELECT count(*) FROM testTable WHERE invertedIndexCol1 = 1.5 LIMIT 100";
    List<Object[]> result5 = new ArrayList<>();
    result5.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result5.add(new Object[]{"COMBINE_AGGREGATE", 2, 1});
    result5.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result5.add(new Object[]{"FAST_FILTERED_COUNT", 3, 2});
    result5.add(new Object[]{
        "FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:EQ,predicate:invertedIndexCol1 = '1.5')", 4, 3
    });
    check(query5, new ResultTable(DATA_SCHEMA, result5));

    // All segments have a EmptyFilterOperator plan for this query as '1.7' is within the min-max range but doesn't
    // exist as a value in any row
    String query6 = "EXPLAIN PLAN FOR SELECT count(*) FROM testTable WHERE invertedIndexCol1 = 1.7 LIMIT 100";
    List<Object[]> result6 = new ArrayList<>();
    result6.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result6.add(new Object[]{"COMBINE_AGGREGATE", 2, 1});
    result6.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result6.add(new Object[]{"FAST_FILTERED_COUNT", 3, 2});
    result6.add(new Object[]{"FILTER_EMPTY", 4, 3});
    check(query6, new ResultTable(DATA_SCHEMA, result6));

    // Ssegments 1 and 3 are pruned because 'pluto' is outside the range of min-max values of these segments
    // Segment 2 has a MatchAllFilterOperator plan as all rows match 'pluto'
    // Segment 4 has an EmptyFilterOperator plan as 'pluto' doesn't exist but is within the value ranges
    // Only the MatchAllOperator plan is returned as it has higher precedence than no matching segment and verbose is
    // disabled
    String query7 = "EXPLAIN PLAN FOR SELECT count(*) FROM testTable WHERE invertedIndexCol3 = 'pluto' LIMIT 100";
    List<Object[]> result7 = new ArrayList<>();
    result7.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result7.add(new Object[]{"COMBINE_AGGREGATE", 2, 1});
    result7.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result7.add(new Object[]{"FAST_FILTERED_COUNT", 3, 2});
    result7.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 4, 3});
    check(query7, new ResultTable(DATA_SCHEMA, result7));

    // Segment 1 has an EmptyFilterOperator plan for this query as '2' is within the segment range but not present
    // The other segments are pruned as '2' is less than the min for these segments
    // Only the EmptyFilterOperator plan is returned as it has higher precedence than no matching segment and verbose
    // is disabled
    String query8 = "EXPLAIN PLAN FOR SELECT count(*) FROM testTable WHERE noIndexCol1 = 2 LIMIT 100";
    List<Object[]> result8 = new ArrayList<>();
    result8.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result8.add(new Object[]{"COMBINE_AGGREGATE", 2, 1});
    result8.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result8.add(new Object[]{"FAST_FILTERED_COUNT", 3, 2});
    result8.add(new Object[]{"FILTER_EMPTY", 4, 3});
    check(query8, new ResultTable(DATA_SCHEMA, result8));

    // Segment 1 is pruned because 'minnie' and 'pluto' are outside the range of min-max values of the segment
    // Segment 2 has a MatchAllFilterOperator plan as all rows match 'pluto'
    // Segment 3 has a FILTERED_SORTED_COUNT plan as it contains 'minnie'
    // Segment 4 has an EmptyFilterOperator plan as neither 'minnie' nor 'pluto' exists but are within the value ranges
    // Only the FILTERED_SORTED_COUNT plan is returned as it has higher precedence than the others and verbose is
    // disabled
    String query9 = "EXPLAIN PLAN FOR SELECT count(*) FROM testTable WHERE invertedIndexCol3 = 'pluto' OR "
        + "invertedIndexCol3 = 'minnie' LIMIT 100";
    List<Object[]> result9 = new ArrayList<>();
    result9.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result9.add(new Object[]{"COMBINE_AGGREGATE", 2, 1});
    result9.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result9.add(new Object[]{"FAST_FILTERED_COUNT", 3, 2});
    result9.add(new Object[]{
        "FILTER_SORTED_INDEX(indexLookUp:sorted_index,operator:EQ,predicate:invertedIndexCol3 = 'minnie')", 4, 3
    });
    check(query9, new ResultTable(DATA_SCHEMA, result9));

    // All segments are pruned
    String query10 = "EXPLAIN PLAN FOR SELECT count(*) FROM testTable WHERE invertedIndexCol3 = 'roadrunner' AND "
        + "noIndexCol1 = 100 LIMIT 100";
    List<Object[]> result10 = new ArrayList<>();
    result10.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result10.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result10.add(new Object[]{"ALL_SEGMENTS_PRUNED_ON_SERVER", 2, 1});
    check(query10, new ResultTable(DATA_SCHEMA, result10));
  }

  @Test
  public void testSelectAggregateUsingFilterIndexVerbose() {
    // Segment 2 is pruned because 'mickey' is not within range (and all values in that segment are 'pluto')
    // Segments 3 and 4 don't contain 'mickey' but 'mickey' is within range so they return EmptyFilterOperator
    // Segment 1 contains 'mickey' so the FILTER_SORTED_INDEX plan is returned for it
    String query1 = "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT count(*) FROM testTable WHERE "
        + "invertedIndexCol3 = 'mickey'";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result1.add(new Object[]{"COMBINE_AGGREGATE", 2, 1});
    result1.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:2)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result1.add(new Object[]{"FAST_FILTERED_COUNT", 3, 2});
    result1.add(new Object[]{"FILTER_EMPTY", 4, 3});
    result1.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result1.add(new Object[]{"FAST_FILTERED_COUNT", 3, 2});
    result1.add(new Object[]{
        "FILTER_SORTED_INDEX(indexLookUp:sorted_index,operator:EQ,predicate:invertedIndexCol3 = 'mickey')", 4, 3
    });
    check(query1, new ResultTable(DATA_SCHEMA, result1));

    // Segment 2 is pruned because 'mickey' is not within range (and all values in that segment are 'pluto')
    // Segments 3 and 4 don't contain 'mickey' but 'mickey' is within range so they return EmptyFilterOperator
    // Segment 1 contains 'mickey' so the FILTER_SORTED_INDEX plan is returned for it
    String query2 = "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT sum(noIndexCol2) FROM testTable WHERE "
        + "invertedIndexCol3 = 'mickey'";
    List<Object[]> result2 = new ArrayList<>();
    result2.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result2.add(new Object[]{"COMBINE_AGGREGATE", 2, 1});
    result2.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result2.add(new Object[]{"AGGREGATE(aggregations:sum(noIndexCol2))", 3, 2});
    result2.add(new Object[]{"PROJECT(noIndexCol2)", 4, 3});
    result2.add(new Object[]{"DOC_ID_SET", 5, 4});
    result2.add(new Object[]{
        "FILTER_SORTED_INDEX(indexLookUp:sorted_index,operator:EQ,predicate:invertedIndexCol3 = 'mickey')", 6, 5
    });
    result2.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:2)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result2.add(new Object[]{"AGGREGATE(aggregations:sum(noIndexCol2))", 3, 2});
    result2.add(new Object[]{"PROJECT(noIndexCol2)", 4, 3});
    result2.add(new Object[]{"DOC_ID_SET", 5, 4});
    result2.add(new Object[]{"FILTER_EMPTY", 6, 5});
    check(query2, new ResultTable(DATA_SCHEMA, result2));

    // None of the segments have a value of '20' for the noIndexCol1 so this part of the OR predicate is removed for
    // all segments.
    // Segment 3 doesn't have the value '1.1' for the invertedIndexCol1 due to which one plan shows EmptyFilterOperator
    String query3 =
        "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT count(*), max(noIndexCol1), sum(noIndexCol2), "
            + "avg(noIndexCol3) FROM testTable WHERE invertedIndexCol1 = 1.1 OR noIndexCol1 = 20";
    List<Object[]> result3 = new ArrayList<>();
    result3.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result3.add(new Object[]{"COMBINE_AGGREGATE", 2, 1});
    result3.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:3)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result3.add(
        new Object[]{"AGGREGATE(aggregations:count(*), max(noIndexCol1), sum(noIndexCol2), avg(noIndexCol3))", 3, 2});
    result3.add(new Object[]{"PROJECT(noIndexCol1, noIndexCol2, noIndexCol3)", 4, 3});
    result3.add(new Object[]{"DOC_ID_SET", 5, 4});
    result3.add(new Object[]{
        "FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:EQ,predicate:invertedIndexCol1 = '1.1')", 6, 5
    });
    result3.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result3.add(
        new Object[]{"AGGREGATE(aggregations:count(*), max(noIndexCol1), sum(noIndexCol2), avg(noIndexCol3))", 3, 2}
    );
    result3.add(new Object[]{"PROJECT(noIndexCol1, noIndexCol2, noIndexCol3)", 4, 3});
    result3.add(new Object[]{"DOC_ID_SET", 5, 4});

    result3.add(new Object[]{"FILTER_EMPTY", 6, 5});
    check(query3, new ResultTable(DATA_SCHEMA, result3));

    // Use a Transform function in filter on an indexed column.
    // Segment 3 doesn't have the value '1.1' for the invertedIndexCol1 due to which one plan doesn't show the
    // FILTER_OR as that predicate is removed.
    String query4 = "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT invertedIndexCol3 FROM testTable WHERE "
        + "concat (invertedIndexCol3, 'test', '-') = 'mickey-test' OR invertedIndexCol1 = 1.1";
    List<Object[]> result4 = new ArrayList<>();
    result4.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result4.add(new Object[]{"COMBINE_SELECT", 2, 1});
    result4.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result4.add(new Object[]{"SELECT(selectList:invertedIndexCol3)", 3, 2});
    result4.add(new Object[]{"PROJECT(invertedIndexCol3)", 4, 3});
    result4.add(new Object[]{"DOC_ID_SET", 5, 4});
    result4.add(new Object[]{
        "FILTER_EXPRESSION(operator:EQ,predicate:concat(invertedIndexCol3,'test','-') = 'mickey-test')", 6, 5
    });
    result4.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:3)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result4.add(new Object[]{"SELECT(selectList:invertedIndexCol3)", 3, 2});
    result4.add(new Object[]{"PROJECT(invertedIndexCol3)", 4, 3});
    result4.add(new Object[]{"DOC_ID_SET", 5, 4});
    result4.add(new Object[]{"FILTER_OR", 6, 5});
    result4.add(new Object[]{
        "FILTER_EXPRESSION(operator:EQ,predicate:concat(invertedIndexCol3,'test','-') = 'mickey-test')", 7, 6
    });
    result4.add(new Object[]{
        "FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:EQ,predicate:invertedIndexCol1 = '1.1')", 8, 6
    });
    check(query4, new ResultTable(DATA_SCHEMA, result4));

    // Segments 1, 2, 4 have an EmptyFilterOperator plan for this query as '1.5' is within the min-max range but
    // doesn't exist as a value in any row
    // Segment 3 contains a row with the value as '1.5' so a FILTERED_INVERTED_INDEX is returned for 1 segment
    String query5 =
        "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT count(*) FROM testTable WHERE invertedIndexCol1 = 1.5 "
            + "LIMIT 100";
    List<Object[]> result5 = new ArrayList<>();
    result5.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result5.add(new Object[]{"COMBINE_AGGREGATE", 2, 1});
    result5.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:3)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result5.add(new Object[]{"FAST_FILTERED_COUNT", 3, 2});
    result5.add(new Object[]{"FILTER_EMPTY", 4, 3});
    result5.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result5.add(new Object[]{"FAST_FILTERED_COUNT", 3, 2});
    result5.add(new Object[]{
        "FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:EQ,predicate:invertedIndexCol1 = '1.5')", 4, 3
    });
    check(query5, new ResultTable(DATA_SCHEMA, result5));

    // All segments have a EmptyFilterOperator plan for this query as '1.7' is within the min-max range but doesn't
    // exist as a value in any row
    String query6 =
        "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT count(*) FROM testTable WHERE invertedIndexCol1 = 1.7 "
            + "LIMIT 100";
    List<Object[]> result6 = new ArrayList<>();
    result6.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result6.add(new Object[]{"COMBINE_AGGREGATE", 2, 1});
    result6.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result6.add(new Object[]{"FAST_FILTERED_COUNT", 3, 2});
    result6.add(new Object[]{"FILTER_EMPTY", 4, 3});
    check(query6, new ResultTable(DATA_SCHEMA, result6));

    // Ssegments 1 and 3 are pruned because 'pluto' is outside the range of min-max values of these segments
    // Segment 2 has a MatchAllFilterOperator plan as all rows match 'pluto'
    // Segment 4 has an EmptyFilterOperator plan as 'pluto' doesn't exist but is within the value ranges
    String query7 =
        "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT count(*) FROM testTable WHERE invertedIndexCol3 = "
            + "'pluto' LIMIT 100";
    List<Object[]> result7 = new ArrayList<>();
    result7.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result7.add(new Object[]{"COMBINE_AGGREGATE", 2, 1});
    result7.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result7.add(new Object[]{"FAST_FILTERED_COUNT", 3, 2});
    result7.add(new Object[]{"FILTER_EMPTY", 4, 3});
    result7.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result7.add(new Object[]{"FAST_FILTERED_COUNT", 3, 2});
    result7.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 4, 3});
    check(query7, new ResultTable(DATA_SCHEMA, result7));

    // Segment 1 has an EmptyFilterOperator plan for this query as '2' is within the segment range but not present
    // The other segments are pruned as '2' is less than the min for these segments
    String query8 =
        "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT count(*) FROM testTable WHERE noIndexCol1 = 2 LIMIT 100";
    List<Object[]> result8 = new ArrayList<>();
    result8.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result8.add(new Object[]{"COMBINE_AGGREGATE", 2, 1});
    result8.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result8.add(new Object[]{"FAST_FILTERED_COUNT", 3, 2});
    result8.add(new Object[]{"FILTER_EMPTY", 4, 3});
    result8.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:2)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result8.add(new Object[]{"ALL_SEGMENTS_PRUNED_ON_SERVER", 3, 2});
    check(query8, new ResultTable(DATA_SCHEMA, result8));

    // Segment 1 is pruned because 'minnie' and 'pluto' are outside the range of min-max values of the segment
    // Segment 2 has a MatchAllFilterOperator plan as all rows match 'pluto'
    // Segment 3 has a FILTERED_SORTED_COUNT plan as it contains 'minnie'
    // Segment 4 has an EmptyFilterOperator plan as neither 'minnie' nor 'pluto' exists but are within the value ranges
    String query9 =
        "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT count(*) FROM testTable WHERE invertedIndexCol3 = "
            + "'pluto' OR invertedIndexCol3 = 'minnie' LIMIT 100";
    List<Object[]> result9 = new ArrayList<>();
    result9.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result9.add(new Object[]{"COMBINE_AGGREGATE", 2, 1});
    result9.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result9.add(new Object[]{"FAST_FILTERED_COUNT", 3, 2});
    result9.add(new Object[]{
        "FILTER_SORTED_INDEX(indexLookUp:sorted_index,operator:EQ,predicate:invertedIndexCol3 = 'minnie')", 4, 3
    });
    result9.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result9.add(new Object[]{"FAST_FILTERED_COUNT", 3, 2});
    result9.add(new Object[]{"FILTER_EMPTY", 4, 3});
    result9.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:1)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result9.add(new Object[]{"FAST_FILTERED_COUNT", 3, 2});
    result9.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 4, 3});
    check(query9, new ResultTable(DATA_SCHEMA, result9));

    // All segments are pruned
    String query10 = "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT count(*) FROM testTable WHERE "
        + "invertedIndexCol3 = 'roadrunner' AND noIndexCol1 = 100 LIMIT 100";
    List<Object[]> result10 = new ArrayList<>();
    result10.add(new Object[]{"BROKER_REDUCE(limit:100)", 1, 0});
    result10.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result10.add(new Object[]{"ALL_SEGMENTS_PRUNED_ON_SERVER", 2, 1});
    check(query10, new ResultTable(DATA_SCHEMA, result10));
  }

  @Test
  public void testSelectAggregateUsingFilterIndexGroupBy() {
    // All segments match this query as '1' is present in all segments
    String query1 = "EXPLAIN PLAN FOR SELECT noIndexCol1, max(noIndexCol2), min(noIndexCol3) FROM testTable WHERE "
        + "invertedIndexCol2 = 1 GROUP BY noIndexCol1";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result1.add(new Object[]{"COMBINE_GROUP_BY", 2, 1});
    result1.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result1.add(new Object[]{
        "GROUP_BY(groupKeys:noIndexCol1, aggregations:max(noIndexCol2), min(noIndexCol3))", 3, 2
    });
    result1.add(new Object[]{"PROJECT(noIndexCol2, noIndexCol3, noIndexCol1)", 4, 3});
    result1.add(new Object[]{"DOC_ID_SET", 5, 4});
    result1.add(new Object[]{
        "FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:EQ,predicate:invertedIndexCol2 = '1')", 6, 5
    });
    check(query1, new ResultTable(DATA_SCHEMA, result1));
  }

  @Test
  public void testSelectAggregateUsingFilterIndexGroupByVerbose() {
    // All segments match this query as '1' is present in all segments
    String query1 = "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT noIndexCol1, max(noIndexCol2), "
        + "min(noIndexCol3) FROM testTable WHERE invertedIndexCol2 = 1 GROUP BY noIndexCol1";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(new Object[]{"BROKER_REDUCE(limit:10)", 1, 0});
    result1.add(new Object[]{"COMBINE_GROUP_BY", 2, 1});
    result1.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result1.add(new Object[]{
        "GROUP_BY(groupKeys:noIndexCol1, aggregations:max(noIndexCol2), min(noIndexCol3))", 3, 2
    });
    result1.add(new Object[]{"PROJECT(noIndexCol2, noIndexCol3, noIndexCol1)", 4, 3});
    result1.add(new Object[]{"DOC_ID_SET", 5, 4});
    result1.add(new Object[]{
        "FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:EQ,predicate:invertedIndexCol2 = '1')", 6, 5
    });
    check(query1, new ResultTable(DATA_SCHEMA, result1));
  }

  @Test
  public void testSelectAggregateUsingFilterIndexGroupByOrderBy() {
    // All segments match this query as '1' is present in all segments but not for all rows
    String query1 =
        "EXPLAIN PLAN FOR SELECT noIndexCol1, concat(invertedIndexCol3, 'test', '-'), count(*) FROM testTable WHERE "
            + "invertedIndexCol2 != 1 GROUP BY noIndexCol1, concat(invertedIndexCol3, 'test', '-') ORDER BY "
            + "noIndexCol1, concat(invertedIndexCol3, 'test', '-')";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(
        new Object[]{"BROKER_REDUCE(sort:[noIndexCol1 ASC, concat(invertedIndexCol3,'test','-') ASC],limit:10)", 1, 0});
    result1.add(new Object[]{"COMBINE_GROUP_BY", 2, 1});
    result1.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result1.add(new Object[]{
        "GROUP_BY(groupKeys:noIndexCol1, concat(invertedIndexCol3,'test','-'), aggregations:count(*))", 3, 2
    });
    result1.add(new Object[]{"TRANSFORM(concat(invertedIndexCol3,'test','-'), noIndexCol1)", 4, 3});
    result1.add(new Object[]{"PROJECT(noIndexCol1, invertedIndexCol3)", 5, 4});
    result1.add(new Object[]{"DOC_ID_SET", 6, 5});
    result1.add(new Object[]{
        "FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:NOT_EQ,predicate:invertedIndexCol2 !="
            + " '1')", 7, 6
    });
    check(query1, new ResultTable(DATA_SCHEMA, result1));
  }

  @Test
  public void testSelectAggregateUsingFilterIndexGroupByOrderByVerbose() {
    // All segments match this query as '1' is present in all segments but not for all rows
    String query1 =
        "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT noIndexCol1, concat(invertedIndexCol3, 'test', '-'), "
            + "count(*) FROM testTable WHERE invertedIndexCol2 != 1 GROUP BY noIndexCol1, concat(invertedIndexCol3, "
            + "'test', '-') ORDER BY noIndexCol1, concat(invertedIndexCol3, 'test', '-')";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(
        new Object[]{"BROKER_REDUCE(sort:[noIndexCol1 ASC, concat(invertedIndexCol3,'test','-') ASC],limit:10)", 1, 0});
    result1.add(new Object[]{"COMBINE_GROUP_BY", 2, 1});
    result1.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result1.add(new Object[]{
        "GROUP_BY(groupKeys:noIndexCol1, concat(invertedIndexCol3,'test','-'), aggregations:count(*))", 3, 2
    });
    result1.add(new Object[]{"TRANSFORM(concat(invertedIndexCol3,'test','-'), noIndexCol1)", 4, 3});
    result1.add(new Object[]{"PROJECT(noIndexCol1, invertedIndexCol3)", 5, 4});
    result1.add(new Object[]{"DOC_ID_SET", 6, 5});
    result1.add(new Object[]{
        "FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:NOT_EQ,predicate:invertedIndexCol2 != '1')", 7, 6
    });
    check(query1, new ResultTable(DATA_SCHEMA, result1));
  }

  @Test
  public void testSelectAggregateUsingFilterIndexGroupByHaving() {
    // All segments match this query as '1' is present in all segments
    String query = "EXPLAIN PLAN FOR SELECT max(noIndexCol1), min(noIndexCol2), noIndexCol3 FROM testTable WHERE "
        + "invertedIndexCol2 = 1 GROUP BY noIndexCol3 HAVING max(noIndexCol1) > 2 ORDER BY max(noIndexCol1) DESC";
    List<Object[]> result = new ArrayList<>();
    result.add(
        new Object[]{"BROKER_REDUCE(havingFilter:max(noIndexCol1) > '2',sort:[max(noIndexCol1) DESC],limit:10)", 1, 0});
    result.add(new Object[]{"COMBINE_GROUP_BY", 2, 1});
    result.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result.add(new Object[]{
        "GROUP_BY(groupKeys:noIndexCol3, aggregations:max(noIndexCol1), min(noIndexCol2))", 3, 2
    });
    result.add(new Object[]{"PROJECT(noIndexCol1, noIndexCol2, noIndexCol3)", 4, 3});
    result.add(new Object[]{"DOC_ID_SET", 5, 4});
    result.add(new Object[]{
        "FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:EQ,predicate:invertedIndexCol2 = '1')", 6, 5
    });
    check(query, new ResultTable(DATA_SCHEMA, result));
  }

  @Test
  public void testSelectAggregateUsingFilterIndexGroupByHavingVerbose() {
    // All segments match this query as '1' is present in all segments
    String query = "SET explainPlanVerbose=true; EXPLAIN PLAN FOR SELECT max(noIndexCol1), min(noIndexCol2), "
        + "noIndexCol3 FROM testTable WHERE invertedIndexCol2 = 1 GROUP BY noIndexCol3 HAVING max(noIndexCol1) > "
        + "2 ORDER BY max(noIndexCol1) DESC";
    List<Object[]> result = new ArrayList<>();
    result.add(
        new Object[]{"BROKER_REDUCE(havingFilter:max(noIndexCol1) > '2',sort:[max(noIndexCol1) DESC],limit:10)", 1, 0});
    result.add(new Object[]{"COMBINE_GROUP_BY", 2, 1});
    result.add(new Object[]{
        "PLAN_START(numSegmentsForThisPlan:4)", ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS
    });
    result.add(new Object[]{
        "GROUP_BY(groupKeys:noIndexCol3, aggregations:max(noIndexCol1), min(noIndexCol2))", 3, 2
    });
    result.add(new Object[]{"PROJECT(noIndexCol1, noIndexCol2, noIndexCol3)", 4, 3});
    result.add(new Object[]{"DOC_ID_SET", 5, 4});
    result.add(new Object[]{
        "FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:EQ,predicate:invertedIndexCol2 = '1')", 6, 5
    });
    check(query, new ResultTable(DATA_SCHEMA, result));
  }

  @AfterClass
  public void tearDown() {
    _brokerReduceService.shutDown();
    _queryExecutor.shutDown();
    _queryExecutorWithPrefetchEnabled.shutDown();
    for (IndexSegment segment : _indexSegments) {
      segment.destroy();
    }
    FileUtils.deleteQuietly(INDEX_DIR);
  }
}
