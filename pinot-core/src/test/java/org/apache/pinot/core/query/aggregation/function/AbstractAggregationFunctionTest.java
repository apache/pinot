/*
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
 *
 */

package org.apache.pinot.core.query.aggregation.function;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.offline.TableDataManagerProvider;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.executor.ServerQueryExecutorV1Impl;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.plugin.inputformat.csv.CSVRecordReaderConfig;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManagerConfig;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.SegmentTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.IngestionSchemaValidator;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;


public abstract class AbstractAggregationFunctionTest {
  private static final String QUERY_EXECUTOR_CONFIG_PATH = "conf/query-executor.properties";
  private File _indexDir = new File(FileUtils.getTempDirectory(), getClass().getSimpleName());
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);
  private static final ExecutorService QUERY_RUNNERS = Executors.newFixedThreadPool(20);

  private final Map<TestTable, List<ImmutableSegment>> _indexSegments = new HashMap<>();
  private final Map<TestTable, List<String>> _segmentNames = new HashMap<>();

  private ServerMetrics _serverMetrics;
  private QueryExecutor _queryExecutor;

  protected abstract List<TestTable> getTestCases();

  protected abstract PropertiesConfiguration getQueryExecutorConfig();

  @BeforeClass
  public void setUp()
      throws Exception {
    // Set up the segments
    FileUtils.deleteQuietly(_indexDir);
    assertTrue(_indexDir.mkdirs());

    // Mock the instance data manager
    _serverMetrics = new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    InstanceDataManagerConfig instanceDataManagerConfig = mock(InstanceDataManagerConfig.class);
    when(instanceDataManagerConfig.getMaxParallelSegmentBuilds()).thenReturn(4);
    when(instanceDataManagerConfig.getStreamSegmentDownloadUntarRateLimit()).thenReturn(-1L);
    when(instanceDataManagerConfig.getMaxParallelSegmentDownloads()).thenReturn(-1);
    when(instanceDataManagerConfig.isStreamSegmentDownloadUntar()).thenReturn(false);
    TableDataManagerProvider.init(instanceDataManagerConfig);

    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);

    for (TestTable testTable : getTestCases()) {
      Schema schema = testTable.getSchema();
      TableConfig tableConfig = testTable.getTableConfig();

      List<FakeSegmentContent> segmentContents = testTable.getSegmentContents();
      List<ImmutableSegment> indexSegments = new ArrayList<>(segmentContents.size());
      List<String> segmentNames = new ArrayList<>(segmentContents.size());

      for (int i = 0; i < segmentContents.size(); i++) {
        FakeSegmentContent segmentContent = segmentContents.get(i);
        File inputFile = Files.createTempFile(_indexDir.toPath(), "data-" + testTable.getTableName(), ".csv").toFile();
        try (CSVPrinter csvPrinter = new CSVPrinter(new FileWriter(inputFile), CSVFormat.DEFAULT)) {
          for (List<Object> row : segmentContent) {
            for (Object cell : row) {
              csvPrinter.printRecord(cell);
            }
            csvPrinter.println();
          }
        }
        SegmentGeneratorConfig config = SegmentTestUtils.getSegmentGeneratorConfig(
            inputFile, FileFormat.CSV, _indexDir, RAW_TABLE_NAME, tableConfig, schema);
        config.setSegmentNamePostfix(Integer.toString(i));
        SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
        driver.init(config);
        driver.build();
        IngestionSchemaValidator ingestionSchemaValidator = driver.getIngestionSchemaValidator();
        Assert.assertFalse(ingestionSchemaValidator.getDataTypeMismatchResult().isMismatchDetected());
        Assert.assertFalse(ingestionSchemaValidator.getSingleValueMultiValueFieldMismatchResult().isMismatchDetected());
        Assert.assertFalse(ingestionSchemaValidator.getMultiValueStructureMismatchResult().isMismatchDetected());
        Assert.assertFalse(ingestionSchemaValidator.getMissingPinotColumnResult().isMismatchDetected());


        indexSegments.add(ImmutableSegmentLoader.load(new File(_indexDir, driver.getSegmentName()), ReadMode.mmap));
        segmentNames.add(driver.getSegmentName());
      }
      _indexSegments.put(testTable, indexSegments);
      _segmentNames.put(testTable, segmentNames);


      String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(testTable.getTableName());
      TableDataManagerConfig tableDataManagerConfig = mock(TableDataManagerConfig.class);
      when(tableDataManagerConfig.getTableName()).thenReturn(offlineTableName);
      when(tableDataManagerConfig.getTableType()).thenReturn(TableType.OFFLINE);
      when(tableDataManagerConfig.getDataDir()).thenReturn(FileUtils.getTempDirectoryPath());
      @SuppressWarnings("unchecked")
      TableDataManager tableDataManager =
          TableDataManagerProvider.getTableDataManager(tableDataManagerConfig, "testInstance",
              mock(ZkHelixPropertyStore.class), mock(ServerMetrics.class), mock(HelixManager.class), null);
      tableDataManager.start();
      for (ImmutableSegment indexSegment : indexSegments) {
        tableDataManager.addSegment(indexSegment);
      }
      when(instanceDataManager.getTableDataManager(offlineTableName)).thenReturn(tableDataManager);
    }

    // Set up the query executor
    PropertiesConfiguration queryExecutorConfig = getQueryExecutorConfig();
    _queryExecutor = new ServerQueryExecutorV1Impl();
    _queryExecutor.init(new PinotConfiguration(queryExecutorConfig), instanceDataManager, _serverMetrics);
  }

  protected InstanceResponseBlock executeQuery(TestTable table, String query) {
    InstanceRequest instanceRequest = new InstanceRequest(0L, CalciteSqlCompiler.compileToBrokerRequest(query));
    instanceRequest.setSearchSegments(_segmentNames.get(table));
    return _queryExecutor.execute(getQueryRequest(instanceRequest), QUERY_RUNNERS);
  }

  @AfterClass
  public void tearDown() {
    for (List<ImmutableSegment> segments : _indexSegments.values()) {
      for (ImmutableSegment segment : segments) {
        segment.destroy();
      }
    }
    FileUtils.deleteQuietly(_indexDir);
  }

  public GivenTable givenTable(Schema schema, TableConfig tableConfig) {
    return new GivenTable(_indexDir, tableConfig, schema);
  }

  private final Schema _nullableIntSchema = new Schema.SchemaBuilder()
      .setSchemaName(RAW_TABLE_NAME)
      .addDimensionField("myInt", FieldSpec.DataType.INT, f -> f.setNullable(true))
      .build();

  private final TableConfig _nullableIntTableConfig = new TableConfigBuilder(TableType.OFFLINE)
      .setTableName(RAW_TABLE_NAME)
      .build();
  public GivenTable givenNullableIntTable() {
    return givenTable(_nullableIntSchema, _nullableIntTableConfig);
  }

  public class GivenTable {
    private final File _indexDir;
    private final TableConfig _tableConfig;
    private final Schema _schema;

    protected GivenTable(File indexDir, TableConfig tableConfig, Schema schema) {
      _indexDir = indexDir;
      _tableConfig = tableConfig;
      _schema = schema;
    }

    public WithSegment withSegment(Object[]... content) {
      return new WithSegment(new FakeSegmentContent(content), _indexDir, _tableConfig, _schema);
    }
  }

  private ServerQueryRequest getQueryRequest(InstanceRequest instanceRequest) {
    return new ServerQueryRequest(instanceRequest, _serverMetrics, System.currentTimeMillis());
  }

  public class WithSegment {

    private final List<FakeSegmentContent> _segmentContents = new ArrayList<>();
    private final File _indexDir;
    private final TableConfig _tableConfig;
    private final Schema _schema;

    public WithSegment(FakeSegmentContent content, File indexDir, TableConfig tableConfig, Schema schema) {
      _segmentContents.add(content);
      _indexDir = indexDir;
      _tableConfig = tableConfig;
      _schema = schema;
    }

    public WithSegment andSegment(Object[]... content) {
      _segmentContents.add(new FakeSegmentContent(content));
      return this;
    }

    public QueryExecuted whenQuery(String query) {
      List<ImmutableSegment> indexSegments = new ArrayList<>(_segmentContents.size());
      List<String> segmentNames = new ArrayList<>(_segmentContents.size());

      try {
        for (int i = 0; i < _segmentContents.size(); i++) {
          FakeSegmentContent segmentContent = _segmentContents.get(i);
          File inputFile = Files.createTempFile(_indexDir.toPath(), "data", ".csv").toFile();
          try (CSVPrinter csvPrinter = new CSVPrinter(new FileWriter(inputFile), CSVFormat.DEFAULT)) {
            for (List<Object> row : segmentContent) {
              for (Object cell : row) {
                csvPrinter.printRecord(cell);
              }
              csvPrinter.println();
            }
          }
          SegmentGeneratorConfig config =
              SegmentTestUtils.getSegmentGeneratorConfig(inputFile, FileFormat.CSV, _indexDir, RAW_TABLE_NAME,
                  _tableConfig, _schema);
          CSVRecordReaderConfig csvRecordReaderConfig = new CSVRecordReaderConfig();
          String header = _schema.getPhysicalColumnNames().stream().collect(Collectors.joining(","));
          csvRecordReaderConfig.setHeader(header);
          csvRecordReaderConfig.setSkipHeader(false);
          config.setReaderConfig(csvRecordReaderConfig);
          config.setSegmentNamePostfix(Integer.toString(i));
          SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
          driver.init(config);
          driver.build();

          indexSegments.add(ImmutableSegmentLoader.load(new File(_indexDir, driver.getSegmentName()), ReadMode.mmap));
          segmentNames.add(driver.getSegmentName());
        }

        String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);
        TableDataManagerConfig tableDataManagerConfig = mock(TableDataManagerConfig.class);
        when(tableDataManagerConfig.getTableName()).thenReturn(offlineTableName);
        when(tableDataManagerConfig.getTableType()).thenReturn(TableType.OFFLINE);
        when(tableDataManagerConfig.getDataDir()).thenReturn(_indexDir.getAbsolutePath());
        @SuppressWarnings("unchecked")
        TableDataManager tableDataManager =
            TableDataManagerProvider.getTableDataManager(tableDataManagerConfig, "testInstance",
                mock(ZkHelixPropertyStore.class), mock(ServerMetrics.class), mock(HelixManager.class), null);
        tableDataManager.start();
        for (ImmutableSegment indexSegment : indexSegments) {
          tableDataManager.addSegment(indexSegment);
        }

        InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
        when(instanceDataManager.getTableDataManager(RAW_TABLE_NAME)).thenReturn(tableDataManager);

        PropertiesConfiguration queryExecutorConfig = getQueryExecutorConfig();
        QueryExecutor queryExecutor = new ServerQueryExecutorV1Impl();
        queryExecutor.init(new PinotConfiguration(queryExecutorConfig), instanceDataManager, _serverMetrics);

        InstanceRequest instanceRequest = new InstanceRequest(0L, CalciteSqlCompiler.compileToBrokerRequest(query));
        instanceRequest.setSearchSegments(segmentNames);

        ServerQueryRequest serverQueryRequest = new ServerQueryRequest(instanceRequest, _serverMetrics,
            System.currentTimeMillis());
        InstanceResponseBlock responseBlock = queryExecutor.execute(serverQueryRequest, QUERY_RUNNERS);
        return new QueryExecuted(responseBlock);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static class QueryExecuted {
    public final InstanceResponseBlock _responseBlock;

    public QueryExecuted(InstanceResponseBlock responseBlock) {
      _responseBlock = responseBlock;
    }

    public QueryExecuted thenResultIs(Object[]... expectedResult) {
      if (_responseBlock.getExceptions() != null && !_responseBlock.getExceptions().isEmpty()) {
        Assert.fail("Query failed with " + _responseBlock.getExceptions());
      }

      List<Object[]> actualRows = _responseBlock.getResultsBlock().getRows();
      for (int i = 0; i < actualRows.size(); i++) {
        Object[] actualRow = actualRows.get(i);
        Object[] expectedRow = expectedResult[i];
        for (int j = 0; j < actualRow.length; j++) {
          Assert.assertEquals(actualRow[j].getClass(), expectedRow[j].getClass());
          Assert.assertEquals(actualRow[j], expectedRow[j]);
        }
      }
      return this;
    }
  }


  protected static abstract class TestTable {

    protected String getTableName() {
      return getClass().getSimpleName();
    }

    protected abstract TableConfig getTableConfig();

    protected abstract Schema getSchema();

    protected abstract List<FakeSegmentContent> getSegmentContents();
  }

  public static class FakeSegmentContent extends ArrayList<List<Object>> {
    public FakeSegmentContent(Object[]... rows) {
      super(rows.length);
      for (Object[] row : rows) {
        add(Arrays.asList(row));
      }
    }
  }
}
