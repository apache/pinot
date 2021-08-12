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
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.metrics.PinotMetricUtils;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.offline.TableDataManagerProvider;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.executor.ServerQueryExecutorV1Impl;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManagerConfig;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.IngestionSchemaValidator;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.segment.local.segment.index.creator.RawIndexCreatorTest.getRandomValue;


/**
 * Class for testing segment generation with byte[] data type.
 */
public class SegmentWithNullValueVectorTest {
  private static final int NUM_ROWS = 10001;

  private static final String SEGMENT_DIR_NAME =
      System.getProperty("java.io.tmpdir") + File.separator + "nullValueVectorTest";
  private static final String SEGMENT_NAME = "testSegment";
  private static final long LONG_VALUE_THRESHOLD = 100;

  private Random _random;
  private Schema _schema;
  private ImmutableSegment _segment;

  private static final String INT_COLUMN = "intColumn";
  private static final String LONG_COLUMN = "longColumn";
  private static final String FLOAT_COLUMN = "floatColumn";
  private static final String DOUBLE_COLUMN = "doubleColumn";
  private static final String STRING_COLUMN = "stringColumn";

  Map<String, boolean[]> _actualNullVectorMap = new HashMap<>();

  // Required for subsequent queries
  private static final Pql2Compiler COMPILER = new Pql2Compiler();
  private final List<String> _segmentNames = new ArrayList<>();
  private InstanceDataManager _instanceDataManager;
  private ServerMetrics _serverMetrics;
  private QueryExecutor _queryExecutor;
  private static final String TABLE_NAME = "testTable";
  private static final String QUERY_EXECUTOR_CONFIG_PATH = "conf/query-executor.properties";
  private static final ExecutorService QUERY_RUNNERS = Executors.newFixedThreadPool(20);
  private int nullIntKeyCount = 0;
  private int longKeyCount = 0;

  /**
   * Setup to build a segment with raw indexes (no-dictionary) of various data types.
   *
   * @throws Exception
   */
  @BeforeClass
  public void setup()
      throws Exception {

    _schema = new Schema();
    _schema.addField(new DimensionFieldSpec(INT_COLUMN, FieldSpec.DataType.INT, true));
    _schema.addField(new DimensionFieldSpec(LONG_COLUMN, FieldSpec.DataType.LONG, true));
    _schema.addField(new DimensionFieldSpec(FLOAT_COLUMN, FieldSpec.DataType.FLOAT, true));
    _schema.addField(new DimensionFieldSpec(DOUBLE_COLUMN, FieldSpec.DataType.DOUBLE, true));
    _schema.addField(new DimensionFieldSpec(STRING_COLUMN, FieldSpec.DataType.STRING, true));

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    _random = new Random(System.nanoTime());
    buildIndex(tableConfig, _schema);
    _segment = ImmutableSegmentLoader.load(new File(SEGMENT_DIR_NAME, SEGMENT_NAME), ReadMode.heap);

    setupQueryServer();
  }

  // Registers the segment and initializes Query Executor
  private void setupQueryServer()
      throws ConfigurationException {
    _segmentNames.add(_segment.getSegmentName());
    // Mock the instance data manager
    _serverMetrics = new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    TableDataManagerConfig tableDataManagerConfig = Mockito.mock(TableDataManagerConfig.class);
    Mockito.when(tableDataManagerConfig.getTableDataManagerType()).thenReturn("OFFLINE");
    Mockito.when(tableDataManagerConfig.getTableName()).thenReturn(TABLE_NAME);
    Mockito.when(tableDataManagerConfig.getDataDir()).thenReturn(FileUtils.getTempDirectoryPath());
    @SuppressWarnings("unchecked")
    TableDataManager tableDataManager = TableDataManagerProvider
        .getTableDataManager(tableDataManagerConfig, "testInstance", Mockito.mock(ZkHelixPropertyStore.class),
            Mockito.mock(ServerMetrics.class), Mockito.mock(HelixManager.class), null);
    tableDataManager.start();
    tableDataManager.addSegment(_segment);
    _instanceDataManager = Mockito.mock(InstanceDataManager.class);
    Mockito.when(_instanceDataManager.getTableDataManager(TABLE_NAME)).thenReturn(tableDataManager);

    // Set up the query executor
    URL resourceUrl = getClass().getClassLoader().getResource(QUERY_EXECUTOR_CONFIG_PATH);
    Assert.assertNotNull(resourceUrl);
    PropertiesConfiguration queryExecutorConfig = new PropertiesConfiguration();
    queryExecutorConfig.setDelimiterParsingDisabled(false);
    queryExecutorConfig.load(new File(resourceUrl.getFile()));
    _queryExecutor = new ServerQueryExecutorV1Impl();
    _queryExecutor.init(new PinotConfiguration(queryExecutorConfig), _instanceDataManager, _serverMetrics);
  }

  /**
   * Helper method to build a segment containing a single valued string column with RAW (no-dictionary) index.
   *
   * @return Array of string values for the rows in the generated index.
   * @throws Exception
   */

  private void buildIndex(TableConfig tableConfig, Schema schema)
      throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);

    config.setOutDir(SEGMENT_DIR_NAME);
    config.setSegmentName(SEGMENT_NAME);
    config.setNullHandlingEnabled(true);

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      boolean[] value = new boolean[NUM_ROWS];
      Arrays.fill(value, false);
      _actualNullVectorMap.put(fieldSpec.getName(), value);
    }
    for (int i = 0; i < NUM_ROWS; i++) {
      HashMap<String, Object> map = new HashMap<>();

      for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
        Object value;
        value = getRandomValue(_random, fieldSpec.getDataType());
        map.put(fieldSpec.getName(), value);
      }

      GenericRow genericRow = new GenericRow();
      //Remove some values to simulate null
      int rowId = i;
      Iterator<Map.Entry<String, Object>> iterator = map.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<String, Object> entry = iterator.next();
        if (_random.nextDouble() < .1) {
          String key = entry.getKey();
          if (_random.nextBoolean()) {
            iterator.remove();
          } else {
            entry.setValue(null);
          }
          _actualNullVectorMap.get(key)[rowId] = true;
        }
      }

      if (_actualNullVectorMap.get(INT_COLUMN)[rowId]) {
        nullIntKeyCount++;
      } else if (!_actualNullVectorMap.get(LONG_COLUMN)[rowId]) {
        if ((long) map.get(LONG_COLUMN) > LONG_VALUE_THRESHOLD) {
          longKeyCount++;
        }
      }

      genericRow.init(map);
      rows.add(genericRow);
    }

    RecordReader recordReader = new GenericRowRecordReader(rows);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, recordReader);
    driver.build();
    IngestionSchemaValidator ingestionSchemaValidator = driver.getIngestionSchemaValidator();
    // Schema validator should be null since the record reader is a generic row record reader
    Assert.assertNull(ingestionSchemaValidator);
  }

  @Test
  public void test()
      throws Exception {
    Map<String, NullValueVectorReader> nullValueVectorReaderMap = new HashMap<>();
    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {

      NullValueVectorReader nullValueVector = _segment.getDataSource(fieldSpec.getName()).getNullValueVector();
      System.out.println("field = " + fieldSpec.getName());
      Assert.assertNotNull(nullValueVector);
      nullValueVectorReaderMap.put(fieldSpec.getName(), nullValueVector);
    }
    for (int i = 0; i < NUM_ROWS; i++) {
      for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
        String colName = fieldSpec.getName();
        Assert.assertEquals(_actualNullVectorMap.get(colName)[i], nullValueVectorReaderMap.get(colName).isNull(i));
      }
    }
  }

  @Test
  public void testNotNullPredicate() {
    String query = "SELECT COUNT(*) FROM " + TABLE_NAME + " where " + INT_COLUMN + " IS NOT NULL";
    InstanceRequest instanceRequest = new InstanceRequest(0L, COMPILER.compileToBrokerRequest(query));
    instanceRequest.setSearchSegments(_segmentNames);
    DataTable instanceResponse = _queryExecutor.processQuery(getQueryRequest(instanceRequest), QUERY_RUNNERS);
    Assert.assertEquals(instanceResponse.getLong(0, 0), NUM_ROWS - nullIntKeyCount);
  }

  @Test
  public void testNullPredicate() {
    String query = "SELECT COUNT(*) FROM " + TABLE_NAME + " where " + INT_COLUMN + " IS NULL";
    InstanceRequest instanceRequest = new InstanceRequest(0L, COMPILER.compileToBrokerRequest(query));
    instanceRequest.setSearchSegments(_segmentNames);
    DataTable instanceResponse = _queryExecutor.processQuery(getQueryRequest(instanceRequest), QUERY_RUNNERS);
    Assert.assertEquals(instanceResponse.getLong(0, 0), nullIntKeyCount);
  }

  @Test
  public void testNullWithAndPredicate() {
    String query =
        "SELECT COUNT(*) FROM " + TABLE_NAME + " where " + INT_COLUMN + " IS NOT NULL and " + LONG_COLUMN + " > "
            + LONG_VALUE_THRESHOLD;
    InstanceRequest instanceRequest = new InstanceRequest(0L, COMPILER.compileToBrokerRequest(query));
    instanceRequest.setSearchSegments(_segmentNames);
    DataTable instanceResponse = _queryExecutor.processQuery(getQueryRequest(instanceRequest), QUERY_RUNNERS);
    Assert.assertEquals(instanceResponse.getLong(0, 0), longKeyCount);
  }

  private ServerQueryRequest getQueryRequest(InstanceRequest instanceRequest) {
    return new ServerQueryRequest(instanceRequest, _serverMetrics, System.currentTimeMillis());
  }

  /**
   * Clean up after test
   */
  @AfterClass
  public void cleanup()
      throws IOException {
    _segment.destroy();
    FileUtils.deleteQuietly(new File(SEGMENT_DIR_NAME));
  }
}
