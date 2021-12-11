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
package org.apache.pinot.core.query.executor;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManagerConfig;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.SegmentTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.IngestionSchemaValidator;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class QueryExecutorTest {
  private static final String AVRO_DATA_PATH = "data/simpleData200001.avro";
  private static final String EMPTY_JSON_DATA_PATH = "data/test_empty_data.json";
  private static final String QUERY_EXECUTOR_CONFIG_PATH = "conf/query-executor.properties";
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "QueryExecutorTest");
  private static final String TABLE_NAME = "testTable";
  private static final int NUM_SEGMENTS_TO_GENERATE = 2;
  private static final int NUM_EMPTY_SEGMENTS_TO_GENERATE = 2;
  private static final Pql2Compiler COMPILER = new Pql2Compiler();
  private static final ExecutorService QUERY_RUNNERS = Executors.newFixedThreadPool(20);

  private final List<ImmutableSegment> _indexSegments = new ArrayList<>(NUM_SEGMENTS_TO_GENERATE);
  private final List<String> _segmentNames = new ArrayList<>(NUM_SEGMENTS_TO_GENERATE);

  private ServerMetrics _serverMetrics;
  private QueryExecutor _queryExecutor;

  @BeforeClass
  public void setUp()
      throws Exception {
    // Set up the segments
    FileUtils.deleteQuietly(INDEX_DIR);
    Assert.assertTrue(INDEX_DIR.mkdirs());
    URL resourceUrl = getClass().getClassLoader().getResource(AVRO_DATA_PATH);
    Assert.assertNotNull(resourceUrl);
    File avroFile = new File(resourceUrl.getFile());
    Schema schema = SegmentTestUtils.extractSchemaFromAvroWithoutTime(avroFile);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    int i = 0;
    for (; i < NUM_SEGMENTS_TO_GENERATE; i++) {
      SegmentGeneratorConfig config = SegmentTestUtils
          .getSegmentGeneratorConfig(avroFile, FileFormat.AVRO, INDEX_DIR, TABLE_NAME, tableConfig, schema);
      config.setSegmentNamePostfix(Integer.toString(i));
      SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
      driver.init(config);
      driver.build();
      IngestionSchemaValidator ingestionSchemaValidator = driver.getIngestionSchemaValidator();
      Assert.assertFalse(ingestionSchemaValidator.getDataTypeMismatchResult().isMismatchDetected());
      Assert.assertFalse(ingestionSchemaValidator.getSingleValueMultiValueFieldMismatchResult().isMismatchDetected());
      Assert.assertFalse(ingestionSchemaValidator.getMultiValueStructureMismatchResult().isMismatchDetected());
      Assert.assertFalse(ingestionSchemaValidator.getMissingPinotColumnResult().isMismatchDetected());
      _indexSegments.add(ImmutableSegmentLoader.load(new File(INDEX_DIR, driver.getSegmentName()), ReadMode.mmap));
      _segmentNames.add(driver.getSegmentName());
    }
    resourceUrl = getClass().getClassLoader().getResource(EMPTY_JSON_DATA_PATH);
    Assert.assertNotNull(resourceUrl);
    File jsonFile = new File(resourceUrl.getFile());
    for (; i < NUM_SEGMENTS_TO_GENERATE + NUM_EMPTY_SEGMENTS_TO_GENERATE; i++) {
      SegmentGeneratorConfig config = SegmentTestUtils
          .getSegmentGeneratorConfig(jsonFile, FileFormat.JSON, INDEX_DIR, TABLE_NAME, tableConfig, schema);
      config.setSegmentNamePostfix(Integer.toString(i));
      SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
      driver.init(config);
      driver.build();
      _indexSegments.add(ImmutableSegmentLoader.load(new File(INDEX_DIR, driver.getSegmentName()), ReadMode.mmap));
      _segmentNames.add(driver.getSegmentName());
    }

    // Mock the instance data manager
    _serverMetrics = new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    TableDataManagerConfig tableDataManagerConfig = mock(TableDataManagerConfig.class);
    when(tableDataManagerConfig.getTableDataManagerType()).thenReturn("OFFLINE");
    when(tableDataManagerConfig.getTableName()).thenReturn(TABLE_NAME);
    when(tableDataManagerConfig.getDataDir()).thenReturn(FileUtils.getTempDirectoryPath());
    @SuppressWarnings("unchecked")
    TableDataManager tableDataManager = TableDataManagerProvider
        .getTableDataManager(tableDataManagerConfig, "testInstance", mock(ZkHelixPropertyStore.class),
            mock(ServerMetrics.class), mock(HelixManager.class), null);
    tableDataManager.start();
    for (ImmutableSegment indexSegment : _indexSegments) {
      tableDataManager.addSegment(indexSegment);
    }
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    when(instanceDataManager.getTableDataManager(TABLE_NAME)).thenReturn(tableDataManager);

    // Set up the query executor
    resourceUrl = getClass().getClassLoader().getResource(QUERY_EXECUTOR_CONFIG_PATH);
    Assert.assertNotNull(resourceUrl);
    PropertiesConfiguration queryExecutorConfig = new PropertiesConfiguration();
    queryExecutorConfig.setDelimiterParsingDisabled(false);
    queryExecutorConfig.load(new File(resourceUrl.getFile()));
    _queryExecutor = new ServerQueryExecutorV1Impl();
    _queryExecutor.init(new PinotConfiguration(queryExecutorConfig), instanceDataManager, _serverMetrics);
  }

  @Test
  public void testCountQuery() {
    String query = "SELECT COUNT(*) FROM " + TABLE_NAME;
    InstanceRequest instanceRequest = new InstanceRequest(0L, COMPILER.compileToBrokerRequest(query));
    instanceRequest.setSearchSegments(_segmentNames);
    DataTable instanceResponse = _queryExecutor.processQuery(getQueryRequest(instanceRequest), QUERY_RUNNERS);
    Assert.assertEquals(instanceResponse.getLong(0, 0), 400002L);
  }

  @Test
  public void testSumQuery() {
    String query = "SELECT SUM(met) FROM " + TABLE_NAME;
    InstanceRequest instanceRequest = new InstanceRequest(0L, COMPILER.compileToBrokerRequest(query));
    instanceRequest.setSearchSegments(_segmentNames);
    DataTable instanceResponse = _queryExecutor.processQuery(getQueryRequest(instanceRequest), QUERY_RUNNERS);
    Assert.assertEquals(instanceResponse.getDouble(0, 0), 40000200000.0);
  }

  @Test
  public void testMaxQuery() {
    String query = "SELECT MAX(met) FROM " + TABLE_NAME;
    InstanceRequest instanceRequest = new InstanceRequest(0L, COMPILER.compileToBrokerRequest(query));
    instanceRequest.setSearchSegments(_segmentNames);
    DataTable instanceResponse = _queryExecutor.processQuery(getQueryRequest(instanceRequest), QUERY_RUNNERS);
    Assert.assertEquals(instanceResponse.getDouble(0, 0), 200000.0);
  }

  @Test
  public void testMinQuery() {
    String query = "SELECT MIN(met) FROM " + TABLE_NAME;
    InstanceRequest instanceRequest = new InstanceRequest(0L, COMPILER.compileToBrokerRequest(query));
    instanceRequest.setSearchSegments(_segmentNames);
    DataTable instanceResponse = _queryExecutor.processQuery(getQueryRequest(instanceRequest), QUERY_RUNNERS);
    Assert.assertEquals(instanceResponse.getDouble(0, 0), 0.0);
  }

  @AfterClass
  public void tearDown() {
    for (IndexSegment segment : _indexSegments) {
      segment.destroy();
    }
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  private ServerQueryRequest getQueryRequest(InstanceRequest instanceRequest) {
    return new ServerQueryRequest(instanceRequest, _serverMetrics, System.currentTimeMillis());
  }
}
