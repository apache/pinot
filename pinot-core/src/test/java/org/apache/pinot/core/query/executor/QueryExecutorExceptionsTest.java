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
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.offline.TableDataManagerProvider;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManagerConfig;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.SegmentTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.IngestionSchemaValidator;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEqualsNoOrder;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class QueryExecutorExceptionsTest {
  private static final String AVRO_DATA_PATH = "data/simpleData200001.avro";
  private static final String EMPTY_JSON_DATA_PATH = "data/test_empty_data.json";
  private static final String QUERY_EXECUTOR_CONFIG_PATH = "conf/query-executor.properties";
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "QueryExecutorTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);
  private static final int NUM_SEGMENTS_TO_GENERATE = 2;
  private static final int NUM_EMPTY_SEGMENTS_TO_GENERATE = 2;
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
    assertTrue(INDEX_DIR.mkdirs());
    URL resourceUrl = getClass().getClassLoader().getResource(AVRO_DATA_PATH);
    assertNotNull(resourceUrl);
    File avroFile = new File(resourceUrl.getFile());
    Schema schema = SegmentTestUtils.extractSchemaFromAvroWithoutTime(avroFile);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
    for (int i = 0; i < NUM_SEGMENTS_TO_GENERATE; i++) {
      SegmentGeneratorConfig config =
          SegmentTestUtils.getSegmentGeneratorConfig(avroFile, FileFormat.AVRO, INDEX_DIR, RAW_TABLE_NAME, tableConfig,
              schema);
      config.setSegmentNamePostfix(Integer.toString(i));
      SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
      driver.init(config);
      driver.build();
      IngestionSchemaValidator ingestionSchemaValidator = driver.getIngestionSchemaValidator();
      assertFalse(ingestionSchemaValidator.getDataTypeMismatchResult().isMismatchDetected());
      assertFalse(ingestionSchemaValidator.getSingleValueMultiValueFieldMismatchResult().isMismatchDetected());
      assertFalse(ingestionSchemaValidator.getMultiValueStructureMismatchResult().isMismatchDetected());
      assertFalse(ingestionSchemaValidator.getMissingPinotColumnResult().isMismatchDetected());
      _indexSegments.add(ImmutableSegmentLoader.load(new File(INDEX_DIR, driver.getSegmentName()), ReadMode.mmap));
      _segmentNames.add(driver.getSegmentName());
    }
    resourceUrl = getClass().getClassLoader().getResource(EMPTY_JSON_DATA_PATH);
    assertNotNull(resourceUrl);
    File jsonFile = new File(resourceUrl.getFile());
    for (int i = NUM_SEGMENTS_TO_GENERATE; i < NUM_SEGMENTS_TO_GENERATE + NUM_EMPTY_SEGMENTS_TO_GENERATE; i++) {
      SegmentGeneratorConfig config =
          SegmentTestUtils.getSegmentGeneratorConfig(jsonFile, FileFormat.JSON, INDEX_DIR, RAW_TABLE_NAME, tableConfig,
              schema);
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
    //we don't add index segments to the data manager to simulate numSegmentsAcquired < numSegmentsQueried
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    when(instanceDataManager.getTableDataManager(OFFLINE_TABLE_NAME)).thenReturn(tableDataManager);

    // Set up the query executor
    resourceUrl = getClass().getClassLoader().getResource(QUERY_EXECUTOR_CONFIG_PATH);
    assertNotNull(resourceUrl);
    PropertiesConfiguration queryExecutorConfig = new PropertiesConfiguration();
    queryExecutorConfig.setDelimiterParsingDisabled(false);
    queryExecutorConfig.load(new File(resourceUrl.getFile()));
    _queryExecutor = new ServerQueryExecutorV1Impl();
    _queryExecutor.init(new PinotConfiguration(queryExecutorConfig), instanceDataManager, _serverMetrics);
  }

  /**
   * Given some segments were missing, when a query is executed, then the correct error code is returned along with
   * the list of missing segments.
   */
  @Test
  public void testServerSegmentMissingExceptionDetails() {
    String query = "SELECT COUNT(*) FROM " + OFFLINE_TABLE_NAME;
    InstanceRequest instanceRequest = new InstanceRequest(0L, CalciteSqlCompiler.compileToBrokerRequest(query));
    instanceRequest.setSearchSegments(_segmentNames);
    InstanceResponseBlock instanceResponse = _queryExecutor.execute(getQueryRequest(instanceRequest), QUERY_RUNNERS);
    Map<Integer, String> exceptions = instanceResponse.getExceptions();
    assertTrue(exceptions.containsKey(QueryException.SERVER_SEGMENT_MISSING_ERROR_CODE));

    String errorMessage = exceptions.get(QueryException.SERVER_SEGMENT_MISSING_ERROR_CODE);
    String[] actualMissingSegments = StringUtils.splitByWholeSeparator(
        errorMessage.substring(1 + errorMessage.indexOf('['), errorMessage.indexOf(']')), ", ");
    String[] expectedMissingSegments = new String[]{"testTable_0", "testTable_1", "testTable_2", "testTable_3"};
    assertEqualsNoOrder(actualMissingSegments, expectedMissingSegments);
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
