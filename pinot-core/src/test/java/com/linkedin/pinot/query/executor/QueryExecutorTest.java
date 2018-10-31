/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.query.executor;

import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.request.InstanceRequest;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.data.manager.InstanceDataManager;
import com.linkedin.pinot.core.data.manager.TableDataManager;
import com.linkedin.pinot.core.data.manager.config.TableDataManagerConfig;
import com.linkedin.pinot.core.data.manager.offline.TableDataManagerProvider;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegment;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import com.linkedin.pinot.core.query.executor.QueryExecutor;
import com.linkedin.pinot.core.query.executor.ServerQueryExecutorV1Impl;
import com.linkedin.pinot.core.query.request.ServerQueryRequest;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;
import com.yammer.metrics.core.MetricsRegistry;
import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class QueryExecutorTest {
  private static final String AVRO_DATA_PATH = "data/simpleData200001.avro";
  private static final String QUERY_EXECUTOR_CONFIG_PATH = "conf/query-executor.properties";
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "QueryExecutorTest");
  private static final String TABLE_NAME = "testTable";
  private static final int NUM_SEGMENTS_TO_GENERATE = 2;
  private static final Pql2Compiler COMPILER = new Pql2Compiler();
  private static final ExecutorService QUERY_RUNNERS = Executors.newFixedThreadPool(20);

  private final List<ImmutableSegment> _indexSegments = new ArrayList<>(NUM_SEGMENTS_TO_GENERATE);
  private final List<String> _segmentNames = new ArrayList<>(NUM_SEGMENTS_TO_GENERATE);

  private ServerMetrics _serverMetrics;
  private QueryExecutor _queryExecutor;

  @BeforeClass
  public void setUp() throws Exception {
    // Set up the segments
    FileUtils.deleteQuietly(INDEX_DIR);
    Assert.assertTrue(INDEX_DIR.mkdirs());
    URL resourceUrl = getClass().getClassLoader().getResource(AVRO_DATA_PATH);
    Assert.assertNotNull(resourceUrl);
    File avroFile = new File(resourceUrl.getFile());
    for (int i = 0; i < NUM_SEGMENTS_TO_GENERATE; i++) {
      SegmentGeneratorConfig config =
          SegmentTestUtils.getSegmentGeneratorConfigWithoutTimeColumn(avroFile, INDEX_DIR, TABLE_NAME);
      config.setSegmentNamePostfix(Integer.toString(i));
      SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
      driver.init(config);
      driver.build();
      _indexSegments.add(ImmutableSegmentLoader.load(new File(INDEX_DIR, driver.getSegmentName()), ReadMode.mmap));
      _segmentNames.add(driver.getSegmentName());
    }

    // Mock the instance data manager
    _serverMetrics = new ServerMetrics(new MetricsRegistry());
    TableDataManagerConfig tableDataManagerConfig = mock(TableDataManagerConfig.class);
    when(tableDataManagerConfig.getTableDataManagerType()).thenReturn("OFFLINE");
    when(tableDataManagerConfig.getTableName()).thenReturn(TABLE_NAME);
    when(tableDataManagerConfig.getDataDir()).thenReturn(FileUtils.getTempDirectoryPath());
    @SuppressWarnings("unchecked")
    TableDataManager tableDataManager =
        TableDataManagerProvider.getTableDataManager(tableDataManagerConfig, "testInstance",
            mock(ZkHelixPropertyStore.class), mock(ServerMetrics.class));
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
    _queryExecutor.init(queryExecutorConfig, instanceDataManager, _serverMetrics);
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
