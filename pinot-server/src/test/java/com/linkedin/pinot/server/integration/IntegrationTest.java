/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.server.integration;

import com.linkedin.pinot.common.query.QueryRequest;
import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.query.QueryExecutor;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.InstanceRequest;
import com.linkedin.pinot.common.request.QuerySource;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.data.manager.offline.FileBasedInstanceDataManager;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;
import com.linkedin.pinot.server.conf.ServerConf;
import com.linkedin.pinot.server.starter.ServerInstance;
import com.linkedin.pinot.util.TestUtils;
import com.yammer.metrics.core.MetricsRegistry;


public class IntegrationTest {

  private final String SMALL_AVRO_DATA = "data/simpleData200001.avro";
  private static File INDEXES_DIR = new File("/tmp/IntegrationTestList-" + System.currentTimeMillis());

  private List<IndexSegment> _indexSegmentList = new ArrayList<IndexSegment>();

  private static final Logger LOGGER = LoggerFactory.getLogger(IntegrationTest.class);

  public static final String PINOT_PROPERTIES = "pinot.properties";

  private static ServerConf _serverConf;
  private static ServerInstance _serverInstance;
  private static QueryExecutor _queryExecutor;

  @BeforeTest
  public void setUp() throws Exception {
    //Process Command Line to get config and port
    FileUtils.deleteDirectory(new File("/tmp/pinot/test1"));
    setupSegmentList();
    File confFile = new File(TestUtils.getFileFromResourceUrl(InstanceServerStarter.class.getClassLoader().getResource("conf/" + PINOT_PROPERTIES)));
    // build _serverConf
    PropertiesConfiguration serverConf = new PropertiesConfiguration();
    serverConf.setDelimiterParsingDisabled(false);
    serverConf.load(confFile);
    _serverConf = new ServerConf(serverConf);

    LOGGER.info("Trying to create a new ServerInstance!");
    _serverInstance = new ServerInstance();
    LOGGER.info("Trying to initial ServerInstance!");
    _serverInstance.init(_serverConf, new MetricsRegistry());
    LOGGER.info("Trying to start ServerInstance!");
    _serverInstance.start();
    _queryExecutor = _serverInstance.getQueryExecutor();

    FileBasedInstanceDataManager instanceDataManager = (FileBasedInstanceDataManager) _serverInstance.getInstanceDataManager();
    for (int i = 0; i < 2; ++i) {
      instanceDataManager.getTableDataManager("testTable");
      instanceDataManager.getTableDataManager("testTable").addSegment(_indexSegmentList.get(i));
    }
  }

  @AfterTest
  public static void Shutdown() {
    _serverInstance.shutDown();
    if (INDEXES_DIR.exists()) {
      FileUtils.deleteQuietly(INDEXES_DIR);
    }
  }

  private void setupSegmentList() throws Exception {
    final URL resource = getClass().getClassLoader().getResource(SMALL_AVRO_DATA);
    final String filePath = TestUtils.getFileFromResourceUrl(resource);
    _indexSegmentList.clear();
    if (INDEXES_DIR.exists()) {
      FileUtils.deleteQuietly(INDEXES_DIR);
    }
    INDEXES_DIR.mkdir();

    for (int i = 0; i < 2; ++i) {
      final File segmentDir = new File(INDEXES_DIR, "segment_" + i);

      final SegmentGeneratorConfig config =
          SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), segmentDir, "dim" + i,
              TimeUnit.DAYS, "testTable");

      final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
      driver.init(config);
      driver.build();

      _indexSegmentList.add(ColumnarSegmentLoader.load(new File(new File(INDEXES_DIR, "segment_" + String.valueOf(i)), driver.getSegmentName()), ReadMode.mmap));

      System.out.println("built at : " + segmentDir.getAbsolutePath());
    }

  }

  @Test
  public void testWvmpQuery() {

    BrokerRequest brokerRequest = getCountQuery();
    brokerRequest.setFilterQuery(null);
    brokerRequest.setFilterQueryIsSet(false);
    QuerySource querySource = new QuerySource();
    querySource.setTableName("testTable");
    brokerRequest.setQuerySource(querySource);
    InstanceRequest instanceRequest = new InstanceRequest(0, brokerRequest);
    List<String> searchSegments = new ArrayList<String>();
    searchSegments.add("testTable_0_9_");
    instanceRequest.setSearchSegments(searchSegments);
    try {
      QueryRequest queryRequest = new QueryRequest(instanceRequest, _serverInstance.getServerMetrics());
      DataTable instanceResponse = _queryExecutor.processQuery(queryRequest);
      System.out.println(instanceResponse.getLong(0, 0));
      System.out.println(instanceResponse.getMetadata().get(DataTable.TIME_USED_MS_METADATA_KEY));
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testCountQuery() {

    BrokerRequest brokerRequest = getCountQuery();

    QuerySource querySource = new QuerySource();
    querySource.setTableName("testTable");
    brokerRequest.setQuerySource(querySource);
    InstanceRequest instanceRequest = new InstanceRequest(0, brokerRequest);
    addTestTableSearchSegmentsToInstanceRequest(instanceRequest);
    try {
      QueryRequest queryRequest = new QueryRequest(instanceRequest, _serverInstance.getServerMetrics());
      DataTable instanceResponse = _queryExecutor.processQuery(queryRequest);
      System.out.println(instanceResponse.getLong(0, 0));
      System.out.println(instanceResponse.getMetadata().get(DataTable.TIME_USED_MS_METADATA_KEY));
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  private void addTestTableSearchSegmentsToInstanceRequest(InstanceRequest instanceRequest) {
    for (IndexSegment segment : _indexSegmentList)
      instanceRequest.addToSearchSegments(segment.getSegmentName());
  }

  @Test
  public void testSumQuery() {
    BrokerRequest brokerRequest = getSumQuery();

    QuerySource querySource = new QuerySource();
    querySource.setTableName("testTable");
    brokerRequest.setQuerySource(querySource);
    InstanceRequest instanceRequest = new InstanceRequest(0, brokerRequest);
    addTestTableSearchSegmentsToInstanceRequest(instanceRequest);
    try {
      QueryRequest queryRequest = new QueryRequest(instanceRequest, _serverInstance.getServerMetrics());
      DataTable instanceResponse = _queryExecutor.processQuery(queryRequest);
      System.out.println(instanceResponse.getDouble(0, 0));
      System.out.println(instanceResponse.getMetadata().get(DataTable.TIME_USED_MS_METADATA_KEY));
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

  }

  @Test
  public void testMaxQuery() {

    BrokerRequest brokerRequest = getMaxQuery();

    QuerySource querySource = new QuerySource();
    querySource.setTableName("testTable");
    brokerRequest.setQuerySource(querySource);
    InstanceRequest instanceRequest = new InstanceRequest(0, brokerRequest);
    addTestTableSearchSegmentsToInstanceRequest(instanceRequest);
    try {
      QueryRequest queryRequest = new QueryRequest(instanceRequest, _serverInstance.getServerMetrics());
      DataTable instanceResponse = _queryExecutor.processQuery(queryRequest);
      System.out.println(instanceResponse.getDouble(0, 0));
      System.out.println(instanceResponse.getMetadata().get(DataTable.TIME_USED_MS_METADATA_KEY));
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testMinQuery() {
    BrokerRequest brokerRequest = getMinQuery();

    QuerySource querySource = new QuerySource();
    querySource.setTableName("testTable");
    brokerRequest.setQuerySource(querySource);
    InstanceRequest instanceRequest = new InstanceRequest(0, brokerRequest);
    addTestTableSearchSegmentsToInstanceRequest(instanceRequest);
    try {
      QueryRequest queryRequest = new QueryRequest(instanceRequest, _serverInstance.getServerMetrics());
      DataTable instanceResponse = _queryExecutor.processQuery(queryRequest);
      System.out.println(instanceResponse.getDouble(0, 0));
      System.out.println(instanceResponse.getMetadata().get(DataTable.TIME_USED_MS_METADATA_KEY));
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

  }

  private BrokerRequest getCountQuery() {
    BrokerRequest query = new BrokerRequest();
    AggregationInfo aggregationInfo = getCountAggregationInfo();
    List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(aggregationInfo);
    query.setAggregationsInfo(aggregationsInfo);
    return query;
  }

  private BrokerRequest getSumQuery() {
    BrokerRequest query = new BrokerRequest();
    AggregationInfo aggregationInfo = getSumAggregationInfo();
    List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(aggregationInfo);
    query.setAggregationsInfo(aggregationsInfo);
    return query;
  }

  private BrokerRequest getMaxQuery() {
    BrokerRequest query = new BrokerRequest();
    AggregationInfo aggregationInfo = getMaxAggregationInfo();
    List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(aggregationInfo);
    query.setAggregationsInfo(aggregationsInfo);
    return query;
  }

  private BrokerRequest getMinQuery() {
    BrokerRequest query = new BrokerRequest();
    AggregationInfo aggregationInfo = getMinAggregationInfo();
    List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(aggregationInfo);
    query.setAggregationsInfo(aggregationsInfo);
    return query;
  }

  private AggregationInfo getCountAggregationInfo() {
    String type = "count";
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", "met");

    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(type);
    aggregationInfo.setAggregationParams(params);
    return aggregationInfo;
  }

  private AggregationInfo getSumAggregationInfo() {
    String type = "sum";
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", "met");

    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(type);
    aggregationInfo.setAggregationParams(params);
    return aggregationInfo;
  }

  private AggregationInfo getMaxAggregationInfo() {
    String type = "max";
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", "met");

    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(type);
    aggregationInfo.setAggregationParams(params);
    return aggregationInfo;
  }

  private AggregationInfo getMinAggregationInfo() {
    String type = "min";
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", "met");

    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(type);
    aggregationInfo.setAggregationParams(params);
    return aggregationInfo;
  }
}
