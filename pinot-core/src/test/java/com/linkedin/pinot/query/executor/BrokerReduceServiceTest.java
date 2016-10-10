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
package com.linkedin.pinot.query.executor;

import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.query.QueryRequest;
import com.linkedin.pinot.common.query.ReduceService;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.InstanceRequest;
import com.linkedin.pinot.common.request.QuerySource;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.response.broker.AggregationResult;
import com.linkedin.pinot.common.response.broker.BrokerResponseNative;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.data.manager.config.FileBasedInstanceDataManagerConfig;
import com.linkedin.pinot.core.data.manager.offline.FileBasedInstanceDataManager;
import com.linkedin.pinot.core.data.manager.offline.TableDataManagerProvider;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.query.executor.ServerQueryExecutorV1Impl;
import com.linkedin.pinot.core.query.reduce.BrokerReduceService;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;
import com.linkedin.pinot.util.TestUtils;
import com.yammer.metrics.core.MetricsRegistry;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class BrokerReduceServiceTest {

  private final String SMALL_AVRO_DATA = "data/simpleData200001.avro";
  private static File INDEXES_DIR =
      new File(FileUtils.getTempDirectory() + File.separator + "TestReduceServiceList");

  private List<IndexSegment> _indexSegmentList = new ArrayList<IndexSegment>();

  private static ServerQueryExecutorV1Impl _queryExecutor;

  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerReduceServiceTest.class);
  public static final String PINOT_PROPERTIES = "pinot.properties";
  private static ReduceService<BrokerResponseNative> _reduceService = new BrokerReduceService();

  @BeforeClass
  public void setup()
      throws Exception {
    TableDataManagerProvider.setServerMetrics(new ServerMetrics(new MetricsRegistry()));

    File confDir = new File(QueryExecutorTest.class.getClassLoader().getResource("conf").toURI());
    setupSegmentList(2);
    FileUtils.deleteDirectory(new File("/tmp/pinot/test1"));
    // ServerBuilder serverBuilder = new ServerBuilder(confDir.getAbsolutePath());
    String configFilePath = confDir.getAbsolutePath();

    // build _serverConf
    PropertiesConfiguration serverConf = new PropertiesConfiguration();
    serverConf.setDelimiterParsingDisabled(false);
    serverConf.load(new File(configFilePath, PINOT_PROPERTIES));

    FileBasedInstanceDataManager instanceDataManager1 = FileBasedInstanceDataManager.getInstanceDataManager();
    instanceDataManager1.init(new FileBasedInstanceDataManagerConfig(serverConf.subset("pinot.server.instance")));
    instanceDataManager1.start();
    for (int i = 0; i < 2; ++i) {
      instanceDataManager1.getTableDataManager("midas");
      instanceDataManager1.getTableDataManager("midas").addSegment(_indexSegmentList.get(i));
    }
    _queryExecutor = new ServerQueryExecutorV1Impl();
    _queryExecutor.init(serverConf.subset("pinot.server.query.executor"), instanceDataManager1,
        new ServerMetrics(new MetricsRegistry()));
  }

  @AfterClass
  public void tearDown() {
    if (INDEXES_DIR.exists()) {
      FileUtils.deleteQuietly(INDEXES_DIR);
    }
    for (IndexSegment segment : _indexSegmentList) {
      segment.destroy();
    }
    _indexSegmentList.clear();
  }

  private void setupSegmentList(int numberOfSegments)
      throws Exception {
    final String filePath = TestUtils.getFileFromResourceUrl(getClass().getClassLoader().getResource(SMALL_AVRO_DATA));
    _indexSegmentList.clear();
    if (INDEXES_DIR.exists()) {
      FileUtils.deleteQuietly(INDEXES_DIR);
    }
    INDEXES_DIR.mkdir();

    for (int i = 0; i < numberOfSegments; ++i) {
      final File segmentDir = new File(INDEXES_DIR, "segment_" + i);

      final SegmentGeneratorConfig config =
          SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), segmentDir, "dim" + i,
              TimeUnit.DAYS, "midas");
      config.setSegmentNamePostfix(String.valueOf(i));
      final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
      driver.init(config);
      driver.build();
      File parent = new File(INDEXES_DIR, "segment_" + String.valueOf(i));
      String segmentName = parent.list()[0];
      _indexSegmentList.add(ColumnarSegmentLoader.load(new File(parent, segmentName), ReadMode.mmap));
      System.out.println("built at : " + segmentDir.getAbsolutePath());
    }
  }

  @Test
  public void testCountQuery() {

    BrokerRequest brokerRequest = getCountQuery();

    QuerySource querySource = new QuerySource();
    querySource.setTableName("midas");

    brokerRequest.setQuerySource(querySource);
    InstanceRequest instanceRequest = new InstanceRequest(0, brokerRequest);
    instanceRequest.setSearchSegments(new ArrayList<String>());

    for (IndexSegment segment : _indexSegmentList) {
      instanceRequest.addToSearchSegments(segment.getSegmentName());
    }

    Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    QueryRequest queryRequest = new QueryRequest(instanceRequest, TableDataManagerProvider.getServerMetrics());
    DataTable instanceResponse1 = _queryExecutor.processQuery(queryRequest);
    instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse1);
    DataTable instanceResponse2 = _queryExecutor.processQuery(queryRequest);
    instanceResponseMap.put(new ServerInstance("localhost:1111"), instanceResponse2);
    BrokerResponseNative brokerResponse = _reduceService.reduceOnDataTable(brokerRequest, instanceResponseMap);
    AggregationResult aggregationResult = brokerResponse.getAggregationResults().get(0);
    LOGGER.info("BrokerResponse is " + aggregationResult);
    checkAggregationResult(aggregationResult, "count_star", 800004.0);
    LOGGER.info("Time used for BrokerResponse is " + brokerResponse.getTimeUsedMs());
  }

  @Test
  public void testSumQuery() {
    BrokerRequest brokerRequest = getSumQuery();

    QuerySource querySource = new QuerySource();
    querySource.setTableName("midas");

    brokerRequest.setQuerySource(querySource);
    InstanceRequest instanceRequest = new InstanceRequest(0, brokerRequest);
    instanceRequest.setSearchSegments(new ArrayList<String>());
    for (IndexSegment segment : _indexSegmentList) {
      instanceRequest.addToSearchSegments(segment.getSegmentName());
    }

    Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    try {
      QueryRequest queryRequest = new QueryRequest(instanceRequest, TableDataManagerProvider.getServerMetrics());
      DataTable instanceResponse1 = _queryExecutor.processQuery(queryRequest);
      instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse1);
      DataTable instanceResponse2 = _queryExecutor.processQuery(queryRequest);
      instanceResponseMap.put(new ServerInstance("localhost:1111"), instanceResponse2);
      BrokerResponseNative brokerResponse = _reduceService.reduceOnDataTable(brokerRequest, instanceResponseMap);
      LOGGER.info("BrokerResponse is " + brokerResponse.getAggregationResults().get(0));
      LOGGER.info("Time used for BrokerResponse is " + brokerResponse.getTimeUsedMs());
    } catch (Exception e) {
      e.printStackTrace();
      // Should never happen
      throw new RuntimeException(e.toString(), e);
    }
  }

  @Test
  public void testMaxQuery() {

    BrokerRequest brokerRequest = getMaxQuery();

    QuerySource querySource = new QuerySource();
    querySource.setTableName("midas");

    brokerRequest.setQuerySource(querySource);
    InstanceRequest instanceRequest = new InstanceRequest(0, brokerRequest);
    instanceRequest.setSearchSegments(new ArrayList<String>());
    for (IndexSegment segment : _indexSegmentList) {
      instanceRequest.addToSearchSegments(segment.getSegmentName());
    }

    Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    try {
      QueryRequest queryRequest = new QueryRequest(instanceRequest, TableDataManagerProvider.getServerMetrics());
      DataTable instanceResponse1 = _queryExecutor.processQuery(queryRequest);
      instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse1);
      DataTable instanceResponse2 = _queryExecutor.processQuery(queryRequest);
      instanceResponseMap.put(new ServerInstance("localhost:1111"), instanceResponse2);
      BrokerResponseNative brokerResponse = _reduceService.reduceOnDataTable(brokerRequest, instanceResponseMap);
      LOGGER.info("BrokerResponse is " + brokerResponse.getAggregationResults().get(0));
      LOGGER.info("Time used for BrokerResponse is " + brokerResponse.getTimeUsedMs());
    } catch (Exception e) {
      e.printStackTrace();
      // Should never happen
      throw new RuntimeException(e.toString(), e);
    }
  }

  @Test
  public void testMinQuery() {
    BrokerRequest brokerRequest = getMinQuery();

    QuerySource querySource = new QuerySource();
    querySource.setTableName("midas");

    brokerRequest.setQuerySource(querySource);
    InstanceRequest instanceRequest = new InstanceRequest(0, brokerRequest);
    instanceRequest.setSearchSegments(new ArrayList<String>());
    for (IndexSegment segment : _indexSegmentList) {
      instanceRequest.addToSearchSegments(segment.getSegmentName());
    }

    Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    try {
      QueryRequest queryRequest = new QueryRequest(instanceRequest, TableDataManagerProvider.getServerMetrics());
      DataTable instanceResponse1 = _queryExecutor.processQuery(queryRequest);
      instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse1);
      DataTable instanceResponse2 = _queryExecutor.processQuery(queryRequest);
      instanceResponseMap.put(new ServerInstance("localhost:1111"), instanceResponse2);
      BrokerResponseNative brokerResponse =
          (BrokerResponseNative) _reduceService.reduceOnDataTable(brokerRequest, instanceResponseMap);
      LOGGER.info("BrokerResponse is " + brokerResponse.getAggregationResults().get(0));
      LOGGER.info("Time used for BrokerResponse is " + brokerResponse.getTimeUsedMs());
    } catch (Exception e) {
      e.printStackTrace();
      // Should never happen
      throw new RuntimeException(e.toString(), e);
    }
  }

  @Test
  public void testAvgQuery() {
    BrokerRequest brokerRequest = getAvgQuery();

    QuerySource querySource = new QuerySource();
    querySource.setTableName("midas");

    brokerRequest.setQuerySource(querySource);
    InstanceRequest instanceRequest = new InstanceRequest(0, brokerRequest);
    instanceRequest.setSearchSegments(new ArrayList<String>());
    for (IndexSegment segment : _indexSegmentList) {
      instanceRequest.addToSearchSegments(segment.getSegmentName());
    }

    Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    try {
      QueryRequest queryRequest = new QueryRequest(instanceRequest, TableDataManagerProvider.getServerMetrics());
      DataTable instanceResponse1 = _queryExecutor.processQuery(queryRequest);
      instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse1);
      DataTable instanceResponse2 = _queryExecutor.processQuery(queryRequest);
      instanceResponseMap.put(new ServerInstance("localhost:1111"), instanceResponse2);

      BrokerResponseNative brokerResponse = _reduceService.reduceOnDataTable(brokerRequest, instanceResponseMap);
      AggregationResult aggregationResult = brokerResponse.getAggregationResults().get(0);
      LOGGER.info("BrokerResponse is " + aggregationResult);
      checkAggregationResult(aggregationResult, "avg_met", 100000.0);
      LOGGER.info("Time used for BrokerResponse is " + brokerResponse.getTimeUsedMs());
    } catch (Exception e) {
      e.printStackTrace();
      // Should never happen
      throw new RuntimeException(e.toString(), e);
    }
  }

  @Test
  public void testDistinctCountQuery0() {
    BrokerRequest brokerRequest = getDistinctCountQuery("dim0");

    QuerySource querySource = new QuerySource();
    querySource.setTableName("midas");

    brokerRequest.setQuerySource(querySource);
    InstanceRequest instanceRequest = new InstanceRequest(0, brokerRequest);
    instanceRequest.setSearchSegments(new ArrayList<String>());
    for (IndexSegment segment : _indexSegmentList) {
      instanceRequest.addToSearchSegments(segment.getSegmentName());
    }

    Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    try {
      QueryRequest queryRequest = new QueryRequest(instanceRequest, TableDataManagerProvider.getServerMetrics());
      DataTable instanceResponse1 = _queryExecutor.processQuery(queryRequest);
      instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse1);
      DataTable instanceResponse2 = _queryExecutor.processQuery(queryRequest);
      instanceResponseMap.put(new ServerInstance("localhost:1111"), instanceResponse2);

      BrokerResponseNative brokerResponse = _reduceService.reduceOnDataTable(brokerRequest, instanceResponseMap);
      AggregationResult aggregationResult = brokerResponse.getAggregationResults().get(0);
      LOGGER.info("BrokerResponse is " + aggregationResult);
      checkAggregationResult(aggregationResult, "distinctCount_dim0", 10.0);

      LOGGER.info("Time used for BrokerResponse is " + brokerResponse.getTimeUsedMs());
    } catch (Exception e) {
      e.printStackTrace();
      // Should never happen
      throw new RuntimeException(e.toString(), e);
    }
  }

  @Test
  public void testDistinctCountQuery1() {
    BrokerRequest brokerRequest = getDistinctCountQuery("dim1");

    QuerySource querySource = new QuerySource();
    querySource.setTableName("midas");

    brokerRequest.setQuerySource(querySource);
    InstanceRequest instanceRequest = new InstanceRequest(0, brokerRequest);
    instanceRequest.setSearchSegments(new ArrayList<String>());
    for (IndexSegment segment : _indexSegmentList) {
      instanceRequest.addToSearchSegments(segment.getSegmentName());
    }

    Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    try {
      QueryRequest queryRequest = new QueryRequest(instanceRequest, TableDataManagerProvider.getServerMetrics());
      DataTable instanceResponse1 = _queryExecutor.processQuery(queryRequest);
      instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse1);
      DataTable instanceResponse2 = _queryExecutor.processQuery(queryRequest);
      instanceResponseMap.put(new ServerInstance("localhost:1111"), instanceResponse2);

      BrokerResponseNative brokerResponse = _reduceService.reduceOnDataTable(brokerRequest, instanceResponseMap);
      AggregationResult aggregationResult = brokerResponse.getAggregationResults().get(0);
      LOGGER.info("BrokerResponse is " + aggregationResult);
      checkAggregationResult(aggregationResult, "distinctCount_dim1", 100.0);

      LOGGER.info("Time used for BrokerResponse is " + brokerResponse.getTimeUsedMs());
    } catch (Exception e) {
      e.printStackTrace();
      // Should never happen
      throw new RuntimeException(e.toString(), e);
    }
  }

  @Test
  public void testMultiAggregationQuery() {
    BrokerRequest brokerRequest = getMultiAggregationQuery();

    QuerySource querySource = new QuerySource();
    querySource.setTableName("midas");

    brokerRequest.setQuerySource(querySource);
    InstanceRequest instanceRequest = new InstanceRequest(0, brokerRequest);
    instanceRequest.setSearchSegments(new ArrayList<String>());
    for (IndexSegment segment : _indexSegmentList) {
      instanceRequest.addToSearchSegments(segment.getSegmentName());
    }

    Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    try {
      QueryRequest queryRequest = new QueryRequest(instanceRequest, TableDataManagerProvider.getServerMetrics());
      instanceResponseMap.put(new ServerInstance("localhost:0000"), _queryExecutor.processQuery(queryRequest));
      instanceResponseMap.put(new ServerInstance("localhost:1111"), _queryExecutor.processQuery(queryRequest));
      instanceResponseMap.put(new ServerInstance("localhost:2222"), _queryExecutor.processQuery(queryRequest));
      instanceResponseMap.put(new ServerInstance("localhost:3333"), _queryExecutor.processQuery(queryRequest));
      instanceResponseMap.put(new ServerInstance("localhost:4444"), _queryExecutor.processQuery(queryRequest));
      instanceResponseMap.put(new ServerInstance("localhost:5555"), _queryExecutor.processQuery(queryRequest));
      instanceResponseMap.put(new ServerInstance("localhost:6666"), _queryExecutor.processQuery(queryRequest));
      instanceResponseMap.put(new ServerInstance("localhost:7777"), _queryExecutor.processQuery(queryRequest));
      instanceResponseMap.put(new ServerInstance("localhost:8888"), _queryExecutor.processQuery(queryRequest));
      instanceResponseMap.put(new ServerInstance("localhost:9999"), _queryExecutor.processQuery(queryRequest));
      BrokerResponseNative brokerResponse = _reduceService.reduceOnDataTable(brokerRequest, instanceResponseMap);

      AggregationResult aggregationResult = brokerResponse.getAggregationResults().get(0);
      LOGGER.info("BrokerResponse is " + aggregationResult);
      checkAggregationResult(aggregationResult, "count_star", 4000020.0);

      aggregationResult = brokerResponse.getAggregationResults().get(1);
      LOGGER.info("BrokerResponse is " + aggregationResult);
      checkAggregationResult(aggregationResult, "sum_met", 400002000000.0);

      aggregationResult = brokerResponse.getAggregationResults().get(2);
      LOGGER.info("BrokerResponse is " + aggregationResult);
      checkAggregationResult(aggregationResult, "max_met", 200000.0);

      aggregationResult = brokerResponse.getAggregationResults().get(3);
      LOGGER.info("BrokerResponse is " + aggregationResult);
      checkAggregationResult(aggregationResult, "min_met", 0.0);

      aggregationResult = brokerResponse.getAggregationResults().get(4);
      LOGGER.info("BrokerResponse is " + aggregationResult);
      checkAggregationResult(aggregationResult, "avg_met", 100000.0);

      aggregationResult = brokerResponse.getAggregationResults().get(5);
      LOGGER.info("BrokerResponse is " + aggregationResult);
      checkAggregationResult(aggregationResult, "distinctCount_dim0", 10.0);

      aggregationResult = brokerResponse.getAggregationResults().get(6);
      LOGGER.info("BrokerResponse is " + aggregationResult);
      checkAggregationResult(aggregationResult, "distinctCount_dim1", 100.0);

      LOGGER.info("Time Used for BrokerResponse is " + brokerResponse.getTimeUsedMs());
      LOGGER.info("Num Docs Scanned is " + brokerResponse.getNumDocsScanned());
      LOGGER.info("Total Docs for BrokerResponse is " + brokerResponse.getTotalDocs());

      System.out.println(brokerResponse.toJson());
    } catch (Exception e) {
      e.printStackTrace();
      // Should never happen
      throw new RuntimeException(e.toString(), e);
    }
  }

  private BrokerRequest getCountQuery() {
    BrokerRequest query = new BrokerRequest();
    AggregationInfo aggregationInfo = getCountAggregationInfo();
    List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(aggregationInfo);
    query.setAggregationsInfo(aggregationsInfo);
    query.setFilterQuery(null);
    return query;
  }

  private BrokerRequest getSumQuery() {
    BrokerRequest query = new BrokerRequest();
    AggregationInfo aggregationInfo = getSumAggregationInfo();
    List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(aggregationInfo);
    query.setAggregationsInfo(aggregationsInfo);
    query.setFilterQuery(null);
    return query;
  }

  private BrokerRequest getMaxQuery() {
    BrokerRequest query = new BrokerRequest();
    AggregationInfo aggregationInfo = getMaxAggregationInfo();
    List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(aggregationInfo);
    query.setAggregationsInfo(aggregationsInfo);
    query.setFilterQuery(null);
    return query;
  }

  private BrokerRequest getMinQuery() {
    BrokerRequest query = new BrokerRequest();
    AggregationInfo aggregationInfo = getMinAggregationInfo();
    List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(aggregationInfo);
    query.setAggregationsInfo(aggregationsInfo);
    query.setFilterQuery(null);
    return query;
  }

  private BrokerRequest getAvgQuery() {
    BrokerRequest query = new BrokerRequest();
    AggregationInfo aggregationInfo = getAvgAggregationInfo();
    List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(aggregationInfo);
    query.setAggregationsInfo(aggregationsInfo);
    query.setFilterQuery(null);
    return query;
  }

  private BrokerRequest getDistinctCountQuery(String dim) {
    BrokerRequest query = new BrokerRequest();
    AggregationInfo aggregationInfo = getDistinctCountAggregationInfo(dim);
    List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(aggregationInfo);
    query.setAggregationsInfo(aggregationsInfo);
    query.setFilterQuery(null);
    return query;
  }

  private BrokerRequest getMultiAggregationQuery() {
    BrokerRequest query = new BrokerRequest();
    List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(getCountAggregationInfo());
    aggregationsInfo.add(getSumAggregationInfo());
    aggregationsInfo.add(getMaxAggregationInfo());
    aggregationsInfo.add(getMinAggregationInfo());
    aggregationsInfo.add(getAvgAggregationInfo());
    aggregationsInfo.add(getDistinctCountAggregationInfo("dim0"));
    aggregationsInfo.add(getDistinctCountAggregationInfo("dim1"));
    query.setAggregationsInfo(aggregationsInfo);
    query.setFilterQuery(null);
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

  private AggregationInfo getAvgAggregationInfo() {
    String type = "avg";
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", "met");

    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(type);
    aggregationInfo.setAggregationParams(params);
    return aggregationInfo;
  }

  private AggregationInfo getDistinctCountAggregationInfo(String dim) {
    String type = "distinctCount";
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", dim);

    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(type);
    aggregationInfo.setAggregationParams(params);
    return aggregationInfo;
  }

  /**
   * Check aggregation result, and assert that actual values are the same as expectedValue values.
   * @param aggregationResult Aggregation result to be checked
   * @param expectedFuncName Expected function name
   * @param expectedValue Expected value
   */
  private static void checkAggregationResult(AggregationResult aggregationResult, String expectedFuncName,
      double expectedValue) {
    Assert.assertEquals(aggregationResult.getFunction(), expectedFuncName);
    Assert.assertEquals(Double.valueOf(aggregationResult.getValue().toString()), expectedValue);
  }
}
