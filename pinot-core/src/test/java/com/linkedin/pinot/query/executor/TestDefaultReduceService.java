package com.linkedin.pinot.query.executor;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.query.ReduceService;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterQuery;
import com.linkedin.pinot.common.request.InstanceRequest;
import com.linkedin.pinot.common.request.QuerySource;
import com.linkedin.pinot.common.response.BrokerResponse;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.data.manager.InstanceDataManager;
import com.linkedin.pinot.core.data.manager.config.InstanceDataManagerConfig;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.query.executor.ServerQueryExecutorV1Impl;
import com.linkedin.pinot.core.query.reduce.DefaultReduceService;
import com.linkedin.pinot.core.query.utils.IndexSegmentUtils;


public class TestDefaultReduceService {
  private static ServerQueryExecutorV1Impl _queryExecutor;

  private static Logger LOGGER = LoggerFactory.getLogger(TestDefaultReduceService.class);
  public static final String PINOT_PROPERTIES = "pinot.properties";
  private static ReduceService _reduceService = new DefaultReduceService();

  @BeforeClass
  public static void setup() throws Exception {
    File confDir = new File(TestQueryExecutor.class.getClassLoader().getResource("conf").toURI());
    FileUtils.deleteDirectory(new File("/tmp/pinot/test1"));
    // ServerBuilder serverBuilder = new ServerBuilder(confDir.getAbsolutePath());
    String configFilePath = confDir.getAbsolutePath();

    // build _serverConf
    PropertiesConfiguration serverConf = new PropertiesConfiguration();
    serverConf.setDelimiterParsingDisabled(false);
    serverConf.load(new File(configFilePath, PINOT_PROPERTIES));

    InstanceDataManager instanceDataManager1 = InstanceDataManager.getInstanceDataManager();
    instanceDataManager1.init(new InstanceDataManagerConfig(serverConf.subset("pinot.server.instance")));
    instanceDataManager1.start();
    for (int i = 0; i < 2; ++i) {
      IndexSegment indexSegment =
          IndexSegmentUtils.getIndexSegmentWithAscendingOrderValues(20000001, "midas", "testTable");
      instanceDataManager1.getResourceDataManager("midas");
      instanceDataManager1.getResourceDataManager("midas").addSegment(indexSegment);
    }
    _queryExecutor = new ServerQueryExecutorV1Impl();
    _queryExecutor.init(serverConf.subset("pinot.server.query.executor"), instanceDataManager1);

  }

  @Test
  public void testCountQuery() {

    BrokerRequest brokerRequest = getCountQuery();

    QuerySource querySource = new QuerySource();
    querySource.setResourceName("midas");
    querySource.setTableName("testTable");
    brokerRequest.setQuerySource(querySource);
    InstanceRequest instanceRequest = new InstanceRequest(0, brokerRequest);

    Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    try {
      DataTable instanceResponse1 = _queryExecutor.processQuery(instanceRequest);
      instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse1);
      DataTable instanceResponse2 = _queryExecutor.processQuery(instanceRequest);
      instanceResponseMap.put(new ServerInstance("localhost:1111"), instanceResponse2);
      BrokerResponse brokerResponse = _reduceService.reduceOnDataTable(brokerRequest, instanceResponseMap);
      LOGGER.info("BrokerResponse is " + brokerResponse.getAggregationResults().get(0));
      Assert.assertEquals(brokerResponse.getAggregationResults().get(0).toString(),
          "{\"value\":\"80000004\",\"function\":\"count_star\"}");
      LOGGER.info("Time used for BrokerResponse is " + brokerResponse.getTimeUsedMs());
    } catch (Exception e) {
      e.printStackTrace();
      // Should never happen
      throw new RuntimeException(e.toString(), e);
      //Assert.assertEquals(true, false);
    }
  }

  @Test
  public void testSumQuery() {
    BrokerRequest brokerRequest = getSumQuery();

    QuerySource querySource = new QuerySource();
    querySource.setResourceName("midas");
    querySource.setTableName("testTable");
    brokerRequest.setQuerySource(querySource);
    InstanceRequest instanceRequest = new InstanceRequest(0, brokerRequest);

    Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    try {
      DataTable instanceResponse1 = _queryExecutor.processQuery(instanceRequest);
      instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse1);
      DataTable instanceResponse2 = _queryExecutor.processQuery(instanceRequest);
      instanceResponseMap.put(new ServerInstance("localhost:1111"), instanceResponse2);
      BrokerResponse brokerResponse = _reduceService.reduceOnDataTable(brokerRequest, instanceResponseMap);
      LOGGER.info("BrokerResponse is " + brokerResponse.getAggregationResults().get(0));
      Assert.assertEquals(brokerResponse.getAggregationResults().get(0).toString(),
          "{\"value\":\"800000040000000.00000\",\"function\":\"sum_met\"}");
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
    querySource.setResourceName("midas");
    querySource.setTableName("testTable");
    brokerRequest.setQuerySource(querySource);
    InstanceRequest instanceRequest = new InstanceRequest(0, brokerRequest);

    Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    try {
      DataTable instanceResponse1 = _queryExecutor.processQuery(instanceRequest);
      instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse1);
      DataTable instanceResponse2 = _queryExecutor.processQuery(instanceRequest);
      instanceResponseMap.put(new ServerInstance("localhost:1111"), instanceResponse2);
      BrokerResponse brokerResponse = _reduceService.reduceOnDataTable(brokerRequest, instanceResponseMap);
      LOGGER.info("BrokerResponse is " + brokerResponse.getAggregationResults().get(0));
      Assert.assertEquals(brokerResponse.getAggregationResults().get(0).toString(),
          "{\"value\":\"20000000.00000\",\"function\":\"max_met\"}");
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
    querySource.setResourceName("midas");
    querySource.setTableName("testTable");
    brokerRequest.setQuerySource(querySource);
    InstanceRequest instanceRequest = new InstanceRequest(0, brokerRequest);

    Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    try {
      DataTable instanceResponse1 = _queryExecutor.processQuery(instanceRequest);
      instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse1);
      DataTable instanceResponse2 = _queryExecutor.processQuery(instanceRequest);
      instanceResponseMap.put(new ServerInstance("localhost:1111"), instanceResponse2);
      BrokerResponse brokerResponse = _reduceService.reduceOnDataTable(brokerRequest, instanceResponseMap);
      LOGGER.info("BrokerResponse is " + brokerResponse.getAggregationResults().get(0));
      Assert.assertEquals(brokerResponse.getAggregationResults().get(0).toString(),
          "{\"value\":\"0.00000\",\"function\":\"min_met\"}");
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
    querySource.setResourceName("midas");
    querySource.setTableName("testTable");
    brokerRequest.setQuerySource(querySource);
    InstanceRequest instanceRequest = new InstanceRequest(0, brokerRequest);

    Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    try {
      DataTable instanceResponse1 = _queryExecutor.processQuery(instanceRequest);
      instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse1);
      DataTable instanceResponse2 = _queryExecutor.processQuery(instanceRequest);
      instanceResponseMap.put(new ServerInstance("localhost:1111"), instanceResponse2);

      BrokerResponse brokerResponse = _reduceService.reduceOnDataTable(brokerRequest, instanceResponseMap);
      LOGGER.info("BrokerResponse is " + brokerResponse.getAggregationResults().get(0));
      Assert.assertEquals(brokerResponse.getAggregationResults().get(0).toString(),
          "{\"value\":\"10000000.00000\",\"function\":\"avg_met\"}");
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
    querySource.setResourceName("midas");
    querySource.setTableName("testTable");
    brokerRequest.setQuerySource(querySource);
    InstanceRequest instanceRequest = new InstanceRequest(0, brokerRequest);

    Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    try {
      DataTable instanceResponse1 = _queryExecutor.processQuery(instanceRequest);
      instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse1);
      DataTable instanceResponse2 = _queryExecutor.processQuery(instanceRequest);
      instanceResponseMap.put(new ServerInstance("localhost:1111"), instanceResponse2);

      BrokerResponse brokerResponse = _reduceService.reduceOnDataTable(brokerRequest, instanceResponseMap);
      LOGGER.info("BrokerResponse is " + brokerResponse.getAggregationResults().get(0));
      Assert.assertEquals(brokerResponse.getAggregationResults().get(0).toString(),
          "{\"value\":\"10\",\"function\":\"distinctCount_dim0\"}");
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
    querySource.setResourceName("midas");
    querySource.setTableName("testTable");
    brokerRequest.setQuerySource(querySource);
    InstanceRequest instanceRequest = new InstanceRequest(0, brokerRequest);

    Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    try {
      DataTable instanceResponse1 = _queryExecutor.processQuery(instanceRequest);
      instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse1);
      DataTable instanceResponse2 = _queryExecutor.processQuery(instanceRequest);
      instanceResponseMap.put(new ServerInstance("localhost:1111"), instanceResponse2);

      BrokerResponse brokerResponse = _reduceService.reduceOnDataTable(brokerRequest, instanceResponseMap);
      LOGGER.info("BrokerResponse is " + brokerResponse.getAggregationResults().get(0));
      Assert.assertEquals(brokerResponse.getAggregationResults().get(0).toString(),
          "{\"value\":\"100\",\"function\":\"distinctCount_dim1\"}");
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
    querySource.setResourceName("midas");
    querySource.setTableName("testTable");
    brokerRequest.setQuerySource(querySource);
    InstanceRequest instanceRequest = new InstanceRequest(0, brokerRequest);

    Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    try {
      instanceResponseMap.put(new ServerInstance("localhost:0000"), _queryExecutor.processQuery(instanceRequest));
      instanceResponseMap.put(new ServerInstance("localhost:1111"), _queryExecutor.processQuery(instanceRequest));
      instanceResponseMap.put(new ServerInstance("localhost:2222"), _queryExecutor.processQuery(instanceRequest));
      instanceResponseMap.put(new ServerInstance("localhost:3333"), _queryExecutor.processQuery(instanceRequest));
      instanceResponseMap.put(new ServerInstance("localhost:4444"), _queryExecutor.processQuery(instanceRequest));
      instanceResponseMap.put(new ServerInstance("localhost:5555"), _queryExecutor.processQuery(instanceRequest));
      instanceResponseMap.put(new ServerInstance("localhost:6666"), _queryExecutor.processQuery(instanceRequest));
      instanceResponseMap.put(new ServerInstance("localhost:7777"), _queryExecutor.processQuery(instanceRequest));
      instanceResponseMap.put(new ServerInstance("localhost:8888"), _queryExecutor.processQuery(instanceRequest));
      instanceResponseMap.put(new ServerInstance("localhost:9999"), _queryExecutor.processQuery(instanceRequest));
      BrokerResponse brokerResponse = _reduceService.reduceOnDataTable(brokerRequest, instanceResponseMap);
      LOGGER.info("BrokerResponse is " + brokerResponse.getAggregationResults().get(0));
      Assert.assertEquals(brokerResponse.getAggregationResults().get(0).toString(),
          "{\"value\":\"400000020\",\"function\":\"count_star\"}");
      LOGGER.info("BrokerResponse is " + brokerResponse.getAggregationResults().get(1));
      Assert.assertEquals(brokerResponse.getAggregationResults().get(1).toString(),
          "{\"value\":\"4000000200000000.00000\",\"function\":\"sum_met\"}");
      LOGGER.info("BrokerResponse is " + brokerResponse.getAggregationResults().get(2));
      Assert.assertEquals(brokerResponse.getAggregationResults().get(2).toString(),
          "{\"value\":\"20000000.00000\",\"function\":\"max_met\"}");
      LOGGER.info("BrokerResponse is " + brokerResponse.getAggregationResults().get(3));
      Assert.assertEquals(brokerResponse.getAggregationResults().get(3).toString(),
          "{\"value\":\"0.00000\",\"function\":\"min_met\"}");
      LOGGER.info("BrokerResponse is " + brokerResponse.getAggregationResults().get(4));
      Assert.assertEquals(brokerResponse.getAggregationResults().get(4).toString(),
          "{\"value\":\"10000000.00000\",\"function\":\"avg_met\"}");
      LOGGER.info("BrokerResponse is " + brokerResponse.getAggregationResults().get(5));
      Assert.assertEquals(brokerResponse.getAggregationResults().get(5).toString(),
          "{\"value\":\"10\",\"function\":\"distinctCount_dim0\"}");
      LOGGER.info("BrokerResponse is " + brokerResponse.getAggregationResults().get(6));
      Assert.assertEquals(brokerResponse.getAggregationResults().get(6).toString(),
          "{\"value\":\"100\",\"function\":\"distinctCount_dim1\"}");
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
    FilterQuery filterQuery = getFilterQuery();
    query.setFilterQuery(filterQuery);
    return query;
  }

  private BrokerRequest getSumQuery() {
    BrokerRequest query = new BrokerRequest();
    AggregationInfo aggregationInfo = getSumAggregationInfo();
    List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(aggregationInfo);
    query.setAggregationsInfo(aggregationsInfo);
    FilterQuery filterQuery = getFilterQuery();
    query.setFilterQuery(filterQuery);
    return query;
  }

  private BrokerRequest getMaxQuery() {
    BrokerRequest query = new BrokerRequest();
    AggregationInfo aggregationInfo = getMaxAggregationInfo();
    List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(aggregationInfo);
    query.setAggregationsInfo(aggregationsInfo);
    FilterQuery filterQuery = getFilterQuery();
    query.setFilterQuery(filterQuery);
    return query;
  }

  private BrokerRequest getMinQuery() {
    BrokerRequest query = new BrokerRequest();
    AggregationInfo aggregationInfo = getMinAggregationInfo();
    List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(aggregationInfo);
    query.setAggregationsInfo(aggregationsInfo);
    FilterQuery filterQuery = getFilterQuery();
    query.setFilterQuery(filterQuery);
    return query;
  }

  private BrokerRequest getAvgQuery() {
    BrokerRequest query = new BrokerRequest();
    AggregationInfo aggregationInfo = getAvgAggregationInfo();
    List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(aggregationInfo);
    query.setAggregationsInfo(aggregationsInfo);
    FilterQuery filterQuery = getFilterQuery();
    query.setFilterQuery(filterQuery);
    return query;
  }

  private BrokerRequest getDistinctCountQuery(String dim) {
    BrokerRequest query = new BrokerRequest();
    AggregationInfo aggregationInfo = getDistinctCountAggregationInfo(dim);
    List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(aggregationInfo);
    query.setAggregationsInfo(aggregationsInfo);
    FilterQuery filterQuery = getFilterQuery();
    query.setFilterQuery(filterQuery);
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
    FilterQuery filterQuery = getFilterQuery();
    query.setFilterQuery(filterQuery);
    return query;
  }

  private FilterQuery getFilterQuery() {
    FilterQuery filterQuery = new FilterQuery();
    return null;
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
}
