package com.linkedin.pinot.server.integration;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import com.linkedin.pinot.common.request.FilterQuery;
import com.linkedin.pinot.common.request.InstanceRequest;
import com.linkedin.pinot.common.request.QuerySource;
import com.linkedin.pinot.common.response.InstanceResponse;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.data.manager.InstanceDataManager;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.query.utils.IndexSegmentUtils;
import com.linkedin.pinot.server.conf.ServerConf;
import com.linkedin.pinot.server.starter.ServerInstance;


public class IntegrationTest {

  private static Logger LOGGER = LoggerFactory.getLogger(IntegrationTest.class);

  public static final String PINOT_PROPERTIES = "pinot.properties";

  private static ServerConf _serverConf;
  private static ServerInstance _serverInstance;
  private static QueryExecutor _queryExecutor;

  @BeforeTest
  public static void setUp() throws Exception {
    //Process Command Line to get config and port
    FileUtils.deleteDirectory(new File("/tmp/pinot/test1"));
    File confDir = new File(InstanceServerStarter.class.getClassLoader().getResource("conf").toURI());
    File confFile = new File(confDir, PINOT_PROPERTIES);
    // build _serverConf
    PropertiesConfiguration serverConf = new PropertiesConfiguration();
    serverConf.setDelimiterParsingDisabled(false);
    serverConf.load(confFile);
    _serverConf = new ServerConf(serverConf);

    LOGGER.info("Trying to create a new ServerInstance!");
    _serverInstance = new ServerInstance();
    LOGGER.info("Trying to initial ServerInstance!");
    _serverInstance.init(_serverConf);
    LOGGER.info("Trying to start ServerInstance!");
    _serverInstance.start();
    _queryExecutor = _serverInstance.getQueryExecutor();

    InstanceDataManager instanceDataManager = (InstanceDataManager) _serverInstance.getInstanceDataManager();
    for (int i = 0; i < 2; ++i) {
      IndexSegment indexSegment = IndexSegmentUtils.getIndexSegmentWithAscendingOrderValues(20000001);
      SegmentMetadata segmentMetadata = indexSegment.getSegmentMetadata();
      //      segmentMetadata.setResourceName("midas");
      //      segmentMetadata.setTableName("testTable");
      //      indexSegment.setSegmentMetadata(segmentMetadata);
      //      indexSegment.setSegmentName("index_" + i);
      instanceDataManager.getResourceDataManager("midas");
      instanceDataManager.getResourceDataManager("midas").addSegment(indexSegment);
    }

  }

  @AfterTest
  public static void Shutdown() {
    _serverInstance.shutDown();
  }

  @Test
  public void testWvmpQuery() {

    BrokerRequest brokerRequest = getCountQuery();
    brokerRequest.setFilterQuery(null);
    brokerRequest.setFilterQueryIsSet(false);
    QuerySource querySource = new QuerySource();
    querySource.setResourceName("wvmp");
    querySource.setTableName("testWvmpTable1");
    brokerRequest.setQuerySource(querySource);
    InstanceRequest instanceRequest = new InstanceRequest(0, brokerRequest);

    try {
      InstanceResponse instanceResponse = _queryExecutor.processQuery(instanceRequest);
      if (instanceResponse.getExceptions() == null) {
        System.out.println(instanceResponse.getAggregationResults().get(0).toString());
      } else {
        System.out.println(instanceResponse.getExceptions().get(0).getErrorCode());
      }
      System.out.println(instanceResponse);
      
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Test
  public void testCountQuery() {

    BrokerRequest brokerRequest = getCountQuery();

    QuerySource querySource = new QuerySource();
    querySource.setResourceName("midas");
    querySource.setTableName("testTable");
    brokerRequest.setQuerySource(querySource);
    InstanceRequest instanceRequest = new InstanceRequest(0, brokerRequest);
    try {
      InstanceResponse instanceResponse = _queryExecutor.processQuery(instanceRequest);
      if (instanceResponse.getExceptions() == null) {
        System.out.println(instanceResponse.getAggregationResults().get(0).toString());
      } else {
        System.out.println(instanceResponse.getExceptions().get(0).getErrorCode());
      }
      System.out.println(instanceResponse.getTimeUsedMs());
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
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
    try {
      InstanceResponse instanceResponse = _queryExecutor.processQuery(instanceRequest);
      if (instanceResponse.getExceptions() == null) {
        System.out.println(instanceResponse.getAggregationResults().get(0).toString());
      } else {
        System.out.println(instanceResponse.getExceptions().get(0).getErrorCode());
      }
      System.out.println(instanceResponse.getTimeUsedMs());
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
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
    try {
      InstanceResponse instanceResponse = _queryExecutor.processQuery(instanceRequest);
      if (instanceResponse.getExceptions() == null) {
        System.out.println(instanceResponse.getAggregationResults().get(0).toString());
      } else {
        System.out.println(instanceResponse.getExceptions().get(0).getErrorCode());
      }
      System.out.println(instanceResponse.getTimeUsedMs());
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
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
    try {
      InstanceResponse instanceResponse = _queryExecutor.processQuery(instanceRequest);
      if (instanceResponse.getExceptions() == null) {
        System.out.println(instanceResponse.getAggregationResults().get(0).toString());
      } else {
        System.out.println(instanceResponse.getExceptions().get(0).getErrorCode());
      }
      System.out.println(instanceResponse.getTimeUsedMs());
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
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

  private FilterQuery getFilterQuery() {
    FilterQuery filterQuery = new FilterQuery();
    return filterQuery;
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
