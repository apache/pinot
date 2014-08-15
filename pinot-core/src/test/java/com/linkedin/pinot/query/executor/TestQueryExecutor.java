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

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterQuery;
import com.linkedin.pinot.common.request.InstanceRequest;
import com.linkedin.pinot.common.request.QuerySource;
import com.linkedin.pinot.common.response.InstanceResponse;
import com.linkedin.pinot.core.data.manager.InstanceDataManager;
import com.linkedin.pinot.core.data.manager.config.InstanceDataManagerConfig;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.query.executor.ServerQueryExecutor;
import com.linkedin.pinot.core.query.utils.IndexSegmentUtils;


public class TestQueryExecutor {
  private static ServerQueryExecutor _queryExecutor;

  private static Logger LOGGER = LoggerFactory.getLogger(TestQueryExecutor.class);
  public static final String PINOT_PROPERTIES = "pinot.properties";

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

    InstanceDataManager instanceDataManager = InstanceDataManager.getInstanceDataManager();
    instanceDataManager.init(new InstanceDataManagerConfig(serverConf.subset("pinot.server.instance")));
    instanceDataManager.start();
    for (int i = 0; i < 2; ++i) {
      IndexSegment indexSegment =
          IndexSegmentUtils.getIndexSegmentWithAscendingOrderValues(20000001, "midas", "testTable");
      instanceDataManager.getResourceDataManager("midas");
      instanceDataManager.getResourceDataManager("midas").addSegment(indexSegment);
    }
    _queryExecutor = new ServerQueryExecutor();
    _queryExecutor.init(serverConf.subset("pinot.server.query.executor"), instanceDataManager);
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
        if (instanceResponse.getAggregationResults() != null && instanceResponse.getAggregationResults().size() > 0) {
          LOGGER.info("InstanceResponse is " + instanceResponse.getAggregationResults().get(0).toString());
          Assert.assertEquals(instanceResponse.getAggregationResults().get(0).getLongVal(), 40000002L);
        }
      } else {
        LOGGER.error("Get exception - " + instanceResponse.getExceptions().get(0).getErrorCode() + " : "
            + instanceResponse.getExceptions().get(0).getMessage());
        // Should never happen
        Assert.assertEquals(true, false);
      }
      LOGGER.info("Time used for instanceResponse is " + instanceResponse.getTimeUsedMs());
    } catch (Exception e) {
      e.printStackTrace();
      // Should never happen
      Assert.assertEquals(true, false);
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
        if (instanceResponse.getAggregationResults() != null && instanceResponse.getAggregationResults().size() > 0) {
          LOGGER.info("InstanceResponse is " + instanceResponse.getAggregationResults().get(0).toString());
          Assert.assertEquals(instanceResponse.getAggregationResults().get(0).getLongVal(), 400000020000000L);
        }
      } else {
        LOGGER.error("Get exception - " + instanceResponse.getExceptions().get(0).getErrorCode() + " : "
            + instanceResponse.getExceptions().get(0).getMessage());
        // Should never happen
        Assert.assertEquals(true, false);
      }
      LOGGER.info("Time used for instanceResponse is " + instanceResponse.getTimeUsedMs());
    } catch (Exception e) {
      e.printStackTrace();
      // Should never happen
      Assert.assertEquals(true, false);
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
        if (instanceResponse.getAggregationResults() != null && instanceResponse.getAggregationResults().size() > 0) {
          LOGGER.info("InstanceResponse is " + instanceResponse.getAggregationResults().get(0).toString());
          Assert.assertEquals(instanceResponse.getAggregationResults().get(0).getDoubleVal(), 20000000.0);
        }
      } else {
        LOGGER.error("Get exception - " + instanceResponse.getExceptions().get(0).getErrorCode() + " : "
            + instanceResponse.getExceptions().get(0).getMessage());
        // Should never happen
        Assert.assertEquals(true, false);
      }
      LOGGER.info("Time used for instanceResponse is " + instanceResponse.getTimeUsedMs());
    } catch (Exception e) {
      e.printStackTrace();
      // Should never happen
      Assert.assertEquals(true, false);
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
        if (instanceResponse.getAggregationResults() != null && instanceResponse.getAggregationResults().size() > 0) {
          LOGGER.info("InstanceResponse is " + instanceResponse.getAggregationResults().get(0).toString());
          Assert.assertEquals(instanceResponse.getAggregationResults().get(0).getDoubleVal(), 0.0);
        }
      } else {
        LOGGER.error("Get exception - " + instanceResponse.getExceptions().get(0).getErrorCode() + " : "
            + instanceResponse.getExceptions().get(0).getMessage());
        // Should never happen
        Assert.assertEquals(true, false);
      }
      LOGGER.info("Time used for instanceResponse is " + instanceResponse.getTimeUsedMs());
    } catch (Exception e) {
      e.printStackTrace();
      // Should never happen
      Assert.assertEquals(true, false);
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
