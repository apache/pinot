package com.linkedin.pinot.query.executor;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.query.response.InstanceResponse;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterQuery;
import com.linkedin.pinot.common.request.InstanceRequest;
import com.linkedin.pinot.common.request.QuerySource;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.data.manager.InstanceDataManager;
import com.linkedin.pinot.core.data.manager.config.InstanceDataManagerConfig;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.query.executor.ServerQueryExecutor;
import com.linkedin.pinot.core.query.utils.IndexSegmentUtils;


public class TestQueryExecutor {
  private static ServerQueryExecutor _queryExecutor;

  public static final String PINOT_PROPERTIES = "pinot.properties";

  @BeforeClass
  public static void setup() throws Exception {
    File confDir = new File(TestQueryExecutor.class.getClassLoader().getResource("conf").toURI());

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
      IndexSegment indexSegment = IndexSegmentUtils.getIndexSegmentWithAscendingOrderValues(20000001);
      SegmentMetadata segmentMetadata = indexSegment.getSegmentMetadata();
      //      segmentMetadata.setResourceName("midas");
      //      segmentMetadata.setTableName("testTable");
      //      indexSegment.setSegmentMetadata(segmentMetadata);
      //      indexSegment.setSegmentName("index_" + i);
      instanceDataManager.getResourceDataManager("midas");
      instanceDataManager.getResourceDataManager("midas").getPartitionDataManager(0).addSegment(indexSegment);

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
      if (instanceResponse.getError() == null) {
        System.out.println(instanceResponse.getAggregationResults().get(0).get(0).toString());
      } else {
        System.out.println(instanceResponse.getError().getErrorMessage(0));
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
      if (instanceResponse.getError() == null) {
        System.out.println(instanceResponse.getAggregationResults().get(0).get(0).toString());
      } else {
        System.out.println(instanceResponse.getError().getErrorMessage(0));
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
      if (instanceResponse.getError() == null) {
        System.out.println(instanceResponse.getAggregationResults().get(0).get(0).toString());
      } else {
        System.out.println(instanceResponse.getError().getErrorMessage(0));
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
      if (instanceResponse.getError() == null) {
        System.out.println(instanceResponse.getAggregationResults().get(0).get(0).toString());
      } else {
        System.out.println(instanceResponse.getError().getErrorMessage(0));
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
