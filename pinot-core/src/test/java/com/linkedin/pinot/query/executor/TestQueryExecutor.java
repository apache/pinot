package com.linkedin.pinot.query.executor;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.query.request.AggregationInfo;
import com.linkedin.pinot.common.query.request.FilterQuery;
import com.linkedin.pinot.common.query.request.Query;
import com.linkedin.pinot.common.query.request.Request;
import com.linkedin.pinot.common.query.response.InstanceResponse;
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

    Query query = getCountQuery();
    query.setSourceName("midas.testTable");
    Request request = new Request();
    request.setQuery(query);
    request.setRequestId(0);
    try {
      InstanceResponse instanceResponse = _queryExecutor.processQuery(request);
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
    Query query = getSumQuery();
    query.setSourceName("midas.testTable");
    Request request = new Request();
    request.setQuery(query);
    request.setRequestId(0);
    try {
      InstanceResponse instanceResponse = _queryExecutor.processQuery(request);
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
    Query query = getMaxQuery();
    query.setSourceName("midas.testTable");
    Request request = new Request();
    request.setQuery(query);
    request.setRequestId(0);
    try {
      InstanceResponse instanceResponse = _queryExecutor.processQuery(request);
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
    Query query = getMinQuery();
    query.setSourceName("midas.testTable");
    Request request = new Request();
    request.setQuery(query);
    request.setRequestId(0);
    try {
      InstanceResponse instanceResponse = _queryExecutor.processQuery(request);
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

  private Query getCountQuery() {
    Query query = new Query();
    AggregationInfo aggregationInfo = getCountAggregationInfo();
    List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(aggregationInfo);
    query.setAggregationsInfo(aggregationsInfo);
    FilterQuery filterQuery = getFilterQuery();
    query.setFilterQuery(filterQuery);
    return query;
  }

  private Query getSumQuery() {
    Query query = new Query();
    AggregationInfo aggregationInfo = getSumAggregationInfo();
    List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(aggregationInfo);
    query.setAggregationsInfo(aggregationsInfo);
    FilterQuery filterQuery = getFilterQuery();
    query.setFilterQuery(filterQuery);
    return query;
  }

  private Query getMaxQuery() {
    Query query = new Query();
    AggregationInfo aggregationInfo = getMaxAggregationInfo();
    List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(aggregationInfo);
    query.setAggregationsInfo(aggregationsInfo);
    FilterQuery filterQuery = getFilterQuery();
    query.setFilterQuery(filterQuery);
    return query;
  }

  private Query getMinQuery() {
    Query query = new Query();
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
    return new AggregationInfo(type, params);
  }

  private AggregationInfo getSumAggregationInfo() {
    String type = "sum";
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", "met");
    return new AggregationInfo(type, params);
  }

  private AggregationInfo getMaxAggregationInfo() {
    String type = "max";
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", "met");
    return new AggregationInfo(type, params);
  }

  private AggregationInfo getMinAggregationInfo() {
    String type = "min";
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", "met");
    return new AggregationInfo(type, params);
  }
}
