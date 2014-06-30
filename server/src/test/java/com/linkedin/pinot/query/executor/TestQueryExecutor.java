package com.linkedin.pinot.query.executor;

import java.io.File;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.BeforeClass;
import org.junit.Test;

import com.linkedin.pinot.index.query.FilterQuery;
import com.linkedin.pinot.index.segment.IndexSegment;
import com.linkedin.pinot.index.segment.SegmentMetadata;
import com.linkedin.pinot.query.request.Query;
import com.linkedin.pinot.query.request.Request;
import com.linkedin.pinot.query.response.InstanceResponse;
import com.linkedin.pinot.query.utils.IndexSegmentUtils;
import com.linkedin.pinot.server.instance.InstanceDataManager;
import com.linkedin.pinot.server.starter.ServerBuilder;


public class TestQueryExecutor {
  private static QueryExecutor _queryExecutor;

  @BeforeClass
  public static void setup() throws Exception {
    File confDir = new File(TestQueryExecutor.class.getClassLoader().getResource("conf").toURI());

    ServerBuilder serverBuilder = new ServerBuilder(confDir, null);

    final InstanceDataManager instanceDataManager = serverBuilder.buildInstanceDataManager();
    instanceDataManager.start();
    for (int i = 0; i < 2; ++i) {
      IndexSegment indexSegment = IndexSegmentUtils.getIndexSegmentWithAscendingOrderValues(20000001);
      SegmentMetadata segmentMetadata = indexSegment.getSegmentMetadata();
      segmentMetadata.setResourceName("midas");
      segmentMetadata.setTableName("testTable");
      indexSegment.setSegmentMetadata(segmentMetadata);
      indexSegment.setSegmentName("index_" + i);
      instanceDataManager.getResourceDataManager("midas");
      instanceDataManager.getResourceDataManager("midas").getPartitionDataManager(0).addSegment(indexSegment);

    }
    _queryExecutor = serverBuilder.buildQueryExecutor(instanceDataManager);

  }

  @Test
  public void testCountQuery() {

    Query query = getCountQuery();
    query.setResourceName("midas");
    query.setTableName("testTable");
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
    query.setResourceName("midas");
    query.setTableName("testTable");
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
    query.setResourceName("midas");
    query.setTableName("testTable");
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
    query.setResourceName("midas");
    query.setTableName("testTable");
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
    JSONArray aggregationJsonArray = getCountAggregationJsonArray();
    query.setAggregations(aggregationJsonArray);
    FilterQuery filterQuery = getFilterQuery();
    query.setFilterQuery(filterQuery);
    return query;
  }

  private Query getSumQuery() {
    Query query = new Query();
    JSONArray aggregationJsonArray = getSumAggregationJsonArray();
    query.setAggregations(aggregationJsonArray);
    FilterQuery filterQuery = getFilterQuery();
    query.setFilterQuery(filterQuery);
    return query;
  }

  private Query getMaxQuery() {
    Query query = new Query();
    JSONArray aggregationJsonArray = getMaxAggregationJsonArray();
    query.setAggregations(aggregationJsonArray);
    FilterQuery filterQuery = getFilterQuery();
    query.setFilterQuery(filterQuery);
    return query;
  }

  private Query getMinQuery() {
    Query query = new Query();
    JSONArray aggregationJsonArray = getMinAggregationJsonArray();
    query.setAggregations(aggregationJsonArray);
    FilterQuery filterQuery = getFilterQuery();
    query.setFilterQuery(filterQuery);
    return query;
  }

  private FilterQuery getFilterQuery() {
    FilterQuery filterQuery = new FilterQuery();
    return filterQuery;
  }

  private JSONArray getCountAggregationJsonArray() {
    JSONArray aggregationJsonArray = new JSONArray();
    JSONObject paramsJsonObject = new JSONObject();
    paramsJsonObject.put("column", "met");
    JSONObject functionJsonObject = new JSONObject();

    functionJsonObject.put("function", "count");
    functionJsonObject.put("params", paramsJsonObject);

    aggregationJsonArray.put(functionJsonObject);
    return aggregationJsonArray;
  }

  private JSONArray getSumAggregationJsonArray() {
    JSONArray aggregationJsonArray = new JSONArray();
    JSONObject paramsJsonObject = new JSONObject();
    paramsJsonObject.put("column", "met");
    JSONObject functionJsonObject = new JSONObject();

    functionJsonObject.put("function", "sum");
    functionJsonObject.put("params", paramsJsonObject);

    aggregationJsonArray.put(functionJsonObject);
    return aggregationJsonArray;
  }

  private JSONArray getMaxAggregationJsonArray() {
    JSONArray aggregationJsonArray = new JSONArray();
    JSONObject paramsJsonObject = new JSONObject();
    paramsJsonObject.put("column", "met");
    JSONObject functionJsonObject = new JSONObject();

    functionJsonObject.put("function", "max");
    functionJsonObject.put("params", paramsJsonObject);

    aggregationJsonArray.put(functionJsonObject);
    return aggregationJsonArray;
  }

  private JSONArray getMinAggregationJsonArray() {
    JSONArray aggregationJsonArray = new JSONArray();
    JSONObject paramsJsonObject = new JSONObject();
    paramsJsonObject.put("column", "met");
    JSONObject functionJsonObject = new JSONObject();

    functionJsonObject.put("function", "min");
    functionJsonObject.put("params", paramsJsonObject);

    aggregationJsonArray.put(functionJsonObject);
    return aggregationJsonArray;
  }

}
