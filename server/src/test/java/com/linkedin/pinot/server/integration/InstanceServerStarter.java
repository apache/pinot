package com.linkedin.pinot.server.integration;

import java.io.File;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.index.query.FilterQuery;
import com.linkedin.pinot.index.segment.IndexSegment;
import com.linkedin.pinot.index.segment.SegmentMetadata;
import com.linkedin.pinot.query.executor.QueryExecutor;
import com.linkedin.pinot.query.request.Query;
import com.linkedin.pinot.query.request.Request;
import com.linkedin.pinot.query.response.InstanceResponse;
import com.linkedin.pinot.query.utils.IndexSegmentUtils;
import com.linkedin.pinot.server.instance.InstanceDataManager;
import com.linkedin.pinot.server.starter.ServerBuilder;
import com.linkedin.pinot.transport.netty.NettyServer;
import com.linkedin.pinot.transport.netty.NettyServer.RequestHandlerFactory;
import com.linkedin.pinot.transport.netty.NettyTCPServer;


public class InstanceServerStarter {
  private static final Logger logger = LoggerFactory.getLogger(InstanceServerStarter.class);

  public static void main(String[] args) throws Exception {

    File confDir = new File(InstanceServerStarter.class.getClassLoader().getResource("conf").toURI());

    logger.info("Trying to build server config");
    ServerBuilder serverBuilder = new ServerBuilder(confDir, null);

    logger.info("Trying to build InstanceDataManager");
    final InstanceDataManager instanceDataManager = serverBuilder.buildInstanceDataManager();
    logger.info("Trying to start InstanceDataManager");
    instanceDataManager.start();
    bootstrapSegments(instanceDataManager);
    logger.info("Trying to build QueryExecutor");
    final QueryExecutor queryExecutor = serverBuilder.buildQueryExecutor(instanceDataManager);

    sendQueryToQueryExecutor(getCountQuery(), queryExecutor);
    sendQueryToQueryExecutor(getSumQuery(), queryExecutor);
    sendQueryToQueryExecutor(getMaxQuery(), queryExecutor);
    sendQueryToQueryExecutor(getMinQuery(), queryExecutor);

    logger.info("Trying to build RequestHandlerFactory");
    RequestHandlerFactory simpleRequestHandlerFactory = serverBuilder.buildRequestHandlerFactory(queryExecutor);
    logger.info("Trying to build NettyServer");
    NettyServer nettyServer = new NettyTCPServer(9999, simpleRequestHandlerFactory, null);
    nettyServer.run();

  }

  private static void sendQueryToQueryExecutor(Query query, QueryExecutor queryExecutor) {
    query.setResourceName("midas");
    query.setTableName("testTable0");
    Request request = new Request();
    request.setQuery(query);
    request.setRequestId(0);
    try {
      InstanceResponse instanceResponse = queryExecutor.processQuery(request);
      if (instanceResponse.getError() == null) {
        System.out.println(instanceResponse.getAggregationResults().get(0).get(0).toString());
      } else {
        System.out.println(instanceResponse.getError().getErrorMessage(0));
      }
      System.out.println(instanceResponse.getTimeUsedMs());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static Query getCountQuery() {
    Query query = new Query();
    JSONArray aggregationJsonArray = getCountAggregationJsonArray();
    query.setAggregations(aggregationJsonArray);
    FilterQuery filterQuery = getFilterQuery();
    query.setFilterQuery(filterQuery);
    return query;
  }

  private static Query getSumQuery() {
    Query query = new Query();
    JSONArray aggregationJsonArray = getSumAggregationJsonArray();
    query.setAggregations(aggregationJsonArray);
    FilterQuery filterQuery = getFilterQuery();
    query.setFilterQuery(filterQuery);
    return query;
  }

  private static Query getMaxQuery() {
    Query query = new Query();
    JSONArray aggregationJsonArray = getMaxAggregationJsonArray();
    query.setAggregations(aggregationJsonArray);
    FilterQuery filterQuery = getFilterQuery();
    query.setFilterQuery(filterQuery);
    return query;
  }

  private static Query getMinQuery() {
    Query query = new Query();
    JSONArray aggregationJsonArray = getMinAggregationJsonArray();
    query.setAggregations(aggregationJsonArray);
    FilterQuery filterQuery = getFilterQuery();
    query.setFilterQuery(filterQuery);
    return query;
  }

  private static FilterQuery getFilterQuery() {
    FilterQuery filterQuery = new FilterQuery();
    return filterQuery;
  }

  private static JSONArray getCountAggregationJsonArray() {
    JSONArray aggregationJsonArray = new JSONArray();
    JSONObject paramsJsonObject = new JSONObject();
    paramsJsonObject.put("column", "met");
    JSONObject functionJsonObject = new JSONObject();

    functionJsonObject.put("function", "count");
    functionJsonObject.put("params", paramsJsonObject);

    aggregationJsonArray.put(functionJsonObject);
    return aggregationJsonArray;
  }

  private static JSONArray getSumAggregationJsonArray() {
    JSONArray aggregationJsonArray = new JSONArray();
    JSONObject paramsJsonObject = new JSONObject();
    paramsJsonObject.put("column", "met");
    JSONObject functionJsonObject = new JSONObject();

    functionJsonObject.put("function", "sum");
    functionJsonObject.put("params", paramsJsonObject);

    aggregationJsonArray.put(functionJsonObject);
    return aggregationJsonArray;
  }

  private static JSONArray getMaxAggregationJsonArray() {
    JSONArray aggregationJsonArray = new JSONArray();
    JSONObject paramsJsonObject = new JSONObject();
    paramsJsonObject.put("column", "met");
    JSONObject functionJsonObject = new JSONObject();

    functionJsonObject.put("function", "max");
    functionJsonObject.put("params", paramsJsonObject);

    aggregationJsonArray.put(functionJsonObject);
    return aggregationJsonArray;
  }

  private static JSONArray getMinAggregationJsonArray() {
    JSONArray aggregationJsonArray = new JSONArray();
    JSONObject paramsJsonObject = new JSONObject();
    paramsJsonObject.put("column", "met");
    JSONObject functionJsonObject = new JSONObject();

    functionJsonObject.put("function", "min");
    functionJsonObject.put("params", paramsJsonObject);

    aggregationJsonArray.put(functionJsonObject);
    return aggregationJsonArray;
  }

  // Bootstrap some segments into instanceDataManger.
  private static void bootstrapSegments(InstanceDataManager instanceDataManager) {
    for (int i = 0; i < 2; ++i) {
      IndexSegment indexSegment = IndexSegmentUtils.getIndexSegmentWithAscendingOrderValues(20000001);
      SegmentMetadata segmentMetadata = indexSegment.getSegmentMetadata();
      segmentMetadata.setResourceName("midas");
      segmentMetadata.setTableName("testTable0");
      indexSegment.setSegmentMetadata(segmentMetadata);
      indexSegment.setSegmentName("index_" + i);
      instanceDataManager.getResourceDataManager("midas");
      instanceDataManager.getResourceDataManager("midas").getPartitionDataManager(0).addSegment(indexSegment);

    }
  }
}
