package com.linkedin.pinot.server.integration;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.PatternLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.index.query.FilterQuery;
import com.linkedin.pinot.index.segment.IndexSegment;
import com.linkedin.pinot.index.segment.SegmentMetadata;
import com.linkedin.pinot.query.executor.QueryExecutor;
import com.linkedin.pinot.query.request.AggregationInfo;
import com.linkedin.pinot.query.request.Query;
import com.linkedin.pinot.query.request.Request;
import com.linkedin.pinot.query.response.InstanceResponse;
import com.linkedin.pinot.query.utils.IndexSegmentUtils;
import com.linkedin.pinot.server.instance.InstanceDataManager;
import com.linkedin.pinot.server.starter.ServerBuilder;
import com.linkedin.pinot.transport.netty.NettyServer.RequestHandlerFactory;


public class InstanceServerStarter {
  private static final Logger logger = LoggerFactory.getLogger(InstanceServerStarter.class);

  static
  {
    org.apache.log4j.Logger.getRootLogger().addAppender(new ConsoleAppender(
        new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN), "System.out"));
  }


  public static void main(String[] args) throws Exception {

    File confDir = new File(InstanceServerStarter.class.getClassLoader().getResource("conf").toURI());

    logger.info("Trying to build server config");
    ServerBuilder serverBuilder = new ServerBuilder(confDir.getAbsolutePath());

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

    System.out.println(getMaxQuery());
    String queryJson = "";
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
    AggregationInfo aggregationInfo = getCountAggregationInfo();
    List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(aggregationInfo);
    query.setAggregationsInfo(aggregationsInfo);
    FilterQuery filterQuery = getFilterQuery();
    query.setFilterQuery(filterQuery);
    return query;
  }

  private static Query getSumQuery() {
    Query query = new Query();
    AggregationInfo aggregationInfo = getSumAggregationInfo();
    List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(aggregationInfo);
    query.setAggregationsInfo(aggregationsInfo);
    FilterQuery filterQuery = getFilterQuery();
    query.setFilterQuery(filterQuery);
    return query;
  }

  private static Query getMaxQuery() {
    Query query = new Query();
    AggregationInfo aggregationInfo = getMaxAggregationInfo();
    List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(aggregationInfo);
    query.setAggregationsInfo(aggregationsInfo);
    FilterQuery filterQuery = getFilterQuery();
    query.setFilterQuery(filterQuery);
    return query;
  }

  private static Query getMinQuery() {
    Query query = new Query();
    AggregationInfo aggregationInfo = getMinAggregationInfo();
    List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(aggregationInfo);
    query.setAggregationsInfo(aggregationsInfo);
    FilterQuery filterQuery = getFilterQuery();
    query.setFilterQuery(filterQuery);
    return query;
  }

  private static FilterQuery getFilterQuery() {
    FilterQuery filterQuery = new FilterQuery();
    return filterQuery;
  }

  private static AggregationInfo getCountAggregationInfo()
  {
    String type = "count";
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", "met");
    return new AggregationInfo(type, params);
  }

  private static AggregationInfo getSumAggregationInfo()
  {
    String type = "sum";
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", "met");
    return new AggregationInfo(type, params);
  }

  private static AggregationInfo getMaxAggregationInfo()
  {
    String type = "max";
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", "met");
    return new AggregationInfo(type, params);
  }

  private static AggregationInfo getMinAggregationInfo()
  {
    String type = "min";
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", "met");
    return new AggregationInfo(type, params);
  }

  // Bootstrap some segments into instanceDataManger.
  private static void bootstrapSegments(InstanceDataManager instanceDataManager) {
    for (int i = 0; i < 2; ++i) {
      IndexSegment indexSegment = IndexSegmentUtils.getIndexSegmentWithAscendingOrderValues(20000001);
      SegmentMetadata segmentMetadata = indexSegment.getSegmentMetadata();
      //      segmentMetadata.setResourceName("midas");
      //      segmentMetadata.setTableName("testTable0");
//      indexSegment.setSegmentMetadata(segmentMetadata);
//      indexSegment.setSegmentName("index_" + i);
      instanceDataManager.getResourceDataManager("midas");
      instanceDataManager.getResourceDataManager("midas").getPartitionDataManager(0).addSegment(indexSegment);
    }
  }
}
