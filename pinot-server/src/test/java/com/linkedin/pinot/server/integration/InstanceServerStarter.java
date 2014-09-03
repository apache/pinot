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

import com.linkedin.pinot.common.data.DataManager;
import com.linkedin.pinot.common.query.QueryExecutor;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterQuery;
import com.linkedin.pinot.common.request.InstanceRequest;
import com.linkedin.pinot.common.request.QuerySource;
import com.linkedin.pinot.common.response.InstanceResponse;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.server.starter.ServerBuilder;
import com.linkedin.pinot.transport.netty.NettyServer.RequestHandlerFactory;


public class InstanceServerStarter {
  private static final Logger logger = LoggerFactory.getLogger(InstanceServerStarter.class);

  static {
    org.apache.log4j.Logger.getRootLogger().addAppender(
        new ConsoleAppender(new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN), "System.out"));
  }

  public static void main(String[] args) throws Exception {

    File confDir = new File(InstanceServerStarter.class.getClassLoader().getResource("conf").toURI());

    logger.info("Trying to build server config");
    ServerBuilder serverBuilder = new ServerBuilder(confDir.getAbsolutePath());

    logger.info("Trying to build InstanceDataManager");
    final DataManager instanceDataManager = serverBuilder.buildInstanceDataManager();
    logger.info("Trying to start InstanceDataManager");
    instanceDataManager.start();
    //    bootstrapSegments(instanceDataManager);

    logger.info("Trying to build QueryExecutor");
    final QueryExecutor queryExecutor = serverBuilder.buildQueryExecutor(instanceDataManager);

    System.out.println(getCountQuery().toString());
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

  private static void sendQueryToQueryExecutor(BrokerRequest brokerRequest, QueryExecutor queryExecutor) {

    QuerySource querySource = new QuerySource();
    querySource.setResourceName("midas");
    querySource.setTableName("testTable");
    brokerRequest.setQuerySource(querySource);
    InstanceRequest instanceRequest = new InstanceRequest(0, brokerRequest);
    try {
      DataTable instanceResponse = queryExecutor.processQuery(instanceRequest);
      System.out.println(instanceResponse.toString());
      System.out.println("Query Time Used : " + instanceResponse.getMetadata().get("timeUsedMs"));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static BrokerRequest getCountQuery() {
    BrokerRequest query = new BrokerRequest();
    AggregationInfo aggregationInfo = getCountAggregationInfo();
    List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(aggregationInfo);
    query.setAggregationsInfo(aggregationsInfo);
    FilterQuery filterQuery = getFilterQuery();
    query.setFilterQuery(filterQuery);
    return query;
  }

  private static BrokerRequest getSumQuery() {
    BrokerRequest query = new BrokerRequest();
    AggregationInfo aggregationInfo = getSumAggregationInfo();
    List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(aggregationInfo);
    query.setAggregationsInfo(aggregationsInfo);
    FilterQuery filterQuery = getFilterQuery();
    query.setFilterQuery(filterQuery);
    return query;
  }

  private static BrokerRequest getMaxQuery() {
    BrokerRequest query = new BrokerRequest();
    AggregationInfo aggregationInfo = getMaxAggregationInfo();
    List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(aggregationInfo);
    query.setAggregationsInfo(aggregationsInfo);
    FilterQuery filterQuery = getFilterQuery();
    query.setFilterQuery(filterQuery);
    return query;
  }

  private static BrokerRequest getMinQuery() {
    BrokerRequest query = new BrokerRequest();
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

  private static AggregationInfo getCountAggregationInfo() {
    String type = "count";
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", "met");

    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(type);
    aggregationInfo.setAggregationParams(params);
    return aggregationInfo;
  }

  private static AggregationInfo getSumAggregationInfo() {
    String type = "sum";
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", "met");

    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(type);
    aggregationInfo.setAggregationParams(params);
    return aggregationInfo;
  }

  private static AggregationInfo getMaxAggregationInfo() {
    String type = "max";
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", "met");

    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(type);
    aggregationInfo.setAggregationParams(params);
    return aggregationInfo;
  }

  private static AggregationInfo getMinAggregationInfo() {
    String type = "min";
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", "met");

    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(type);
    aggregationInfo.setAggregationParams(params);
    return aggregationInfo;
  }
  //  // Bootstrap some segments into instanceDataManger.
  //  private static void bootstrapSegments(InstanceDataManager instanceDataManager) {
  //    for (int i = 0; i < 2; ++i) {
  //      IndexSegment indexSegment = IndexSegmentUtils.getIndexSegmentWithAscendingOrderValues(20000001);
  //      SegmentMetadata segmentMetadata = indexSegment.getSegmentMetadata();
  //      //      segmentMetadata.setResourceName("midas");
  //      //      segmentMetadata.setTableName("testTable0");
  //      //      indexSegment.setSegmentMetadata(segmentMetadata);
  //      //      indexSegment.setSegmentName("index_" + i);
  //      instanceDataManager.getResourceDataManager("midas");
  //      instanceDataManager.getResourceDataManager("midas").getPartitionDataManager(0).addSegment(indexSegment);
  //    }
  //  }
}
