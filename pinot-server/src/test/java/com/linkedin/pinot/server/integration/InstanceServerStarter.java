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
package com.linkedin.pinot.server.integration;

import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.query.QueryRequest;
import com.linkedin.pinot.core.query.scheduler.QueryScheduler;
import com.yammer.metrics.core.MetricsRegistry;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.data.DataManager;
import com.linkedin.pinot.common.query.QueryExecutor;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterQuery;
import com.linkedin.pinot.common.request.InstanceRequest;
import com.linkedin.pinot.common.request.QuerySource;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.server.starter.ServerBuilder;
import com.linkedin.pinot.transport.netty.NettyServer.RequestHandlerFactory;


public class InstanceServerStarter {
  private static final Logger LOGGER = LoggerFactory.getLogger(InstanceServerStarter.class);

  public static void main(String[] args) throws Exception {

    File confDir = new File(InstanceServerStarter.class.getClassLoader().getResource("conf").toURI());

    LOGGER.info("Trying to build server config");
    MetricsRegistry metricsRegistry = new MetricsRegistry();
    ServerBuilder serverBuilder = new ServerBuilder(confDir.getAbsolutePath(), metricsRegistry);

    LOGGER.info("Trying to build InstanceDataManager");
    final DataManager instanceDataManager = serverBuilder.buildInstanceDataManager();
    LOGGER.info("Trying to start InstanceDataManager");
    instanceDataManager.start();
    //    bootstrapSegments(instanceDataManager);

    LOGGER.info("Trying to build QueryExecutor");
    final QueryExecutor queryExecutor = serverBuilder.buildQueryExecutor(instanceDataManager);

    System.out.println(getCountQuery().toString());
    sendQueryToQueryExecutor(getCountQuery(), queryExecutor, serverBuilder.getServerMetrics());
    sendQueryToQueryExecutor(getSumQuery(), queryExecutor, serverBuilder.getServerMetrics());
    sendQueryToQueryExecutor(getMaxQuery(), queryExecutor, serverBuilder.getServerMetrics());
    sendQueryToQueryExecutor(getMinQuery(), queryExecutor, serverBuilder.getServerMetrics());

    LOGGER.info("Trying to build RequestHandlerFactory");
    QueryScheduler queryScheduler = serverBuilder.buildQueryScheduler(queryExecutor);
    RequestHandlerFactory simpleRequestHandlerFactory = serverBuilder.buildRequestHandlerFactory(queryScheduler);
    LOGGER.info("Trying to build NettyServer");

    System.out.println(getMaxQuery());
    String queryJson = "";
  }

  private static void sendQueryToQueryExecutor(BrokerRequest brokerRequest, QueryExecutor queryExecutor, ServerMetrics metrics) {

    QuerySource querySource = new QuerySource();
    querySource.setTableName("midas");
    brokerRequest.setQuerySource(querySource);
    InstanceRequest instanceRequest = new InstanceRequest(0, brokerRequest);
    try {
      QueryRequest queryRequest = new QueryRequest(instanceRequest, metrics);
      DataTable instanceResponse = queryExecutor.processQuery(queryRequest);
      System.out.println(instanceResponse.toString());
      System.out.println("Query Time Used : " + instanceResponse.getMetadata().get(DataTable.TIME_USED_MS_METADATA_KEY));
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
}
