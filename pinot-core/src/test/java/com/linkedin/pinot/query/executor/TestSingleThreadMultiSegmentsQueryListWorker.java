package com.linkedin.pinot.query.executor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterQuery;
import com.linkedin.pinot.common.response.AggregationResult;
import com.linkedin.pinot.common.utils.NamedThreadFactory;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.query.executor.DefaultPlanExecutor;
import com.linkedin.pinot.core.query.executor.PlanExecutor;
import com.linkedin.pinot.core.query.planner.ParallelQueryPlannerImpl;
import com.linkedin.pinot.core.query.planner.QueryPlan;
import com.linkedin.pinot.core.query.planner.QueryPlanner;
import com.linkedin.pinot.core.query.utils.IndexSegmentUtils;


public class TestSingleThreadMultiSegmentsQueryListWorker {
  private static ExecutorService _globalExecutorService;

  @BeforeClass
  public static void setup() {
    //_globalExecutorService = Executors.newFixedThreadPool(20, new NamedThreadFactory("test-plan-executor-global"));
    _globalExecutorService = Executors.newCachedThreadPool(new NamedThreadFactory("test-plan-executor-global"));

  }

  @Test
  public void testCountQuery() throws Exception {
    int numDocsPerSegment = 20000001;
    int numSegments = 2;
    long startTime, endTime;

    List<IndexSegment> indexSegmentList = new ArrayList<IndexSegment>();
    for (int i = 0; i < numSegments; ++i) {
      indexSegmentList.add(IndexSegmentUtils.getIndexSegmentWithAscendingOrderValues(numDocsPerSegment));
    }

    startTime = System.currentTimeMillis();
    BrokerRequest query = getCountQuery();
    QueryPlanner queryPlanner = new ParallelQueryPlannerImpl();
    QueryPlan queryPlan = queryPlanner.computeQueryPlan(query, indexSegmentList);

    PlanExecutor planExecutor = new DefaultPlanExecutor(_globalExecutorService);

    List<AggregationResult> instanceResults =
        planExecutor.ProcessQueryBasedOnPlan(query, queryPlan).getAggregationResults();
    endTime = System.currentTimeMillis();
    System.out.println("Time used : " + (endTime - startTime));
    for (int j = 0; j < instanceResults.size(); ++j) {
      System.out.println(instanceResults.get(j).toString());
    }
  }

  @Test
  public void testSumQuery() throws Exception {
    int numDocsPerSegment = 20000001;
    int numSegments = 2;

    List<IndexSegment> indexSegmentList = new ArrayList<IndexSegment>();
    for (int i = 0; i < numSegments; ++i) {
      indexSegmentList.add(IndexSegmentUtils.getIndexSegmentWithAscendingOrderValues(numDocsPerSegment));
    }

    BrokerRequest query = getSumQuery();
    QueryPlanner queryPlanner = new ParallelQueryPlannerImpl();
    QueryPlan queryPlan = queryPlanner.computeQueryPlan(query, indexSegmentList);

    processQuery(indexSegmentList, query, queryPlan);

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

  private FilterQuery getFilterQuery() {
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

  private void processQuery(List<IndexSegment> indexSegmentList, BrokerRequest query, QueryPlan queryPlan)
      throws Exception {
    long startTime = System.currentTimeMillis();

    PlanExecutor planExecutor = new DefaultPlanExecutor(_globalExecutorService);

    List<AggregationResult> instanceResults =
        planExecutor.ProcessQueryBasedOnPlan(query, queryPlan).getAggregationResults();
    long endTime = System.currentTimeMillis();
    System.out.println("Time used : " + (endTime - startTime));
    for (int j = 0; j < instanceResults.size(); ++j) {
      System.out.println(instanceResults.get(j).toString());
    }
  }

}
