package com.linkedin.pinot.query.executor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.BeforeClass;
import org.junit.Test;

import com.linkedin.pinot.core.query.FilterQuery;
import com.linkedin.pinot.query.aggregation.AggregationResult;
import com.linkedin.pinot.query.planner.ParallelQueryPlannerImpl;
import com.linkedin.pinot.query.planner.QueryPlan;
import com.linkedin.pinot.query.planner.QueryPlanner;
import com.linkedin.pinot.query.request.AggregationInfo;
import com.linkedin.pinot.query.request.Query;
import com.linkedin.pinot.query.utils.IndexSegmentUtils;
import com.linkedin.pinot.server.partition.SegmentDataManager;
import com.linkedin.pinot.server.utils.NamedThreadFactory;


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

    List<SegmentDataManager> segmentDataManagers = new ArrayList<SegmentDataManager>();
    for (int i = 0; i < numSegments; ++i) {
      segmentDataManagers.add(getSegmentDataManager(numDocsPerSegment));
    }

    startTime = System.currentTimeMillis();
    Query query = getCountQuery();
    QueryPlanner queryPlanner = new ParallelQueryPlannerImpl();
    QueryPlan queryPlan = queryPlanner.computeQueryPlan(query, segmentDataManagers);

    PlanExecutor planExecutor = new DefaultPlanExecutor(_globalExecutorService);

    List<List<AggregationResult>> instanceResults =
        planExecutor.ProcessQueryBasedOnPlan(query, queryPlan).getAggregationResults();
    endTime = System.currentTimeMillis();
    System.out.println("Time used : " + (endTime - startTime));
    for (int j = 0; j < instanceResults.size(); ++j) {
      System.out.println(instanceResults.get(j).get(0).toString());
    }
  }

  @Test
  public void testSumQuery() throws Exception {
    int numDocsPerSegment = 20000001;
    int numSegments = 2;

    List<SegmentDataManager> segmentDataManagers = new ArrayList<SegmentDataManager>();
    for (int i = 0; i < numSegments; ++i) {
      segmentDataManagers.add(getSegmentDataManager(numDocsPerSegment));
    }

    Query query = getSumQuery();
    QueryPlanner queryPlanner = new ParallelQueryPlannerImpl();
    QueryPlan queryPlan = queryPlanner.computeQueryPlan(query, segmentDataManagers);

    processQuery(segmentDataManagers, query, queryPlan);

  }

  private SegmentDataManager getSegmentDataManager(int numberOfDocs) {
    SegmentDataManager segmentDataManager =
        new SegmentDataManager(IndexSegmentUtils.getIndexSegmentWithAscendingOrderValues(numberOfDocs));
    return segmentDataManager;
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

  private FilterQuery getFilterQuery() {
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

  private void processQuery(List<SegmentDataManager> segmentDataManagers, Query query, QueryPlan queryPlan)
      throws Exception {
    long startTime = System.currentTimeMillis();

    PlanExecutor planExecutor = new DefaultPlanExecutor(_globalExecutorService);

    List<List<AggregationResult>> instanceResults =
        planExecutor.ProcessQueryBasedOnPlan(query, queryPlan).getAggregationResults();
    long endTime = System.currentTimeMillis();
    System.out.println("Time used : " + (endTime - startTime));
    for (int j = 0; j < instanceResults.size(); ++j) {
      System.out.println(instanceResults.get(j).get(0).toString());
    }
  }

}
