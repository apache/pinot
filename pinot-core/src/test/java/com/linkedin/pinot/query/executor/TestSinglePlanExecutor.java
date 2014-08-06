package com.linkedin.pinot.query.executor;

import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.linkedin.pinot.common.query.response.AggregationResult;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterQuery;
import com.linkedin.pinot.common.utils.NamedThreadFactory;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionFactory;
import com.linkedin.pinot.core.query.aggregation.CombineLevel;
import com.linkedin.pinot.core.query.aggregation.CombineReduceService;
import com.linkedin.pinot.core.query.executor.SingleThreadMultiSegmentsWorker;
import com.linkedin.pinot.core.query.utils.IndexSegmentUtils;


public class TestSinglePlanExecutor {
  private static ExecutorService _globalExecutorService;

  @BeforeClass
  public static void setup() {
    //_globalExecutorService = Executors.newFixedThreadPool(20, new NamedThreadFactory("test-plan-executor-global"));
    _globalExecutorService = Executors.newCachedThreadPool(new NamedThreadFactory("test-plan-executor-global"));

  }

  @Test
  public void testCountQuery() {
    int numDocsPerSegment = 20000001;
    int numJobs = 4;
    int numSegmentsPerJob = 1;
    BrokerRequest query = getCountQuery();
    List<List<IndexSegment>> indexSegmentsList = new ArrayList<List<IndexSegment>>();
    new ArrayList<IndexSegment>();

    for (int i = 0; i < numJobs; ++i) {
      indexSegmentsList.add(new ArrayList<IndexSegment>());
      for (int j = 0; j < numSegmentsPerJob; ++j) {
        indexSegmentsList.get(i).add(IndexSegmentUtils.getIndexSegmentWithAscendingOrderValues(numDocsPerSegment));
      }
    }
    // long startTime = System.currentTimeMillis();
    long endTime = 0;
    List<Future<List<List<AggregationResult>>>> producerJobs = new ArrayList<Future<List<List<AggregationResult>>>>();
    for (int i = 0; i < numJobs; ++i) {
      producerJobs.add(_globalExecutorService.submit(new SingleThreadMultiSegmentsWorker(i, indexSegmentsList.get(i),
          query)));
    }
    List<List<AggregationResult>> instanceResults = new ArrayList<List<AggregationResult>>();
    for (int i = 0; i < query.getAggregationsInfo().size(); ++i) {
      instanceResults.add(new ArrayList<AggregationResult>());
    }
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < numJobs; ++i) {
      try {
        List<List<AggregationResult>> segmentResults = producerJobs.get(i).get(100000, TimeUnit.MILLISECONDS);
        for (int j = 0; j < segmentResults.size(); ++j) {
          instanceResults.get(j).addAll(segmentResults.get(j));
        }
        endTime = System.currentTimeMillis();
        System.out.println("Time used : " + (endTime - startTime));
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (ExecutionException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (TimeoutException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    endTime = System.currentTimeMillis();
    CombineReduceService.combine(AggregationFunctionFactory.getAggregationFunction(query), instanceResults,
        CombineLevel.INSTANCE);

    System.out.println("Time used : " + (endTime - startTime));
    for (int j = 0; j < instanceResults.size(); ++j) {
      System.out.println(instanceResults.get(j).get(0).toString());
    }
  }

  @Test
  public void testSumQuery() {
    int numDocsPerSegment = 20000001;
    int numJobs = 1;
    int numSegmentsPerJob = 8;

    BrokerRequest query = getSumQuery();
    List<List<IndexSegment>> indexSegmentsList = new ArrayList<List<IndexSegment>>();
    new ArrayList<IndexSegment>();

    for (int i = 0; i < numJobs; ++i) {
      indexSegmentsList.add(new ArrayList<IndexSegment>());
      for (int j = 0; j < numSegmentsPerJob; ++j) {
        indexSegmentsList.get(i).add(IndexSegmentUtils.getIndexSegmentWithAscendingOrderValues(numDocsPerSegment));
      }
    }
    long startTime = System.currentTimeMillis();
    long endTime = 0;
    List<Future<List<List<AggregationResult>>>> producerJobs = new ArrayList<Future<List<List<AggregationResult>>>>();
    for (int i = 0; i < numJobs; ++i) {
      producerJobs.add(_globalExecutorService.submit(new SingleThreadMultiSegmentsWorker(i, indexSegmentsList.get(i),
          query)));
    }
    List<List<AggregationResult>> instanceResults = new ArrayList<List<AggregationResult>>();
    for (int i = 0; i < query.getAggregationsInfo().size(); ++i) {
      instanceResults.add(new ArrayList<AggregationResult>());
    }

    for (int i = 0; i < numJobs; ++i) {
      try {
        List<List<AggregationResult>> segmentResults = producerJobs.get(i).get(100000, TimeUnit.MILLISECONDS);
        for (int j = 0; j < segmentResults.size(); ++j) {
          instanceResults.get(j).addAll(segmentResults.get(j));
        }
        endTime = System.currentTimeMillis();
        System.out.println("Time used : " + (endTime - startTime));
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (ExecutionException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (TimeoutException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    endTime = System.currentTimeMillis();
    CombineReduceService.combine(AggregationFunctionFactory.getAggregationFunction(query), instanceResults,
        CombineLevel.INSTANCE);

    System.out.println("Total Time used : " + (endTime - startTime));
    for (int j = 0; j < instanceResults.size(); ++j) {
      System.out.println(instanceResults.get(j).get(0).toString());
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

}
