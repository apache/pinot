package com.linkedin.pinot.query.executor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.linkedin.pinot.query.config.QueryExecutorConfig;
import com.linkedin.pinot.query.planner.ParallelQueryPlannerImpl;
import com.linkedin.pinot.query.planner.QueryPlan;
import com.linkedin.pinot.query.planner.QueryPlanner;
import com.linkedin.pinot.query.pruner.SegmentPrunerService;
import com.linkedin.pinot.query.pruner.SegmentPrunerServiceImpl;
import com.linkedin.pinot.query.request.Query;
import com.linkedin.pinot.query.request.Request;
import com.linkedin.pinot.query.response.Error;
import com.linkedin.pinot.query.response.InstanceResponse;
import com.linkedin.pinot.server.instance.InstanceDataManager;
import com.linkedin.pinot.server.partition.PartitionDataManager;
import com.linkedin.pinot.server.partition.SegmentDataManager;
import com.linkedin.pinot.server.resource.ResourceDataManager;
import com.linkedin.pinot.server.utils.NamedThreadFactory;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;


public class QueryExecutor {

  private static final String Domain = "com.linkedin.pinot";
  private QueryExecutorConfig _queryExecutorConfig = null;
  private InstanceDataManager _instanceDataManager = null;
  private SegmentPrunerService _segmentPrunerService = null;
  private QueryPlanner _queryPlanner = null;
  private PlanExecutor _planExecutor = null;
  private Timer _queryExecutorTimer = null;

  public QueryExecutor(QueryExecutorConfig queryExecutorConfig, InstanceDataManager instanceDataManager) {
    _queryExecutorConfig = queryExecutorConfig;
    _instanceDataManager = instanceDataManager;
    init();
  }

  public void init() {
    if (_segmentPrunerService == null) {
      _segmentPrunerService = new SegmentPrunerServiceImpl(_queryExecutorConfig.getPrunerConfig());
    }
    if (_queryPlanner == null) {
      _queryPlanner = new ParallelQueryPlannerImpl();
    }
    if (_planExecutor == null) {
      _planExecutor =
          new DefaultPlanExecutor(Executors.newCachedThreadPool(new NamedThreadFactory("plan-executor-global")));
    }
    if (_queryExecutorTimer == null) {
      _queryExecutorTimer =
          Metrics.newTimer(new MetricName(Domain, "timer", "query-executor-time-"), TimeUnit.MILLISECONDS,
              TimeUnit.SECONDS);
    }
  }

  public InstanceResponse processQuery(Request request) {
    long start = System.currentTimeMillis();
    final Query query = request.getQuery();
    List<SegmentDataManager> queryableSegmentDataManagerList = getPrunedQueryableSegments(query);
    final QueryPlan queryPlan = _queryPlanner.computeQueryPlan(query, queryableSegmentDataManagerList);

    InstanceResponse result = null;
    try {
      result = _queryExecutorTimer.time(new Callable<InstanceResponse>() {
        @Override
        public InstanceResponse call() throws Exception {
          return _planExecutor.ProcessQueryBasedOnPlan(query, queryPlan);
        }
      });
    } catch (Exception e) {
      result = new InstanceResponse();
      Error error = new Error();
      error.setError(250, e.getMessage());
      result.setError(error);
    }
    long end = System.currentTimeMillis();
    result.setTimeUsedMs(end - start);
    return result;
  }

  private List<SegmentDataManager> getPrunedQueryableSegments(Query query) {
    String resourceName = query.getResourceName();
    String tableName = query.getTableName();
    ResourceDataManager resourceDataManager = _instanceDataManager.getResourceDataManager(resourceName);
    if (resourceDataManager == null) {
      return null;
    }
    List<SegmentDataManager> queryableSegmentDataManagerList = new ArrayList<SegmentDataManager>();
    for (PartitionDataManager partitionDataManager : resourceDataManager.getPartitionDataManagerList()) {
      if (partitionDataManager == null || partitionDataManager.getTableDataManager(tableName) == null) {
        continue;
      }
      for (SegmentDataManager segmentDataManager : partitionDataManager.getTableDataManager(tableName)
          .getSegmentDataManagerList()) {
        if (!_segmentPrunerService.prune(segmentDataManager.getSegment(), query)) {
          queryableSegmentDataManagerList.add(segmentDataManager);
        }
      }
    }
    return queryableSegmentDataManagerList;
  }
}
