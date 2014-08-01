package com.linkedin.pinot.core.query.executor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.linkedin.pinot.common.query.request.Query;
import com.linkedin.pinot.common.query.response.AggregationResult;
import com.linkedin.pinot.common.query.response.InstanceResponse;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionFactory;
import com.linkedin.pinot.core.query.aggregation.CombineLevel;
import com.linkedin.pinot.core.query.aggregation.CombineReduceService;
import com.linkedin.pinot.core.query.planner.JobVertex;
import com.linkedin.pinot.core.query.planner.QueryPlan;


/**
 * DefaultPlanExecutor will take a queryPlan and query, then compute the results.
 * 
 * @author xiafu
 *
 */
public class DefaultPlanExecutor implements PlanExecutor {

  private final int _timeout = 8000;
  private ExecutorService _globalExecutorService = null;

  public DefaultPlanExecutor(ExecutorService globalExecutorService) {
    this._globalExecutorService = globalExecutorService;
  }

  @Override
  public InstanceResponse ProcessQueryBasedOnPlan(final Query query, QueryPlan queryPlan) throws Exception {

    List<JobVertex> currentRootVertexList = new ArrayList<JobVertex>();
    List<JobVertex> availableToSubmitJobVertexList = new ArrayList<JobVertex>();
    currentRootVertexList.add(queryPlan.getVirtualRoot());
    availableToSubmitJobVertexList.add(queryPlan.getVirtualRoot());

    markJobVertexAsSubmitted(availableToSubmitJobVertexList, queryPlan.getVirtualRoot());
    markJobVertexAsFinished(currentRootVertexList, availableToSubmitJobVertexList, queryPlan.getVirtualRoot());

    ArrayList<Future<List<List<AggregationResult>>>> futureJobList =
        new ArrayList<Future<List<List<AggregationResult>>>>();
    Map<Future<List<List<AggregationResult>>>, JobVertex> futureToJobVertexMap =
        new HashMap<Future<List<List<AggregationResult>>>, JobVertex>();
    List<List<AggregationResult>> aggregationResultsList = null;
    long startTime = System.currentTimeMillis();
    while (!currentRootVertexList.isEmpty()) {
      submitJobs(availableToSubmitJobVertexList, futureJobList, futureToJobVertexMap, queryPlan, query);
      for (Future<List<List<AggregationResult>>> future : futureJobList) {
        try {
          if (aggregationResultsList == null) {
            aggregationResultsList =
                future.get((startTime + _timeout) - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
            markJobVertexAsFinished(currentRootVertexList, availableToSubmitJobVertexList,
                futureToJobVertexMap.get(future));
          } else {
            List<List<AggregationResult>> tempResultsList =
                future.get((startTime + _timeout) - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
            markJobVertexAsFinished(currentRootVertexList, availableToSubmitJobVertexList,
                futureToJobVertexMap.get(future));
            for (int j = 0; j < aggregationResultsList.size(); ++j) {
              aggregationResultsList.get(j).addAll(tempResultsList.get(j));
            }
          }
        } catch (ExecutionException e) {
          throw new ExecutionException(e);
        } catch (TimeoutException e) {
          throw new TimeoutException("Time out for query: " + query.toString());
        }
      }
      try {
        Thread.sleep(10);
      } catch (Exception e) {
      }
    }

    List<List<AggregationResult>> instanceResultsList =
        CombineReduceService.combine(AggregationFunctionFactory.getAggregationFunction(query), aggregationResultsList,
            CombineLevel.INSTANCE);
    InstanceResponse instancePinotResult = new InstanceResponse();
    instancePinotResult.setAggregationResults(instanceResultsList);
    return instancePinotResult;
  }

  public void submitJobs(List<JobVertex> availableVertexList,
      ArrayList<Future<List<List<AggregationResult>>>> futureJobList,
      Map<Future<List<List<AggregationResult>>>, JobVertex> futureToJobMap, QueryPlan queryPlan, Query query) {

    while (availableVertexList.size() > 0) {
      final JobVertex jobVertex = availableVertexList.get(0);
      futureJobList.add(_globalExecutorService
          .submit(new SingleThreadMultiSegmentsWorker(0, jobVertex.getIndexSegmentList(), query)));
      futureToJobMap.put(futureJobList.get(futureJobList.size() - 1), jobVertex);
      markJobVertexAsSubmitted(availableVertexList, jobVertex);
    }
  }

  public boolean markJobVertexAsSubmitted(List<JobVertex> availableToSubmitJobVertexList, JobVertex submittedNode) {
    return availableToSubmitJobVertexList.remove(submittedNode);
  }

  public boolean markJobVertexAsFinished(List<JobVertex> currentRootVertexList,
      List<JobVertex> availableToSubmitJobVertexList, JobVertex finishedNode) {
    if (currentRootVertexList.contains(finishedNode)) {
      List<JobVertex> successors = finishedNode.getSuccessors();
      for (JobVertex successor : successors) {
        successor.removeParent(finishedNode);
        if ((successor.getParents() == null) || (successor.getParents().size() == 0)) {
          currentRootVertexList.add(successor);
          availableToSubmitJobVertexList.add(successor);
        }
      }
      currentRootVertexList.remove(finishedNode);
      return true;
    }
    return false;
  }

  @Override
  public void shutDown() {
    _globalExecutorService.shutdown();
  }
}
