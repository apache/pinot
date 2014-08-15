package com.linkedin.pinot.core.query.reduce;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.linkedin.pinot.common.query.ReduceService;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.response.AggregationResult;
import com.linkedin.pinot.common.response.BrokerResponse;
import com.linkedin.pinot.common.response.InstanceResponse;
import com.linkedin.pinot.common.response.ProcessingException;
import com.linkedin.pinot.common.response.ResponseStatistics;
import com.linkedin.pinot.common.response.RowEvent;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionFactory;


public class DefaultReduceService implements ReduceService {

  @Override
  public BrokerResponse reduce(BrokerRequest brokerRequest, Map<ServerInstance, InstanceResponse> instanceResponseMap) {
    BrokerResponse brokerResponse = new BrokerResponse();

    List<List<AggregationResult>> aggregationResultsList = new ArrayList<List<AggregationResult>>();
    for (int i = 0; i < brokerRequest.getAggregationsInfoSize(); ++i) {
      aggregationResultsList.add(new ArrayList<AggregationResult>());
    }

    for (ServerInstance serverInstance : instanceResponseMap.keySet()) {
      InstanceResponse instanceResponse = instanceResponseMap.get(serverInstance);
      // Shuffle AggregationResults
      if (instanceResponse.getAggregationResults() != null) {
        for (int i = 0; i < brokerRequest.getAggregationsInfoSize(); ++i) {
          aggregationResultsList.get(i).add(instanceResponse.getAggregationResults().get(i));
        }
      }
      // reduceOnSelectionResults
      reduceOnSelectionResults(brokerResponse.getRowEvents(), serverInstance, instanceResponse.getRowEvents());

      // reduceOnExceptions
      reduceOnExceptions(brokerResponse.getExceptions(), serverInstance, instanceResponse.getExceptions());

      // debug mode enable : reduceOnTraceInfo
      if (brokerRequest.isEnableTrace()) {
        reduceOnSegmentStatistics(brokerResponse.getSegmentStatistics(), serverInstance,
            instanceResponse.getSegmentStatistics());
        reduceOnTraceInfos(brokerResponse.getTraceInfo(), serverInstance, instanceResponse.getTraceInfo());
      }
      // reduceOnNumDocsScanned
      brokerResponse.setNumDocsScanned(brokerResponse.getNumDocsScanned() + instanceResponse.getNumDocsScanned());
      // reduceOnTotalDocs
      brokerResponse.setTotalDocs(brokerResponse.getTotalDocs() + instanceResponse.getTotalDocs());

    }
    brokerResponse.setAggregationResults(reduceOnAggregationResults(brokerRequest, aggregationResultsList));
    return brokerResponse;
  }

  private void reduceOnTraceInfos(Map<String, String> brokerTraceInfo, ServerInstance serverInstance,
      Map<String, String> traceInfoToAdd) {
    for (String key : traceInfoToAdd.keySet()) {
      brokerTraceInfo.put(serverInstance.getHostname() + " : " + key, traceInfoToAdd.get(key));
    }
  }

  private void reduceOnSegmentStatistics(List<ResponseStatistics> brokerSegmentStatistics,
      ServerInstance serverInstance, List<ResponseStatistics> segmentStatisticsToAdd) {
    brokerSegmentStatistics.addAll(segmentStatisticsToAdd);
  }

  private void reduceOnExceptions(List<ProcessingException> brokerExceptions, ServerInstance serverInstance,
      List<ProcessingException> exceptionsToAdd) {
    brokerExceptions.addAll(exceptionsToAdd);
  }

  private void reduceOnSelectionResults(List<RowEvent> brokerRowEvents, ServerInstance serverInstance,
      List<RowEvent> rowEventsToAdd) {
    // TODO Auto-generated method stub
  }

  private List<AggregationResult> reduceOnAggregationResults(BrokerRequest brokerRequest,
      List<List<AggregationResult>> aggregationResultsList) {
    List<AggregationResult> retAggregationResults = new ArrayList<AggregationResult>();
    List<AggregationFunction> aggregationFunctions = AggregationFunctionFactory.getAggregationFunction(brokerRequest);
    for (int i = 0; i < aggregationFunctions.size(); ++i) {
      retAggregationResults.add(aggregationFunctions.get(i).reduce(aggregationResultsList.get(i)));
    }
    return retAggregationResults;
  }

}
