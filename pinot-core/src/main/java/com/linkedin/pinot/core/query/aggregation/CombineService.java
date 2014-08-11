package com.linkedin.pinot.core.query.aggregation;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.response.AggregationResult;
import com.linkedin.pinot.common.response.ProcessingException;
import com.linkedin.pinot.common.response.RowEvent;
import com.linkedin.pinot.core.block.aggregation.AggregationAndSelectionResultBlock;


/**
 * CombineReduceService will take a list of intermediate results and merge them.
 *
 */
public class CombineService {

  private static Logger LOGGER = LoggerFactory.getLogger(CombineService.class);

  public static List<AggregationResult> combine(List<AggregationFunction> aggregationFunctionList,
      List<List<AggregationResult>> aggregationResultsList, CombineLevel combineLevel) {
    List<AggregationResult> combinedResultsList = new ArrayList<AggregationResult>();
    if (aggregationResultsList == null) {
      return null;
    }
    for (int i = 0; i < aggregationFunctionList.size(); ++i) {
      if (aggregationResultsList.get(i) != null) {
        AggregationFunction aggregationFunction = aggregationFunctionList.get(i);
        AggregationResult combinedResults = aggregationFunction.combine(aggregationResultsList.get(i), combineLevel);
        combinedResultsList.add(combinedResults);
      } else {
        combinedResultsList.add(null);
      }
    }
    return combinedResultsList;
  }

  public static void mergeTwoBlocks(BrokerRequest brokerRequest, AggregationAndSelectionResultBlock mergedBlock,
      AggregationAndSelectionResultBlock blockToMerge) {

    // Combine NumDocsScanned
    mergedBlock.setNumDocsScanned(mergedBlock.getNumDocsScanned() + blockToMerge.getNumDocsScanned());
    // Combine TotalDocs
    mergedBlock.setTotalDocs(mergedBlock.getTotalDocs() + blockToMerge.getTotalDocs());
    // Debug mode enable : Combine SegmentStatistics and TraceInfo
    if (brokerRequest.isEnableTrace()) {
      mergedBlock.getSegmentStatistics().addAll(blockToMerge.getSegmentStatistics());
      mergedBlock.getTraceInfo().putAll(blockToMerge.getTraceInfo());
    }
    // Combine Exceptions
    mergedBlock.setExceptionsList(combineExceptions(mergedBlock.getExceptions(), blockToMerge.getExceptions()));
    // Combine SelectionResults
    mergedBlock.setRowEvents(combineSelectionResults(brokerRequest, mergedBlock.getRowEvents(),
        blockToMerge.getRowEvents()));
    // Combine Aggregations
    mergedBlock.setAggregationResults(combineAggregationResults(brokerRequest, mergedBlock.getAggregationResult(),
        blockToMerge.getAggregationResult()));

  }

  private static List<AggregationResult> combineAggregationResults(BrokerRequest brokerRequest,
      List<AggregationResult> aggregationResult1, List<AggregationResult> aggregationResult2) {

    List<List<AggregationResult>> aggregationResultsList = new ArrayList<List<AggregationResult>>();
    for (int i = 0; i < brokerRequest.getAggregationsInfoSize(); ++i) {
      aggregationResultsList.add(new ArrayList<AggregationResult>());
    }

    for (int i = 0; i < brokerRequest.getAggregationsInfoSize(); ++i) {
      aggregationResultsList.add(new ArrayList<AggregationResult>());
      aggregationResultsList.get(i).add(aggregationResult1.get(i));
      aggregationResultsList.get(i).add(aggregationResult2.get(i));
    }

    List<AggregationResult> retAggregationResults = new ArrayList<AggregationResult>();
    List<AggregationFunction> aggregationFunctions = AggregationFunctionFactory.getAggregationFunction(brokerRequest);
    for (int i = 0; i < aggregationFunctions.size(); ++i) {
      retAggregationResults.add(aggregationFunctions.get(i).reduce(aggregationResultsList.get(i)));
    }
    return retAggregationResults;
  }

  private static List<RowEvent> combineSelectionResults(BrokerRequest brokerRequest, List<RowEvent> rowEvents1,
      List<RowEvent> rowEvents2) {
    if (rowEvents1 == null) {
      return rowEvents2;
    }
    if (rowEvents2 == null) {
      return rowEvents1;
    }
    // TODO(xiafu): Implement rowEvents merge logic.
    throw new UnsupportedOperationException();
  }

  private static List<ProcessingException> combineExceptions(List<ProcessingException> exceptions1,
      List<ProcessingException> exceptions2) {
    if (exceptions1 == null) {
      return exceptions2;
    }
    if (exceptions2 == null) {
      return exceptions1;
    }
    exceptions1.addAll(exceptions2);
    return exceptions1;
  }
}
