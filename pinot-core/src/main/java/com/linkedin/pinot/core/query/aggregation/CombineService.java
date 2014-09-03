package com.linkedin.pinot.core.query.aggregation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.response.ProcessingException;
import com.linkedin.pinot.common.response.RowEvent;
import com.linkedin.pinot.core.block.aggregation.IntermediateResultsBlock;
import com.linkedin.pinot.core.query.aggregation.groupby.GroupByAggregationService;
import com.linkedin.pinot.core.query.selection.SelectionService;


/**
 * CombineReduceService will take a list of intermediate results and merge them.
 *
 */
public class CombineService {

  private static Logger LOGGER = LoggerFactory.getLogger(CombineService.class);

  public static List<Serializable> combine(List<AggregationFunction> aggregationFunctionList,
      List<List<Serializable>> aggregationResultsList, CombineLevel combineLevel) {
    List<Serializable> combinedResultsList = new ArrayList<Serializable>();
    if (aggregationResultsList == null) {
      return null;
    }
    for (int i = 0; i < aggregationFunctionList.size(); ++i) {
      if (aggregationResultsList.get(i) != null) {
        AggregationFunction aggregationFunction = aggregationFunctionList.get(i);
        List<Serializable> combinedResults = aggregationFunction.combine(aggregationResultsList.get(i), combineLevel);
        combinedResultsList.addAll(combinedResults);
      } else {
        combinedResultsList.add(null);
      }
    }
    return combinedResultsList;
  }

  public static void mergeTwoBlocks(BrokerRequest brokerRequest, IntermediateResultsBlock mergedBlock,
      IntermediateResultsBlock blockToMerge) {

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
    if (brokerRequest.isSetAggregationsInfo()) {
      if (brokerRequest.isSetGroupBy()) {
        // Combine AggregationGroupBy

        mergedBlock.setAggregationGroupByResult(combineAggregationGroupByResults(brokerRequest,
            mergedBlock.getAggregationGroupByResult(), blockToMerge.getAggregationGroupByResult()));
      } else {
        // Combine Aggregations
        List<AggregationFunction> aggregationFunctions =
            AggregationFunctionFactory.getAggregationFunction(brokerRequest);
        mergedBlock.setAggregationFunctions(aggregationFunctions);
        mergedBlock.setAggregationResults(combineAggregationResults(brokerRequest, mergedBlock.getAggregationResult(),
            blockToMerge.getAggregationResult()));
      }
    } else {
      // Combine Selections
      SelectionService selectionService =
          new SelectionService(brokerRequest.getSelections(), mergedBlock.getSelectionDataSchema());
      mergedBlock.setSelectionResult(selectionService.merge(mergedBlock.getSelectionResult(),
          blockToMerge.getSelectionResult()));
    }
  }

  private static HashMap<String, List<Serializable>> combineAggregationGroupByResults(BrokerRequest brokerRequest,
      HashMap<String, List<Serializable>> aggregationGroupByResult1,
      HashMap<String, List<Serializable>> aggregationGroupByResult2) {
    GroupByAggregationService groupByAggregationService = new GroupByAggregationService();
    groupByAggregationService.init(brokerRequest.getAggregationsInfo(), brokerRequest.getGroupBy());
    return groupByAggregationService.combine(aggregationGroupByResult1, aggregationGroupByResult2);

  }

  private static List<Serializable> combineAggregationResults(BrokerRequest brokerRequest,
      List<Serializable> aggregationResult1, List<Serializable> aggregationResult2) {

    List<List<Serializable>> aggregationResultsList = new ArrayList<List<Serializable>>();
    for (int i = 0; i < brokerRequest.getAggregationsInfoSize(); ++i) {
      aggregationResultsList.add(new ArrayList<Serializable>());
    }

    for (int i = 0; i < brokerRequest.getAggregationsInfoSize(); ++i) {
      aggregationResultsList.add(new ArrayList<Serializable>());
      aggregationResultsList.get(i).add(aggregationResult1.get(i));
      aggregationResultsList.get(i).add(aggregationResult2.get(i));
    }

    List<Serializable> retAggregationResults = new ArrayList<Serializable>();
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
