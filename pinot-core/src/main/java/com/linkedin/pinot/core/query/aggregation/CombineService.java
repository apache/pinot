package com.linkedin.pinot.core.query.aggregation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.response.ProcessingException;
import com.linkedin.pinot.common.response.RowEvent;
import com.linkedin.pinot.core.block.query.IntermediateResultsBlock;
import com.linkedin.pinot.core.query.selection.SelectionOperatorService;


/**
 * CombineReduceService will take a list of intermediate results and merge them.
 *
 */
public class CombineService {

  private static Logger LOGGER = LoggerFactory.getLogger(CombineService.class);

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
        mergedBlock.setAggregationGroupByResult1(combineAggregationGroupByResults1(brokerRequest,
            mergedBlock.getAggregationGroupByOperatorResult(), blockToMerge.getAggregationGroupByOperatorResult()));
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
      SelectionOperatorService selectionService =
          new SelectionOperatorService(brokerRequest.getSelections(), mergedBlock.getSelectionDataSchema());
      mergedBlock.setSelectionResult(selectionService.merge(mergedBlock.getSelectionResult(),
          blockToMerge.getSelectionResult()));
    }
  }

  private static List<Map<String, Serializable>> combineAggregationGroupByResults1(BrokerRequest brokerRequest,
      List<Map<String, Serializable>> list1, List<Map<String, Serializable>> list2) {
    for (int i = 0; i < list1.size(); ++i) {
      list1.set(i, mergeTwoGroupedResults(brokerRequest.getAggregationsInfo().get(i), list1.get(i), list2.get(i)));
    }

    return list1;

  }

  private static Map<String, Serializable> mergeTwoGroupedResults(AggregationInfo aggregationInfo,
      Map<String, Serializable> map1, Map<String, Serializable> map2) {
    AggregationFunction aggregationFunction = AggregationFunctionFactory.get(aggregationInfo);
    for (String key : map2.keySet()) {
      if (map1.containsKey(key)) {
        map1.put(key, aggregationFunction.combineTwoValues(map1.get(key), map2.get(key)));
      } else {
        map1.put(key, map2.get(key));
      }
    }
    return map1;
  }

  private static List<Serializable> combineAggregationResults(BrokerRequest brokerRequest,
      List<Serializable> aggregationResult1, List<Serializable> aggregationResult2) {

    List<List<Serializable>> aggregationResultsList = new ArrayList<List<Serializable>>();

    for (int i = 0; i < brokerRequest.getAggregationsInfoSize(); ++i) {
      aggregationResultsList.add(new ArrayList<Serializable>());
      if (aggregationResult1.get(i) != null) {
        aggregationResultsList.get(i).add(aggregationResult1.get(i));
      }
      if (aggregationResult2.get(i) != null) {
        aggregationResultsList.get(i).add(aggregationResult2.get(i));
      }
    }

    List<Serializable> retAggregationResults = new ArrayList<Serializable>();
    List<AggregationFunction> aggregationFunctions = AggregationFunctionFactory.getAggregationFunction(brokerRequest);
    for (int i = 0; i < aggregationFunctions.size(); ++i) {
      retAggregationResults.add((Serializable) aggregationFunctions.get(i)
          .combine(aggregationResultsList.get(i), CombineLevel.INSTANCE).get(0));
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
