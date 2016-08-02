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
package com.linkedin.pinot.core.query.aggregation;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.response.ProcessingException;
import com.linkedin.pinot.common.utils.DataTableBuilder;
import com.linkedin.pinot.core.operator.blocks.IntermediateResultsBlock;
import com.linkedin.pinot.core.query.selection.SelectionOperatorService;
import com.linkedin.pinot.core.query.selection.SelectionOperatorUtils;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * CombineReduceService will take a list of intermediate results and merge them.
 *
 */
public class CombineService {

  private static final Logger LOGGER = LoggerFactory.getLogger(CombineService.class);

  public static void mergeTwoBlocks(BrokerRequest brokerRequest, IntermediateResultsBlock mergedBlock,
      IntermediateResultsBlock blockToMerge) {
    // Sanity check
    Preconditions.checkNotNull(mergedBlock);
    if (blockToMerge == null) {
      return;
    }

    // Combine NumDocsScanned
    mergedBlock.setNumDocsScanned(mergedBlock.getNumDocsScanned() + blockToMerge.getNumDocsScanned());
    // Combine TotalDocs
    mergedBlock.setTotalRawDocs(mergedBlock.getTotalRawDocs() + blockToMerge.getTotalRawDocs());
    // Debug mode enable : Combine SegmentStatistics and TraceInfo
    if (brokerRequest.isEnableTrace()) {
      // mergedBlock.getSegmentStatistics().addAll(blockToMerge.getSegmentStatistics());
    }
    // Combine Exceptions
    mergedBlock.setExceptionsList(combineExceptions(mergedBlock.getExceptions(), blockToMerge.getExceptions()));

    if (brokerRequest.isSetAggregationsInfo()) {
      if (brokerRequest.isSetGroupBy()) {
        // Combine AggregationGroupBy
        mergedBlock.setAggregationGroupByResult(combineAggregationGroupByResults1(brokerRequest,
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

      DataTableBuilder.DataSchema mergedBlockSchema = mergedBlock.getSelectionDataSchema();
      DataTableBuilder.DataSchema blockToMergeSchema = blockToMerge.getSelectionDataSchema();
      Collection<Serializable[]> mergedResultSet = mergedBlock.getSelectionResult();
      Collection<Serializable[]> toMergeResultSet = blockToMerge.getSelectionResult();

      // [pinot 2315] For select * queries we let every segment determine schema and then
      // combine results here. Combine can fail or silently give wrong results if the
      // schemas are different. Here, we select the schema that returns atleast one
      // row. If both mergedBlock and blockToMerge have result sets but different schema
      // then the results are random.
      // Data schema can be null if the filter query does not return results
      if (mergedBlockSchema != null && ! mergedBlockSchema.equals(blockToMergeSchema)) {
        if (mergedResultSet.size() == 0 && toMergeResultSet.size() > 0) {
          // select the schema that has returned atleast one row
          mergedBlock.setSelectionDataSchema(blockToMergeSchema);
          // fall to the logic below which correctly handles null results sets
        } else {
          LOGGER
              .warn("Schema inconsistency. merged block schema: {}, block to merge schema: {}. Dropping block to merge",
                  mergedBlockSchema, blockToMergeSchema);
          return;
        }
      }

      // Combine Selections
      if (brokerRequest.getSelections().isSetSelectionSortSequence()) {
        SelectionOperatorService selectionService =
            new SelectionOperatorService(brokerRequest.getSelections(), mergedBlockSchema);
        mergedBlock.setSelectionResult(selectionService.merge(mergedResultSet, toMergeResultSet));
      } else {
        mergedBlock.setSelectionResult(SelectionOperatorUtils.mergeWithoutOrdering(mergedResultSet,
            toMergeResultSet,
            brokerRequest.getSelections().getSize()));
      }
    }
  }

  private static List<Map<String, Serializable>> combineAggregationGroupByResults1(BrokerRequest brokerRequest,
      List<Map<String, Serializable>> list1, List<Map<String, Serializable>> list2) {
    if (list1 == null) {
      return list2;
    }
    if (list2 == null) {
      return list1;
    }

    for (int i = 0; i < list1.size(); ++i) {
      list1.set(i, mergeTwoGroupedResults(brokerRequest.getAggregationsInfo().get(i), list1.get(i), list2.get(i)));
    }

    return list1;

  }

  private static Map<String, Serializable> mergeTwoGroupedResults(AggregationInfo aggregationInfo,
      Map<String, Serializable> map1, Map<String, Serializable> map2) {
    if (map1 == null) {
      return map2;
    }
    if (map2 == null) {
      return map1;
    }

    AggregationFunction aggregationFunction = AggregationFunctionFactory.get(aggregationInfo, true);
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
    if (aggregationResult1 == null) {
      return aggregationResult2;
    }
    if (aggregationResult2 == null) {
      return aggregationResult1;
    }
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
