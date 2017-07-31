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
package com.linkedin.pinot.core.query.utils;

import com.google.common.collect.ImmutableSet;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.core.metadata.segment.SegmentMetadata;
import com.linkedin.pinot.core.metadata.startree.StarTreeMetadata;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class StarTreeUtils {
  private StarTreeUtils() {
  }

  private static final Set<String> ALLOWED_AGGREGATION_FUNCTIONS = ImmutableSet.of("sum", "fasthll");
  private static final String USE_STAR_TREE_KEY = "useStarTree";

  /**
   * Returns true for the following, false otherwise:
   * <ul>
   *   <li>BrokerRequest debug options have not explicitly disabled use of star tree</li>
   *   <li>Query is aggregation/group-by</li>
   *   <li>Segment contains star tree index</li>
   *   <li>All aggregation functions in the query should be in {@link #ALLOWED_AGGREGATION_FUNCTIONS}</li>
   *   <li>All group by columns and predicate columns are materialized</li>
   *   <li>Predicates do not contain any metric columns</li>
   *   <li>Query consists only of simple predicates, conjoined by AND</li>
   * </ul>
   */
  public static boolean isFitForStarTreeIndex(SegmentMetadata segmentMetadata, FilterQueryTree filterTree,
      BrokerRequest brokerRequest) {

    // If broker request disables use of star tree, return false.
    if (!isStarTreeEnabledInBrokerRequest(brokerRequest)) {
      return false;
    }
    // Apply the checks in order of their runtime.
    List<AggregationInfo> aggregationsInfo = brokerRequest.getAggregationsInfo();

    // There should have some aggregation
    if (aggregationsInfo == null || aggregationsInfo.isEmpty()) {
      return false;
    }

    // Segment metadata should contain star tree metadata.
    StarTreeMetadata starTreeMetadata = segmentMetadata.getStarTreeMetadata();
    if (starTreeMetadata == null) {
      return false;
    }

    Set<String> metricColumnSet = new HashSet<>(segmentMetadata.getSchema().getMetricNames());
    List<String> skipMaterializationList = starTreeMetadata.getSkipMaterializationForDimensions();
    Set<String> skipMaterializationSet = null;
    if (skipMaterializationList != null && !skipMaterializationList.isEmpty()) {
      skipMaterializationSet = new HashSet<>(skipMaterializationList);
    }

    // Ensure that none of the group-by columns are metric or skipped for materialization.
    GroupBy groupBy = brokerRequest.getGroupBy();
    if (groupBy != null) {
      List<String> groupByColumns = groupBy.getColumns();
      for (String groupByColumn : groupByColumns) {
        if (metricColumnSet.contains(groupByColumn)) {
          return false;
        }
        if (skipMaterializationSet != null && skipMaterializationSet.contains(groupByColumn)) {
          return false;
        }
      }
    }

    // We currently support only limited aggregations
    for (AggregationInfo aggregationInfo : aggregationsInfo) {
      String aggregationFunctionName = aggregationInfo.getAggregationType().toLowerCase();
      if (!ALLOWED_AGGREGATION_FUNCTIONS.contains(aggregationFunctionName)) {
        return false;
      }
    }

    //if the filter tree has children, ensure that root is AND and all its children are leaves, and
    //no metric columns appear in the predicates.
    if (filterTree != null && filterTree.getChildren() != null && !filterTree.getChildren().isEmpty()) {
      //ensure that its AND
      if (filterTree.getOperator() != FilterOperator.AND) {
        return false;
      }
      //ensure that children are not nested further and only one predicate per column
      Set<String> predicateColumns = new HashSet<>();
      for (FilterQueryTree child : filterTree.getChildren()) {
        if (child.getChildren() != null && !child.getChildren().isEmpty()) {
          //star tree index cannot support nested filter predicates
          return false;
        }
        //only one predicate per column is supported
        String column = child.getColumn();
        if (predicateColumns.contains(column)) {
          return false;
        }

        // predicate columns should be materialized.
        if ((skipMaterializationSet != null) && skipMaterializationSet.contains(column)) {
          return false;
        }

        // predicate should not contain metric columns.
        if (metricColumnSet.contains(column)) {
          return false;
        }
        predicateColumns.add(column);
      }
    } else if (filterTree != null) {
      // Predicate column of root node should be materialized.
      String rootColumn = filterTree.getColumn();
      if (skipMaterializationSet != null && skipMaterializationSet.contains(rootColumn)) {
        return false;
      }

      // predicate should not contain metric columns.
      if (metricColumnSet.contains(rootColumn)) {
        return false;
      }
    }

    return true;
  }

  /**
   * This method returns the value of {@link #USE_STAR_TREE_KEY} boolean flag specified in the debug options
   * in broker request. If the flag is not specified in the debug options, it returns true.
   *
   * @param brokerRequest Broker Request
   * @return Value of {@link #USE_STAR_TREE_KEY} debug option, or true if not option not specified.
   */
  private static boolean isStarTreeEnabledInBrokerRequest(BrokerRequest brokerRequest) {
    Map<String, String> debugOptions = brokerRequest.getDebugOptions();
    if (debugOptions == null) {
      return true;
    }
    String useStarTreeString = debugOptions.get(USE_STAR_TREE_KEY);
    return (useStarTreeString != null) ? Boolean.valueOf(useStarTreeString) : true;
  }
}
