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
package com.linkedin.pinot.common.utils.request;

import com.google.common.collect.ImmutableSet;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.request.FilterQuery;
import com.linkedin.pinot.common.request.FilterQueryMap;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.common.request.HavingFilterQuery;
import com.linkedin.pinot.common.request.HavingFilterQueryMap;
import com.linkedin.pinot.common.request.transform.TransformExpressionTree;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.segment.StarTreeMetadata;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.mutable.MutableInt;


public class RequestUtils {
  private RequestUtils() {
  }

  private static final String USE_STAR_TREE_KEY = "useStarTree";
  private static final Pql2Compiler PQL2_COMPILER = new Pql2Compiler();


  /**
   * Generates thrift compliant filterQuery and populate it in the broker request
   * @param filterQueryTree
   * @param request
   */
  public static void generateFilterFromTree(FilterQueryTree filterQueryTree, BrokerRequest request) {
    Map<Integer, FilterQuery> filterQueryMap = new HashMap<Integer, FilterQuery>();
    MutableInt currentId = new MutableInt(0);
    FilterQuery root = traverseFilterQueryAndPopulateMap(filterQueryTree, filterQueryMap, currentId);
    filterQueryMap.put(root.getId(), root);
    request.setFilterQuery(root);
    FilterQueryMap mp = new FilterQueryMap();
    mp.setFilterQueryMap(filterQueryMap);
    request.setFilterSubQueryMap(mp);
  }

  public static void generateFilterFromTree(HavingQueryTree filterQueryTree, BrokerRequest request) {
    Map<Integer, HavingFilterQuery> filterQueryMap = new HashMap<Integer, HavingFilterQuery>();
    MutableInt currentId = new MutableInt(0);
    HavingFilterQuery root = traverseHavingFilterQueryAndPopulateMap(filterQueryTree, filterQueryMap, currentId);
    filterQueryMap.put(root.getId(), root);
    request.setHavingFilterQuery(root);
    HavingFilterQueryMap mp = new HavingFilterQueryMap();
    mp.setFilterQueryMap(filterQueryMap);
    request.setHavingFilterSubQueryMap(mp);
  }

  private static FilterQuery traverseFilterQueryAndPopulateMap(FilterQueryTree tree,
      Map<Integer, FilterQuery> filterQueryMap, MutableInt currentId) {
    int currentNodeId = currentId.intValue();
    currentId.increment();

    final List<Integer> f = new ArrayList<Integer>();
    if (null != tree.getChildren()) {
      for (final FilterQueryTree c : tree.getChildren()) {
        int childNodeId = currentId.intValue();
        currentId.increment();

        f.add(childNodeId);
        final FilterQuery q = traverseFilterQueryAndPopulateMap(c, filterQueryMap, currentId);
        filterQueryMap.put(childNodeId, q);
      }
    }

    FilterQuery query = new FilterQuery();
    query.setColumn(tree.getColumn());
    query.setId(currentNodeId);
    query.setNestedFilterQueryIds(f);
    query.setOperator(tree.getOperator());
    query.setValue(tree.getValue());
    return query;
  }

  private static HavingFilterQuery traverseHavingFilterQueryAndPopulateMap(HavingQueryTree tree,
      Map<Integer, HavingFilterQuery> filterQueryMap, MutableInt currentId) {
    int currentNodeId = currentId.intValue();
    currentId.increment();

    final List<Integer> filterIds = new ArrayList<Integer>();
    if (null != tree.getChildren()) {
      for (final HavingQueryTree child : tree.getChildren()) {
        int childNodeId = currentId.intValue();
        currentId.increment();
        filterIds.add(childNodeId);
        final HavingFilterQuery filterQuery = traverseHavingFilterQueryAndPopulateMap(child, filterQueryMap, currentId);
        filterQueryMap.put(childNodeId, filterQuery);
      }
    }

    HavingFilterQuery havingFilterQuery = new HavingFilterQuery();
    havingFilterQuery.setAggregationInfo(tree.getAggregationInfo());
    havingFilterQuery.setId(currentNodeId);
    havingFilterQuery.setNestedFilterQueryIds(filterIds);
    havingFilterQuery.setOperator(tree.getOperator());
    havingFilterQuery.setValue(tree.getValue());
    return havingFilterQuery;
  }

  /**
   * Generate FilterQueryTree from Broker Request
   * @param request Broker Request
   * @return
   */
  public static FilterQueryTree generateFilterQueryTree(BrokerRequest request) {
    FilterQueryTree root = null;

    FilterQuery q = request.getFilterQuery();

    if (null != q && null != request.getFilterSubQueryMap()) {
      root = buildFilterQuery(q.getId(), request.getFilterSubQueryMap().getFilterQueryMap());
    }

    return root;
  }

  public static FilterQueryTree buildFilterQuery(Integer id, Map<Integer, FilterQuery> queryMap) {
    FilterQuery q = queryMap.get(id);

    List<Integer> children = q.getNestedFilterQueryIds();

    List<FilterQueryTree> c = null;
    if (null != children && !children.isEmpty()) {
      c = new ArrayList<FilterQueryTree>();
      for (final Integer i : children) {
        final FilterQueryTree t = buildFilterQuery(i, queryMap);
        c.add(t);
      }
    }

    FilterQueryTree q2 = new FilterQueryTree(q.getColumn(), q.getValue(), q.getOperator(), c);
    return q2;
  }

  public static final Set<String> ALLOWED_AGGREGATION_FUNCTIONS = ImmutableSet.of("sum", "fasthll");

  /**
   * Returns true for the following, false otherwise:
   * - BrokerRequest debug options have not explicitly disabled use of star tree
   * - Query is not aggregation/group-by
   * - Segment does not contain star tree
   * - The only aggregation function in the query should be in {@link #ALLOWED_AGGREGATION_FUNCTIONS}
   * - All group by columns and predicate columns are materialized
   * - Predicates do not contain any metric columns
   * - Query consists only of simple predicates, conjoined by AND.
   *   <p>
   *   e.g. WHERE d1 = d1v1 AND d2 = d2v2 AND d3 = d3v3 AND d4 between t1,t2
   *   </p>
   *
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
      Set<String> allGroupByColumns = getAllGroupByColumns(groupBy);

      for (String groupByColumn : allGroupByColumns) {
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
  public static boolean isStarTreeEnabledInBrokerRequest(BrokerRequest brokerRequest) {
    Map<String, String> debugOptions = brokerRequest.getDebugOptions();
    if (debugOptions == null) {
      return true;
    }
    String useStarTreeString = debugOptions.get(USE_STAR_TREE_KEY);
    return (useStarTreeString != null) ? Boolean.valueOf(useStarTreeString) : true;
  }

  /**
   * Helper method to extract all column names from group by columns and expressions
   * @param groupBy
   * @return
   */
  public static Set<String> getAllGroupByColumns(GroupBy groupBy) {
    Set<String> allGroupByColumns = new HashSet<>();

    if (groupBy != null) {
      List<String> groupByColumns = groupBy.getColumns();
      if (groupByColumns != null) {
        allGroupByColumns.addAll(groupByColumns);
      }

      List<String> groupByExpressions = groupBy.getExpressions();
      if (groupByExpressions != null) {
        for (String expression : groupByExpressions) {
          TransformExpressionTree expressionTree = PQL2_COMPILER.compileToExpressionTree(expression);
          List<String> columns = new ArrayList<>();
          expressionTree.getColumns(columns);
          allGroupByColumns.addAll(columns);
        }
      }
    }
    return allGroupByColumns;
  }
}
