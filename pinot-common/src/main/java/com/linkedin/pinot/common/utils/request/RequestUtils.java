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

import com.google.common.base.Preconditions;
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
import org.apache.commons.lang3.StringUtils;


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

  /**
   * Helper method to extract all column names from group-by columns and expressions.
   * <p>We have this method because group-by columns might be passed from columns (old behavior) or expressions (UDF).
   * TODO: revisit and check if we can unify them.
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

  public static final Set<String> STAR_TREE_AGGREGATION_FUNCTIONS = ImmutableSet.of("sum", "fasthll");

  /**
   * Return whether the query is fit for star tree index.
   * <p>The query is fit for star tree index if the following conditions are met:
   * <ul>
   *   <li>Segment contains star tree</li>
   *   <li>BrokerRequest debug options have not explicitly disabled use of star tree</li>
   *   <li>Query is aggregation/group-by with all aggregation functions in {@link #STAR_TREE_AGGREGATION_FUNCTIONS}</li>
   *   <li>The aggregations must apply on metric column</li>
   *   <li>All predicate columns and group-by columns are materialized dimensions</li>
   *   <li>All predicates are conjoined by AND</li>
   * </ul>
   */
  public static boolean isFitForStarTreeIndex(SegmentMetadata segmentMetadata, BrokerRequest brokerRequest,
      FilterQueryTree rootFilterNode) {
    // Check whether segment contains star tree
    if (!segmentMetadata.hasStarTree()) {
      return false;
    }

    // Check whether star tree is disabled explicitly in BrokerRequest
    Map<String, String> debugOptions = brokerRequest.getDebugOptions();
    if (debugOptions != null && StringUtils.compareIgnoreCase(debugOptions.get(USE_STAR_TREE_KEY), "false") == 0) {
      return false;
    }

    // Get all metrics
    // NOTE: we treat all non-metric columns as dimensions in star tree
    Set<String> metrics = new HashSet<>(segmentMetadata.getSchema().getMetricNames());

    // Check whether all aggregation functions are supported and apply on metric column
    List<AggregationInfo> aggregationsInfo = brokerRequest.getAggregationsInfo();
    if (aggregationsInfo == null) {
      return false;
    }
    for (AggregationInfo aggregationInfo : aggregationsInfo) {
      if (!STAR_TREE_AGGREGATION_FUNCTIONS.contains(aggregationInfo.getAggregationType().toLowerCase())) {
        return false;
      }
      if (!metrics.contains(aggregationInfo.getAggregationParams().get("column").trim())) {
        return false;
      }
    }

    // Get all un-materialized dimensions
    StarTreeMetadata starTreeMetadata = segmentMetadata.getStarTreeMetadata();
    Preconditions.checkNotNull(starTreeMetadata);
    Set<String> unMaterializedDimensions = new HashSet<>(starTreeMetadata.getSkipMaterializationForDimensions());

    // Check whether all group-by columns are materialized dimensions
    GroupBy groupBy = brokerRequest.getGroupBy();
    if (groupBy != null) {
      Set<String> groupByColumns = getAllGroupByColumns(groupBy);
      for (String groupByColumn : groupByColumns) {
        if (metrics.contains(groupByColumn) || unMaterializedDimensions.contains(groupByColumn)) {
          return false;
        }
      }
    }

    // Check whether all predicate columns are materialized dimensions, and all predicates are conjoined by AND
    return rootFilterNode == null || checkPredicatesForStarTree(rootFilterNode, metrics, unMaterializedDimensions);
  }

  /**
   * Helper method to check whether all columns in predicates are materialized dimensions, and all predicates are
   * conjoined by AND. This is a pre-requisite in order to use star tree.
   */
  private static boolean checkPredicatesForStarTree(FilterQueryTree filterNode, Set<String> metrics,
      Set<String> unMaterializedDimensions) {
    FilterOperator operator = filterNode.getOperator();
    if (operator == FilterOperator.OR) {
      return false;
    }
    if (operator == FilterOperator.AND) {
      for (FilterQueryTree child : filterNode.getChildren()) {
        if (!checkPredicatesForStarTree(child, metrics, unMaterializedDimensions)) {
          return false;
        }
      }
      return true;
    }
    String column = filterNode.getColumn();
    return !metrics.contains(column) && !unMaterializedDimensions.contains(column);
  }
}
