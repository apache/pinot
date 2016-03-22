/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.segment.StarTreeMetadata;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.request.FilterQuery;
import com.linkedin.pinot.common.request.FilterQueryMap;


public class RequestUtils {
  /**
   * Generates thrift compliant filterQuery and populate it in the broker request
   * @param filterQueryTree
   * @param request
   */
  public static void generateFilterFromTree(FilterQueryTree filterQueryTree, BrokerRequest request) {
    Map<Integer, FilterQuery> filterQueryMap = new HashMap<Integer, FilterQuery>();
    FilterQuery root = traverseFilterQueryAndPopulateMap(filterQueryTree, filterQueryMap);
    filterQueryMap.put(root.getId(), root);
    request.setFilterQuery(root);
    FilterQueryMap mp = new FilterQueryMap();
    mp.setFilterQueryMap(filterQueryMap);
    request.setFilterSubQueryMap(mp);
  }

  private static FilterQuery traverseFilterQueryAndPopulateMap(FilterQueryTree tree,
      Map<Integer, FilterQuery> filterQueryMap) {
    final List<Integer> f = new ArrayList<Integer>();
    if (null != tree.getChildren()) {
      for (final FilterQueryTree c : tree.getChildren()) {
        f.add(c.getId());
        final FilterQuery q = traverseFilterQueryAndPopulateMap(c, filterQueryMap);
        filterQueryMap.put(c.getId(), q);
      }
    }
    FilterQuery query = new FilterQuery();
    query.setColumn(tree.getColumn());
    query.setId(tree.getId());
    query.setNestedFilterQueryIds(f);
    query.setOperator(tree.getOperator());
    query.setValue(tree.getValue());
    return query;
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

    FilterQueryTree q2 = new FilterQueryTree(id, q.getColumn(), q.getValue(), q.getOperator(), c);
    return q2;
  }

  /**
   * Returns true if the filter query consists only of simple predicates, conjoined by AND.
   * <p>
   * e.g. WHERE d1 = d1v1 AND d2 = d2v2 AND d3 = d3v3 AND d4 between t1,t2
   * </p>
   */
  public static boolean isFitForStarTreeIndex(SegmentMetadata segmentMetadata, FilterQueryTree filterTree,
      List<AggregationInfo> aggregationsInfo) {
    // Apply the checks in order of their runtime.

    // There should have some aggregation
    if (aggregationsInfo == null || aggregationsInfo.isEmpty()) {
      return false;
    }

    // Segment metadata should contain star tree metadata.
    StarTreeMetadata starTreeMetadata = segmentMetadata.getStarTreeMetadata();
    if (starTreeMetadata == null) {
      return false;
    }

    List<String> skipMaterializationList = starTreeMetadata.getSkipMaterializationForDimensions();
    Set<String> skipMaterializationSet = null;
    if (skipMaterializationList != null && !skipMaterializationList.isEmpty()) {
      skipMaterializationSet = new HashSet<String>(skipMaterializationList);
    }

    // We currently support only sum
    for (AggregationInfo aggregationInfo : aggregationsInfo) {
      if (!aggregationInfo.getAggregationType().equalsIgnoreCase("sum")) {
        return false;
      }
    }

    //if the filter tree has children, ensure that root is AND and all its children are leaves
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
        predicateColumns.add(child.getColumn());
      }
    } else if (filterTree != null){
      // Predicate column of root node should be materialized.
      String rootColumn = filterTree.getColumn();
      if (skipMaterializationSet != null && skipMaterializationSet.contains(rootColumn)) {
        return false;
      }
    }

    return true;
  }
}
