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
package com.linkedin.pinot.core.plan;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.DataSourceMetadata;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.filter.AndOperator;
import com.linkedin.pinot.core.operator.filter.BaseFilterOperator;
import com.linkedin.pinot.core.operator.filter.BitmapBasedFilterOperator;
import com.linkedin.pinot.core.operator.filter.EmptyFilterOperator;
import com.linkedin.pinot.core.operator.filter.MatchEntireSegmentOperator;
import com.linkedin.pinot.core.operator.filter.OrOperator;
import com.linkedin.pinot.core.operator.filter.ScanBasedFilterOperator;
import com.linkedin.pinot.core.operator.filter.SortedInvertedIndexBasedFilterOperator;
import com.linkedin.pinot.core.operator.filter.StarTreeIndexOperator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 */
public class FilterPlanNode implements PlanNode {
  private static final Logger LOGGER = LoggerFactory.getLogger(FilterPlanNode.class);
  private final BrokerRequest _brokerRequest;
  private final IndexSegment _segment;
  private boolean _optimizeAlwaysFalse;

  public FilterPlanNode(IndexSegment segment, BrokerRequest brokerRequest) {
    _segment = segment;
    _brokerRequest = brokerRequest;
    _optimizeAlwaysFalse = true;
  }

  @Override
  public Operator run() {
    long start = System.currentTimeMillis();
    Operator operator;
    FilterQueryTree filterQueryTree = RequestUtils.generateFilterQueryTree(_brokerRequest);
    if (_segment.getSegmentMetadata().hasStarTree()
        && RequestUtils.isFitForStarTreeIndex(_segment.getSegmentMetadata(), filterQueryTree, _brokerRequest)) {
      operator = new StarTreeIndexOperator(_segment, _brokerRequest);
    } else {
      operator = constructPhysicalOperator(filterQueryTree, _segment, _optimizeAlwaysFalse);
    }
    long end = System.currentTimeMillis();
    LOGGER.debug("FilterPlanNode.run took:{}", (end - start));
    return operator;
  }

  /**
   * Helper method to build the operator tree from the filter query tree.
   * @param filterQueryTree
   * @param segment Index segment
   * @param optimizeAlwaysFalse Optimize isResultEmpty predicates
   * @return Filter Operator created
   */
  @VisibleForTesting
  public static BaseFilterOperator constructPhysicalOperator(FilterQueryTree filterQueryTree, IndexSegment segment,
      boolean optimizeAlwaysFalse) {
    BaseFilterOperator ret;

    if (null == filterQueryTree) {
      return new MatchEntireSegmentOperator(segment.getSegmentMetadata().getTotalRawDocs());
    }

    final List<FilterQueryTree> childFilters = filterQueryTree.getChildren();
    final boolean isLeaf = (childFilters == null) || childFilters.isEmpty();

    if (!isLeaf) {
      int numChildrenAlwaysFalse = 0;
      int numChildren = childFilters.size();
      List<BaseFilterOperator> operators = new ArrayList<>();

      final FilterOperator filterType = filterQueryTree.getOperator();
      for (final FilterQueryTree query : childFilters) {
        BaseFilterOperator childOperator = constructPhysicalOperator(query, segment, optimizeAlwaysFalse);

        // Count number of always false children.
        if (optimizeAlwaysFalse && childOperator.isResultEmpty()) {
          numChildrenAlwaysFalse++;

          // Early bailout for 'AND' as soon as one of the children always evaluates to false.
          if (filterType == FilterOperator.AND) {
            break;
          }
        }
        operators.add(childOperator);
      }

      ret = buildNonLeafOperator(filterType, operators, numChildrenAlwaysFalse, numChildren, optimizeAlwaysFalse);
    } else {
      final FilterOperator filterType = filterQueryTree.getOperator();
      final String column = filterQueryTree.getColumn();
      Predicate predicate = Predicate.newPredicate(filterQueryTree);

      DataSource ds;
      ds = segment.getDataSource(column);
      DataSourceMetadata dataSourceMetadata = ds.getDataSourceMetadata();
      BaseFilterOperator baseFilterOperator;
      int startDocId = 0;
      int endDocId = segment.getSegmentMetadata().getTotalRawDocs() - 1; //end is inclusive
      //use inverted index only if the column has dictionary.
      if (dataSourceMetadata.hasInvertedIndex() && dataSourceMetadata.hasDictionary()) {
        // range evaluation based on inv index is inefficient, so do this only if is NOT range.
        if (!filterType.equals(FilterOperator.RANGE) && !filterType.equals(FilterOperator.REGEXP_LIKE)) {
          if (dataSourceMetadata.isSingleValue() && dataSourceMetadata.isSorted()) {
            // if the column is sorted use sorted inverted index based implementation
            baseFilterOperator = new SortedInvertedIndexBasedFilterOperator(predicate, ds, startDocId, endDocId);
          } else {
            baseFilterOperator = new BitmapBasedFilterOperator(predicate, ds, startDocId, endDocId);
          }
        } else {
          baseFilterOperator = new ScanBasedFilterOperator(predicate, ds, startDocId, endDocId);
        }
      } else {
        baseFilterOperator = new ScanBasedFilterOperator(predicate, ds, startDocId, endDocId);
      }
      ret = baseFilterOperator;
    }
    // If operator evaluates to false, then just return an empty operator.
    if (ret.isResultEmpty()) {
      ret = new EmptyFilterOperator();
    }
    return ret;
  }

  /**
   * Helper method to build AND/OR operators.
   * <ul>
   *   <li> Returns {@link EmptyFilterOperator} if at least on child always evaluates to false for AND. </li>
   *   <li> Returns {@link EmptyFilterOperator} if all children always evaluates to false for OR. </li>
   *   <li> Returns {@link AndOperator} or {@link OrOperator} based on filterType, otherwise. </li>
   * </ul>
   * @param filterType AND/OR
   * @param nonFalseChildren Children that are not alwaysFalse.
   * @param numChildrenAlwaysFalse Number of children that are always false.
   * @param numChildren Total number of children.
   * @param optimizeAlwaysFalse Optimize alwaysFalse predicates
   * @return Filter Operator created
   */
  private static BaseFilterOperator buildNonLeafOperator(FilterOperator filterType,
      List<BaseFilterOperator> nonFalseChildren, int numChildrenAlwaysFalse, int numChildren,
      boolean optimizeAlwaysFalse) {
    BaseFilterOperator operator;

    switch (filterType) {
      case AND:
        if (optimizeAlwaysFalse && numChildrenAlwaysFalse > 0) {
          operator = new EmptyFilterOperator();
        } else {
          reorder(nonFalseChildren);
          operator = new AndOperator(nonFalseChildren);
        }
        break;

      case OR:
        if (optimizeAlwaysFalse && numChildrenAlwaysFalse == numChildren) {
          operator = new EmptyFilterOperator();
        } else {
          reorder(nonFalseChildren);
          operator = new OrOperator(nonFalseChildren);
        }
        break;
      default:
        throw new UnsupportedOperationException("Not support filter type - " + filterType + " with children operators");
    }

    return operator;
  }

  /**
   * Re orders operators, puts Sorted -> Inverted and then Raw scan. TODO: With Inverted, we can
   * further optimize based on cardinality
   * @param operators
   */
  private static void reorder(List<BaseFilterOperator> operators) {

    final Map<Operator, Integer> operatorPriorityMap = new HashMap<Operator, Integer>();
    for (Operator operator : operators) {
      Integer priority = Integer.MAX_VALUE;
      if (operator instanceof SortedInvertedIndexBasedFilterOperator) {
        priority = 0;
      } else if (operator instanceof AndOperator) {
        priority = 1;
      } else if (operator instanceof BitmapBasedFilterOperator) {
        priority = 2;
      } else if (operator instanceof ScanBasedFilterOperator) {
        priority = 3;
      } else if (operator instanceof OrOperator) {
        priority = 4;
      }
      operatorPriorityMap.put(operator, priority);
    }

    Comparator<? super Operator> comparator = new Comparator<Operator>() {
      @Override
      public int compare(Operator o1, Operator o2) {
        return Integer.compare(operatorPriorityMap.get(o1), operatorPriorityMap.get(o2));
      }
    };
    Collections.sort(operators, comparator);
  }

  @Override
  public void showTree(String prefix) {
    final String treeStructure = prefix + "Filter Plan Node\n" + prefix + "Operator: Filter\n" + prefix + "Argument 0: "
        + _brokerRequest.getFilterQuery();
    LOGGER.debug(treeStructure);
  }
}
