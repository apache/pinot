/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.operator.filter;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.datasource.DataSource;


public class FilterOperatorUtils {
  private FilterOperatorUtils() {
  }

  /**
   * Returns the leaf filter operator (i.e. not {@link AndFilterOperator} or {@link OrFilterOperator}).
   */
  public static BaseFilterOperator getLeafFilterOperator(PredicateEvaluator predicateEvaluator, DataSource dataSource,
      int numDocs) {
    return getLeafFilterOperator(predicateEvaluator, dataSource, numDocs, false);
  }

  /**
   * Returns the leaf filter operator (i.e. not {@link AndFilterOperator} or {@link OrFilterOperator}).
   */
  public static BaseFilterOperator getLeafFilterOperator(PredicateEvaluator predicateEvaluator, DataSource dataSource,
      int numDocs, boolean nullHandlingEnabled) {
    if (predicateEvaluator.isAlwaysFalse()) {
      return EmptyFilterOperator.getInstance();
    } else if (predicateEvaluator.isAlwaysTrue()) {
      return new MatchAllFilterOperator(numDocs);
    }

    // Currently sorted index based filtering is supported only for
    // dictionary encoded columns. The on-disk segment metadata
    // will indicate if the column is sorted or not regardless of
    // whether it is raw or dictionary encoded. Here when creating
    // the filter operator, we need to make sure that sort filter
    // operator is used only if the column is sorted and has dictionary.
    Predicate.Type predicateType = predicateEvaluator.getPredicateType();
    if (predicateType == Predicate.Type.RANGE) {
      if (dataSource.getDataSourceMetadata().isSorted() && dataSource.getDictionary() != null) {
        return new SortedIndexBasedFilterOperator(predicateEvaluator, dataSource, numDocs);
      }
      if (dataSource.getRangeIndex() != null) {
        return new RangeIndexBasedFilterOperator(predicateEvaluator, dataSource, numDocs);
      }
      return new ScanBasedFilterOperator(predicateEvaluator, dataSource, numDocs, nullHandlingEnabled);
    } else if (predicateType == Predicate.Type.REGEXP_LIKE) {
      if (dataSource.getFSTIndex() != null && dataSource.getDataSourceMetadata().isSorted()) {
        return new SortedIndexBasedFilterOperator(predicateEvaluator, dataSource, numDocs);
      }
      if (dataSource.getFSTIndex() != null && dataSource.getInvertedIndex() != null) {
        return new BitmapBasedFilterOperator(predicateEvaluator, dataSource, numDocs);
      }
      return new ScanBasedFilterOperator(predicateEvaluator, dataSource, numDocs, nullHandlingEnabled);
    } else {
      if (dataSource.getDataSourceMetadata().isSorted() && dataSource.getDictionary() != null) {
        return new SortedIndexBasedFilterOperator(predicateEvaluator, dataSource, numDocs);
      }
      if (dataSource.getInvertedIndex() != null) {
        return new BitmapBasedFilterOperator(predicateEvaluator, dataSource, numDocs);
      }
      return new ScanBasedFilterOperator(predicateEvaluator, dataSource, numDocs, nullHandlingEnabled);
    }
  }

  /**
   * Returns the AND filter operator or equivalent filter operator.
   */
  public static BaseFilterOperator getAndFilterOperator(QueryContext queryContext,
      List<BaseFilterOperator> filterOperators, int numDocs) {
    List<BaseFilterOperator> childFilterOperators = new ArrayList<>(filterOperators.size());
    for (BaseFilterOperator filterOperator : filterOperators) {
      if (filterOperator.isResultEmpty()) {
        return EmptyFilterOperator.getInstance();
      } else if (!filterOperator.isResultMatchingAll()) {
        childFilterOperators.add(filterOperator);
      }
    }
    int numChildFilterOperators = childFilterOperators.size();
    if (numChildFilterOperators == 0) {
      // Return match all filter operator if all child filter operators match all records
      return new MatchAllFilterOperator(numDocs);
    } else if (numChildFilterOperators == 1) {
      // Return the child filter operator if only one left
      return childFilterOperators.get(0);
    } else {
      // Return the AND filter operator with re-ordered child filter operators
      FilterOperatorUtils.reorderAndFilterChildOperators(queryContext, childFilterOperators);
      return new AndFilterOperator(childFilterOperators);
    }
  }

  /**
   * Returns the OR filter operator or equivalent filter operator.
   */
  public static BaseFilterOperator getOrFilterOperator(QueryContext queryContext,
      List<BaseFilterOperator> filterOperators, int numDocs) {
    List<BaseFilterOperator> childFilterOperators = new ArrayList<>(filterOperators.size());
    for (BaseFilterOperator filterOperator : filterOperators) {
      if (filterOperator.isResultMatchingAll()) {
        return new MatchAllFilterOperator(numDocs);
      } else if (!filterOperator.isResultEmpty()) {
        childFilterOperators.add(filterOperator);
      }
    }
    int numChildFilterOperators = childFilterOperators.size();
    if (numChildFilterOperators == 0) {
      // Return empty filter operator if all child filter operators's result is empty
      return EmptyFilterOperator.getInstance();
    } else if (numChildFilterOperators == 1) {
      // Return the child filter operator if only one left
      return childFilterOperators.get(0);
    } else {
      // Return the OR filter operator with child filter operators
      return new OrFilterOperator(childFilterOperators, numDocs);
    }
  }

  /**
   * Returns the NOT filter operator or equivalent filter operator.
   */
  public static BaseFilterOperator getNotFilterOperator(QueryContext queryContext, BaseFilterOperator filterOperator,
      int numDocs) {
    if (filterOperator.isResultMatchingAll()) {
      return EmptyFilterOperator.getInstance();
    } else if (filterOperator.isResultEmpty()) {
      return new MatchAllFilterOperator(numDocs);
    }

    return new NotFilterOperator(filterOperator, numDocs);
  }

  /**
   * For AND filter operator, reorders its child filter operators based on the their cost and puts the ones with
   * inverted index first in order to reduce the number of documents to be processed.
   * <p>Special filter operators such as {@link MatchAllFilterOperator} and {@link EmptyFilterOperator} should be
   * removed from the list before calling this method.
   */
  private static void reorderAndFilterChildOperators(QueryContext queryContext,
      List<BaseFilterOperator> filterOperators) {
    filterOperators.sort(new Comparator<BaseFilterOperator>() {
      @Override
      public int compare(BaseFilterOperator o1, BaseFilterOperator o2) {
        return getPriority(o1) - getPriority(o2);
      }

      int getPriority(BaseFilterOperator filterOperator) {
        if (filterOperator instanceof SortedIndexBasedFilterOperator) {
          return 0;
        }
        if (filterOperator instanceof BitmapBasedFilterOperator) {
          return 1;
        }
        if (filterOperator instanceof RangeIndexBasedFilterOperator
            || filterOperator instanceof TextContainsFilterOperator || filterOperator instanceof TextMatchFilterOperator
            || filterOperator instanceof JsonMatchFilterOperator || filterOperator instanceof H3IndexFilterOperator
            || filterOperator instanceof H3InclusionIndexFilterOperator) {
          return 2;
        }
        if (filterOperator instanceof AndFilterOperator) {
          return 3;
        }
        if (filterOperator instanceof OrFilterOperator) {
          return 4;
        }
        if (filterOperator instanceof NotFilterOperator) {
          return getPriority(((NotFilterOperator) filterOperator).getChildFilterOperator());
        }
        if (filterOperator instanceof ScanBasedFilterOperator) {
          return getScanBasedFilterPriority(queryContext, (ScanBasedFilterOperator) filterOperator, 5);
        }
        if (filterOperator instanceof ExpressionFilterOperator) {
          return 10;
        }
        throw new IllegalStateException(filterOperator.getClass().getSimpleName()
            + " should not be reordered, remove it from the list before calling this method");
      }
    });
  }

  /**
   * Returns the priority for scan based filtering. Multivalue column evaluation is costly, so
   * reorder such that multivalue columns are evaluated after single value columns.
   *
   * TODO: additional cost based prioritization to be added
   *
   * @param scanBasedFilterOperator the filter operator to prioritize
   * @param queryContext query context
   * @return the priority to be associated with the filter
   */
  private static int getScanBasedFilterPriority(QueryContext queryContext,
      ScanBasedFilterOperator scanBasedFilterOperator, int basePriority) {
    if (queryContext.isSkipScanFilterReorder()) {
      return basePriority;
    }

    if (scanBasedFilterOperator.getDataSourceMetadata().isSingleValue()) {
      return basePriority;
    } else {
      // Lower priority for multi-value column
      return basePriority + 1;
    }
  }
}
