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
import java.util.OptionalInt;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.datasource.DataSource;


public class FilterOperatorUtils {

  private static Implementation _instance = new DefaultImplementation();

  private FilterOperatorUtils() {
  }

  public static void setImplementation(Implementation newImplementation) {
    _instance = newImplementation;
  }

  public interface Implementation {

    /**
     * Returns the leaf filter operator (i.e. not {@link AndFilterOperator} or {@link OrFilterOperator}).
     */
    BaseFilterOperator getLeafFilterOperator(PredicateEvaluator predicateEvaluator, DataSource dataSource,
        int numDocs, boolean nullHandlingEnabled);

    /**
     * Returns the AND filter operator or equivalent filter operator.
     */
    BaseFilterOperator getAndFilterOperator(QueryContext queryContext,
        List<BaseFilterOperator> filterOperators, int numDocs);

    /**
     * Returns the OR filter operator or equivalent filter operator.
     */
    BaseFilterOperator getOrFilterOperator(QueryContext queryContext,
        List<BaseFilterOperator> filterOperators, int numDocs);

    /**
     * Returns the NOT filter operator or equivalent filter operator.
     */
    BaseFilterOperator getNotFilterOperator(QueryContext queryContext, BaseFilterOperator filterOperator,
        int numDocs);
  }

  public static class DefaultImplementation implements Implementation {
    @Override
    public BaseFilterOperator getLeafFilterOperator(PredicateEvaluator predicateEvaluator, DataSource dataSource,
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
        if (RangeIndexBasedFilterOperator.canEvaluate(predicateEvaluator, dataSource)) {
          return new RangeIndexBasedFilterOperator(predicateEvaluator, dataSource, numDocs);
        }
        return new ScanBasedFilterOperator(predicateEvaluator, dataSource, numDocs, nullHandlingEnabled);
      } else if (predicateType == Predicate.Type.REGEXP_LIKE) {
        if (dataSource.getFSTIndex() != null && dataSource.getDataSourceMetadata().isSorted()) {
          return new SortedIndexBasedFilterOperator(predicateEvaluator, dataSource, numDocs);
        }
        if (dataSource.getFSTIndex() != null && dataSource.getInvertedIndex() != null) {
          return new InvertedIndexFilterOperator(predicateEvaluator, dataSource, numDocs);
        }
        return new ScanBasedFilterOperator(predicateEvaluator, dataSource, numDocs, nullHandlingEnabled);
      } else {
        if (dataSource.getDataSourceMetadata().isSorted() && dataSource.getDictionary() != null) {
          return new SortedIndexBasedFilterOperator(predicateEvaluator, dataSource, numDocs);
        }
        if (dataSource.getInvertedIndex() != null) {
          return new InvertedIndexFilterOperator(predicateEvaluator, dataSource, numDocs);
        }
        if (RangeIndexBasedFilterOperator.canEvaluate(predicateEvaluator, dataSource)) {
          return new RangeIndexBasedFilterOperator(predicateEvaluator, dataSource, numDocs);
        }
        return new ScanBasedFilterOperator(predicateEvaluator, dataSource, numDocs, nullHandlingEnabled);
      }
    }

    @Override
    public BaseFilterOperator getAndFilterOperator(QueryContext queryContext, List<BaseFilterOperator> filterOperators,
        int numDocs) {
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
        reorderAndFilterChildOperators(queryContext, childFilterOperators);
        return new AndFilterOperator(childFilterOperators, queryContext.getQueryOptions());
      }
    }

    @Override
    public BaseFilterOperator getOrFilterOperator(QueryContext queryContext,
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

    @Override
    public BaseFilterOperator getNotFilterOperator(QueryContext queryContext, BaseFilterOperator filterOperator,
        int numDocs) {
      if (filterOperator.isResultMatchingAll()) {
        return EmptyFilterOperator.getInstance();
      } else if (filterOperator.isResultEmpty()) {
        return new MatchAllFilterOperator(numDocs);
      }

      return new NotFilterOperator(filterOperator, numDocs);
    }


    /**
     * For AND filter operator, reorders its child filter operators based on their cost and puts the ones with
     * inverted index first in order to reduce the number of documents to be processed.
     * <p>Special filter operators such as {@link MatchAllFilterOperator} and {@link EmptyFilterOperator} should be
     * removed from the list before calling this method.
     */
    protected void reorderAndFilterChildOperators(QueryContext queryContext, List<BaseFilterOperator> filterOperators) {
      filterOperators.sort(new Comparator<BaseFilterOperator>() {
        @Override
        public int compare(BaseFilterOperator o1, BaseFilterOperator o2) {
          return getPriority(o1) - getPriority(o2);
        }

        int getPriority(BaseFilterOperator filterOperator) {
          if (filterOperator instanceof PrioritizedFilterOperator) {
            OptionalInt priority = ((PrioritizedFilterOperator<?>) filterOperator).getPriority();
            if (priority.isPresent()) {
              return priority.getAsInt();
            }
          }
          if (filterOperator instanceof SortedIndexBasedFilterOperator) {
            return PrioritizedFilterOperator.HIGH_PRIORITY;
          }
          if (filterOperator instanceof BitmapBasedFilterOperator) {
            return PrioritizedFilterOperator.MEDIUM_PRIORITY;
          }
          if (filterOperator instanceof RangeIndexBasedFilterOperator
              || filterOperator instanceof TextContainsFilterOperator
              || filterOperator instanceof TextMatchFilterOperator || filterOperator instanceof JsonMatchFilterOperator
              || filterOperator instanceof H3IndexFilterOperator
              || filterOperator instanceof H3InclusionIndexFilterOperator) {
            return PrioritizedFilterOperator.LOW_PRIORITY;
          }
          if (filterOperator instanceof AndFilterOperator) {
            return PrioritizedFilterOperator.AND_PRIORITY;
          }
          if (filterOperator instanceof OrFilterOperator) {
            return PrioritizedFilterOperator.OR_PRIORITY;
          }
          if (filterOperator instanceof NotFilterOperator) {
            return getPriority(((NotFilterOperator) filterOperator).getChildFilterOperator());
          }
          if (filterOperator instanceof ScanBasedFilterOperator) {
            int basePriority = PrioritizedFilterOperator.SCAN_PRIORITY;
            return getScanBasedFilterPriority(queryContext, (ScanBasedFilterOperator) filterOperator, basePriority);
          }
          if (filterOperator instanceof ExpressionFilterOperator) {
            return PrioritizedFilterOperator.EXPRESSION_PRIORITY;
          }
          return PrioritizedFilterOperator.UNKNOWN_FILTER_PRIORITY;
        }
      });
    }

    public static int getScanBasedFilterPriority(QueryContext queryContext,
        ScanBasedFilterOperator scanBasedFilterOperator, int basePriority) {
      if (queryContext.isSkipScanFilterReorder()) {
        return basePriority;
      }

      if (scanBasedFilterOperator.getDataSourceMetadata().isSingleValue()) {
        return basePriority;
      } else {
        // Lower priority for multi-value column
        return basePriority + 50;
      }
    }
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
    return _instance.getLeafFilterOperator(predicateEvaluator, dataSource, numDocs, nullHandlingEnabled);
  }

  /**
   * Returns the AND filter operator or equivalent filter operator.
   */
  public static BaseFilterOperator getAndFilterOperator(QueryContext queryContext,
      List<BaseFilterOperator> filterOperators, int numDocs) {
    return _instance.getAndFilterOperator(queryContext, filterOperators, numDocs);
  }

  /**
   * Returns the OR filter operator or equivalent filter operator.
   */
  public static BaseFilterOperator getOrFilterOperator(QueryContext queryContext,
      List<BaseFilterOperator> filterOperators, int numDocs) {
    return _instance.getOrFilterOperator(queryContext, filterOperators, numDocs);
  }

  /**
   * Returns the NOT filter operator or equivalent filter operator.
   */
  public static BaseFilterOperator getNotFilterOperator(QueryContext queryContext, BaseFilterOperator filterOperator,
      int numDocs) {
    return _instance.getNotFilterOperator(queryContext, filterOperator, numDocs);
  }
}
