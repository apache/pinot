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
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.common.DataSourceMetadata;
import org.apache.pinot.core.common.Predicate;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;


public class FilterOperatorUtils {
  private FilterOperatorUtils() {
  }

  // Debug option to enable or disable multi-value optimization
  public static final String USE_SCAN_REORDER_OPTIMIZATION = "useScanReorderOpt";

  /**
   * Returns the leaf filter operator (i.e. not {@link AndFilterOperator} or {@link OrFilterOperator}).
   */
  public static BaseFilterOperator getLeafFilterOperator(PredicateEvaluator predicateEvaluator, DataSource dataSource,
      int numDocs) {
    if (predicateEvaluator.isAlwaysFalse()) {
      return EmptyFilterOperator.getInstance();
    } else if (predicateEvaluator.isAlwaysTrue()) {
      return new MatchAllFilterOperator(numDocs);
    }

    int startDocId = 0;
    // NOTE: end document Id is inclusive
    // TODO: make it exclusive
    int endDocId = numDocs - 1;

    // Use inverted index if the predicate type is not RANGE or REGEXP_LIKE for efficiency
    DataSourceMetadata dataSourceMetadata = dataSource.getDataSourceMetadata();
    Predicate.Type predicateType = predicateEvaluator.getPredicateType();
    if (dataSourceMetadata.hasInvertedIndex() && (predicateType != Predicate.Type.RANGE) && (predicateType
        != Predicate.Type.REGEXP_LIKE)) {
      if (dataSourceMetadata.isSorted()) {
        return new SortedInvertedIndexBasedFilterOperator(predicateEvaluator, dataSource, startDocId, endDocId);
      } else {
        return new BitmapBasedFilterOperator(predicateEvaluator, dataSource, startDocId, endDocId);
      }
    } else {
      return new ScanBasedFilterOperator(predicateEvaluator, dataSource, startDocId, endDocId);
    }
  }

  /**
   * Returns the AND filter operator or equivalent filter operator.
   */
  public static BaseFilterOperator getAndFilterOperator(List<BaseFilterOperator> filterOperators, int numDocs,
      @Nullable Map<String, String> debugOptions) {
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
      FilterOperatorUtils.reorderAndFilterChildOperators(childFilterOperators, debugOptions);
      return new AndFilterOperator(childFilterOperators);
    }
  }

  /**
   * Returns the OR filter operator or equivalent filter operator.
   */
  public static BaseFilterOperator getOrFilterOperator(List<BaseFilterOperator> filterOperators, int numDocs,
      @Nullable Map<String, String> debugOptions) {
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
      return new OrFilterOperator(childFilterOperators);
    }
  }

  /**
   * For AND filter operator, reorders its child filter operators based on the their cost and puts the ones with
   * inverted index first in order to reduce the number of documents to be processed.
   * <p>Special filter operators such as {@link MatchAllFilterOperator} and {@link EmptyFilterOperator} should be
   * removed from the list before calling this method.
   */
  private static void reorderAndFilterChildOperators(List<BaseFilterOperator> filterOperators,
      @Nullable Map<String, String> debugOptions) {
    filterOperators.sort(new Comparator<BaseFilterOperator>() {
      @Override
      public int compare(BaseFilterOperator o1, BaseFilterOperator o2) {
        return getPriority(o1) - getPriority(o2);
      }

      int getPriority(BaseFilterOperator filterOperator) {
        if (filterOperator instanceof SortedInvertedIndexBasedFilterOperator) {
          return 0;
        }
        if (filterOperator instanceof BitmapBasedFilterOperator) {
          return 1;
        }
        if (filterOperator instanceof AndFilterOperator) {
          return 2;
        }
        if (filterOperator instanceof OrFilterOperator) {
          return 3;
        }
        if (filterOperator instanceof ScanBasedFilterOperator) {
          return getScanBasedFilterPriority((ScanBasedFilterOperator) filterOperator, 4, debugOptions);
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
   * @param debugOptions  debug-options to enable/disable the optimization
   * @return the priority to be associated with the filter
   */
  private static int getScanBasedFilterPriority(ScanBasedFilterOperator scanBasedFilterOperator, int basePriority,
      @Nullable Map<String, String> debugOptions) {
    boolean disabled = false;
    if (debugOptions != null
        && StringUtils.compareIgnoreCase(debugOptions.get(USE_SCAN_REORDER_OPTIMIZATION), "false") == 0) {
      disabled = true;
    }
    DataSourceMetadata metadata = scanBasedFilterOperator.getDataSourceMetadata();
    if (disabled || metadata == null || metadata.isSingleValue()) {
      return basePriority;
    }

    // lower priority for multivalue
    return basePriority + 1;
  }
}
