/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.operator.filter;

import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.DataSourceMetadata;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluator;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;


public class FilterOperatorUtils {
  private FilterOperatorUtils() {
  }

  // Debug option to enable or disable the optimization of reordering the scan filters
  public static final String SCAN_REORDER_OPTIMIZATION = "scanReorderOpt";

  /**
   * Get the leaf filter operator (i.e. not {@link AndFilterOperator} or {@link OrFilterOperator}).
   */
  public static BaseFilterOperator getLeafFilterOperator(PredicateEvaluator predicateEvaluator, DataSource dataSource,
      int startDocId, int endDocId) {
    if (predicateEvaluator.isAlwaysFalse()) {
      return EmptyFilterOperator.getInstance();
    }

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
   * Returns whether to use scan re-order optimization (ON by default) based on the given debug options.
   */
  public static boolean enableScanReorderOptimization(@Nullable Map<String, String> debugOptions) {
    return debugOptions == null || !"false".equalsIgnoreCase(debugOptions.get(SCAN_REORDER_OPTIMIZATION));
  }

  /**
   * Reorders filter operators based on the their cost. Put the ones with inverted index first so we can process less
   * documents.
   * <p>Special filter operators such as {@link MatchEntireSegmentOperator} and {@link EmptyFilterOperator} should be
   * removed from the list before calling this method.
   */
  public static void reorderFilterOperators(List<BaseFilterOperator> filterOperators,
      boolean enableScanReorderOptimization) {
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
          if (enableScanReorderOptimization) {
            return 4 + getScanBasedFilterPriority((ScanBasedFilterOperator) filterOperator);
          } else {
            return 4;
          }
        }
        throw new IllegalStateException(filterOperator.getClass().getSimpleName()
            + " should not be reordered, remove it from the list before calling this method");
      }
    });
  }

  /**
   * Returns the priority of the scan based filter.
   * <p>The priority is determined based on:
   * <ul>
   *   <li>Multi-valued column has lower priority because of its higher reading cost</li>
   * </ul>
   *
   * TODO: additional cost based prioritization to be added
   */
  private static int getScanBasedFilterPriority(ScanBasedFilterOperator scanBasedFilterOperator) {
    DataSourceMetadata dataSourceMetadata = scanBasedFilterOperator.getDataSourceMetadata();

    // Multi-valued column has lower priority
    return dataSourceMetadata.isSingleValue() ? 0 : 1;
  }
}
