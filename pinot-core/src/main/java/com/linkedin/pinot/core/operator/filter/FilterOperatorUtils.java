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
package com.linkedin.pinot.core.operator.filter;

import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.DataSourceMetadata;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluator;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


public class FilterOperatorUtils {
  private FilterOperatorUtils() {
  }

  /**
   * Get the leaf filter operator (i.e. not {@link AndOperator} or {@link OrOperator}).
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
   * Re-order filter operators based on the their cost. Put the ons with inverted index first so we can process less
   * documents.
   * <p>Special filter operators such as {@link MatchEntireSegmentOperator} and {@link EmptyFilterOperator} should be
   * removed from the list before calling this method.
   */
  public static void reOrderFilterOperators(List<BaseFilterOperator> filterOperators) {
    Collections.sort(filterOperators, new Comparator<BaseFilterOperator>() {
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
        if (filterOperator instanceof AndOperator) {
          return 2;
        }
        if (filterOperator instanceof OrOperator) {
          return 3;
        }
        if (filterOperator instanceof ScanBasedFilterOperator) {
          return 4;
        }
        throw new IllegalStateException(filterOperator.getClass().getSimpleName()
            + " should not be re-ordered, remove it from the list before calling this method");
      }
    });
  }
}
