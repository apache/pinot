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
package org.apache.pinot.core.operator.filter.predicate;

import com.google.common.annotations.VisibleForTesting;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.InPredicate;
import org.apache.pinot.common.request.context.predicate.NotEqPredicate;
import org.apache.pinot.common.request.context.predicate.NotInPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.request.context.predicate.RangePredicate;
import org.apache.pinot.common.request.context.predicate.RegexpLikePredicate;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.RangeIndexReader;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.exception.BadQueryRequestException;


public class PredicateEvaluatorProvider {
  private PredicateEvaluatorProvider() {
  }

  /// Builds a [PredicateEvaluator] for a leaf filter on the column backed by `dataSource`. The dictionary is derived
  /// via [#getDictionaryUsableForFiltering], which keeps it only when a dict-consuming filter operator (inverted /
  /// exact range) will actually run for this predicate type on the column's forward-index encoding. The data type is
  /// taken from the data-source metadata.
  public static PredicateEvaluator getPredicateEvaluator(Predicate predicate, DataSource dataSource,
      QueryContext queryContext) {
    Dictionary dictionary = getDictionaryUsableForFiltering(dataSource, queryContext, predicate);
    DataType dataType = dataSource.getDataSourceMetadata().getDataType();
    return buildEvaluator(predicate, dictionary, dataType, queryContext);
  }

  /// Builds a [PredicateEvaluator] when the value source and `dictionary` are already in sync by construction: when
  /// `dictionary` is non-null the source produces dict ids decodable by that dictionary; when `dictionary` is null
  /// the source produces raw values. No gating logic runs — the dictionary (if any) is taken as-is, so the caller is
  /// responsible for the match.
  // TODO: Always pass in query context
  public static PredicateEvaluator getPredicateEvaluator(Predicate predicate, @Nullable Dictionary dictionary,
      DataType dataType, @Nullable QueryContext queryContext) {
    return buildEvaluator(predicate, dictionary, dataType, queryContext);
  }

  private static PredicateEvaluator buildEvaluator(Predicate predicate, @Nullable Dictionary dictionary,
      DataType dataType, @Nullable QueryContext queryContext) {
    try {
      if (dictionary != null) {
        // dictionary based predicate evaluators
        switch (predicate.getType()) {
          case EQ:
            return EqualsPredicateEvaluatorFactory.newDictionaryBasedEvaluator((EqPredicate) predicate, dictionary,
                dataType);
          case NOT_EQ:
            return NotEqualsPredicateEvaluatorFactory.newDictionaryBasedEvaluator((NotEqPredicate) predicate,
                dictionary, dataType);
          case IN:
            return InPredicateEvaluatorFactory.newDictionaryBasedEvaluator((InPredicate) predicate, dictionary,
                dataType, queryContext);
          case NOT_IN:
            return NotInPredicateEvaluatorFactory.newDictionaryBasedEvaluator((NotInPredicate) predicate, dictionary,
                dataType, queryContext);
          case RANGE:
            return RangePredicateEvaluatorFactory.newDictionaryBasedEvaluator((RangePredicate) predicate, dictionary,
                dataType);
          case REGEXP_LIKE:
            return RegexpLikePredicateEvaluatorFactory.newDictionaryBasedEvaluator((RegexpLikePredicate) predicate,
                dictionary, dataType, queryContext);
          default:
            throw new UnsupportedOperationException("Unsupported predicate type: " + predicate.getType());
        }
      } else {
        // raw value based predicate evaluators
        switch (predicate.getType()) {
          case EQ:
            return EqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator((EqPredicate) predicate, dataType);
          case NOT_EQ:
            return NotEqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator((NotEqPredicate) predicate, dataType);
          case IN:
            return InPredicateEvaluatorFactory.newRawValueBasedEvaluator((InPredicate) predicate, dataType);
          case NOT_IN:
            return NotInPredicateEvaluatorFactory.newRawValueBasedEvaluator((NotInPredicate) predicate, dataType);
          case RANGE:
            return RangePredicateEvaluatorFactory.newRawValueBasedEvaluator((RangePredicate) predicate, dataType);
          case REGEXP_LIKE:
            return RegexpLikePredicateEvaluatorFactory
                .newRawValueBasedEvaluator((RegexpLikePredicate) predicate, dataType);
          default:
            throw new UnsupportedOperationException("Unsupported predicate type: " + predicate.getType());
        }
      }
    } catch (Exception e) {
      // Exception here is caused by mismatch between the column data type and the predicate value in the query
      throw new BadQueryRequestException(e);
    }
  }

  /// Returns the column dictionary if the planner can actually use it for filtering this specific predicate, otherwise
  /// `null`.
  ///
  /// When the forward index is RAW, scan-based filtering reads raw values rather than dict IDs, so a dict-based
  /// predicate evaluator (which only implements `applySV(int dictId)`) cannot be applied during scan. The
  /// dictionary is only useful when the planner will pick a dict-consuming filter operator that resolves the
  /// predicate fully (no scan fallback). The decision is per-predicate-type because each predicate type uses a
  /// different subset of dict-consuming operators (see `FilterOperatorUtils.getLeafFilterOperator`):
  ///
  /// - `RANGE`: only the range-index path can consume the dictionary on a RAW column. (Sorted index implies a
  ///   dict-encoded forward index, so it cannot apply here.) Furthermore, the range index must be exact —
  ///   non-exact (legacy) range readers fall back to `ScanBasedFilterOperator` for partial matches, which feeds
  ///   raw forward-index values into the predicate evaluator and breaks if the evaluator is dict-based.
  /// - `REGEXP_LIKE / NOT_EQ / IN / NOT_IN`: only the inverted-index path consumes the dictionary on a RAW
  ///   column. FST/IFST handle their own evaluators upstream of this method.
  /// - `EQ`: inverted index, plus exact range index (see `RangeIndexBasedFilterOperator#canEvaluate`).
  ///
  /// If the forward index itself is missing (explicitly disabled), scan is impossible and dict-based eval is the
  /// only option, so the dictionary is preserved regardless of predicate type.
  @VisibleForTesting
  @Nullable
  static Dictionary getDictionaryUsableForFiltering(DataSource dataSource, @Nullable QueryContext queryContext,
      Predicate predicate) {
    Dictionary dictionary = dataSource.getDictionary();
    if (dictionary == null) {
      return null;
    }
    ForwardIndexReader<?> forwardIndex = dataSource.getForwardIndex();
    if (forwardIndex == null || forwardIndex.isDictionaryEncoded()) {
      return dictionary;
    }
    // RAW forward index: keep the dictionary only if a dict-consuming filter operator is available AND enabled for
    // this predicate type. NOTE: a sorted forward index is run-length dict-encoded by definition, so the sorted
    // path is unreachable from this RAW branch — only inverted and (exact) range remain.
    boolean invertedAvailable = dataSource.getInvertedIndex() != null
        && isIndexAllowedForQuery(queryContext, dataSource, FieldConfig.IndexType.INVERTED);
    RangeIndexReader<?> rangeIndex = dataSource.getRangeIndex();
    boolean exactRangeAvailable = rangeIndex != null && rangeIndex.isExact()
        && isIndexAllowedForQuery(queryContext, dataSource, FieldConfig.IndexType.RANGE);
    switch (predicate.getType()) {
      case RANGE:
        return exactRangeAvailable ? dictionary : null;
      case EQ:
        return (invertedAvailable || exactRangeAvailable) ? dictionary : null;
      case REGEXP_LIKE:
      case NOT_EQ:
      case IN:
      case NOT_IN:
        return invertedAvailable ? dictionary : null;
      default:
        return dictionary;
    }
  }

  private static boolean isIndexAllowedForQuery(@Nullable QueryContext queryContext, DataSource dataSource,
      FieldConfig.IndexType indexType) {
    return queryContext == null || queryContext.isIndexUseAllowed(dataSource, indexType);
  }
}
