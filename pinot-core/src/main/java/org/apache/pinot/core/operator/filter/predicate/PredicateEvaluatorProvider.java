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

  public static PredicateEvaluator getPredicateEvaluator(Predicate predicate, @Nullable Dictionary dictionary,
      DataType dataType) {
    return getPredicateEvaluator(predicate, dictionary, dataType, null);
  }

  public static PredicateEvaluator getPredicateEvaluator(Predicate predicate, @Nullable Dictionary dictionary,
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

  public static PredicateEvaluator getPredicateEvaluator(Predicate predicate, DataSource dataSource,
      QueryContext queryContext) {
    return getPredicateEvaluator(predicate, getDictionaryUsableForFiltering(dataSource, queryContext, predicate),
        dataSource.getDataSourceMetadata().getDataType(), queryContext);
  }

  /// Returns the column dictionary if the planner can actually use it for filtering this specific predicate, otherwise
  /// `null`.
  ///
  /// When the forward index is RAW, scan-based filtering reads raw values rather than dict IDs, so a dict-based
  /// predicate evaluator (which only implements `applySV(int dictId)`) cannot be applied during scan. The
  /// dictionary is only useful when the planner will pick a dict-consuming filter operator AND that operator is
  /// enabled for this query. The decision must be made per-predicate-type because each predicate type uses a
  /// different subset of dict-consuming operators (see `FilterOperatorUtils.getLeafFilterOperator`):
  ///
  /// - `RANGE`: sorted-index and range-index paths consume the dictionary. The range-index format is chosen at
  ///   segment build time based on whether a dictionary exists (`RangeIndexType#createIndexCreator`) — when a
  ///   dictionary is present, the range index stores dict IDs, even if the forward index is RAW.
  /// - `REGEXP_LIKE`: sorted and inverted paths consume the dictionary (when a dict-id-based regex evaluator is
  ///   built). FST/IFST handle their own evaluators upstream of this method.
  /// - `EQ`: sorted, inverted, and exact-range paths consume the dictionary. Range-index serves EQ only when
  ///   `isExact()` is true (see `RangeIndexBasedFilterOperator#canEvaluate`).
  /// - `NOT_EQ / IN / NOT_IN`: only sorted and inverted paths consume the dictionary.
  ///
  /// If the forward index itself is missing (explicitly disabled), scan is impossible and dict-based eval is the
  /// only option, so the dictionary is preserved regardless of predicate type.
  @Nullable
  public static Dictionary getDictionaryUsableForFiltering(DataSource dataSource, @Nullable QueryContext queryContext,
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
    // this predicate type.
    boolean sortedAvailable = dataSource.getDataSourceMetadata().isSorted()
        && isIndexAllowedForQuery(queryContext, dataSource, FieldConfig.IndexType.SORTED);
    boolean invertedAvailable = dataSource.getInvertedIndex() != null
        && isIndexAllowedForQuery(queryContext, dataSource, FieldConfig.IndexType.INVERTED);
    RangeIndexReader<?> rangeIndex = dataSource.getRangeIndex();
    boolean rangeAvailable = rangeIndex != null
        && isIndexAllowedForQuery(queryContext, dataSource, FieldConfig.IndexType.RANGE);
    switch (predicate.getType()) {
      case RANGE:
        // Range index always serves RANGE predicates; when a dictionary exists the range index is built over dict
        // IDs and requires the dict-based evaluator.
        return (sortedAvailable || rangeAvailable) ? dictionary : null;
      case EQ:
        // Range index serves EQ only when isExact() is true; otherwise EQ falls through to scan.
        return (sortedAvailable || invertedAvailable || (rangeAvailable && rangeIndex.isExact())) ? dictionary : null;
      case REGEXP_LIKE:
      case NOT_EQ:
      case IN:
      case NOT_IN:
        return (sortedAvailable || invertedAvailable) ? dictionary : null;
      default:
        return dictionary;
    }
  }

  private static boolean isIndexAllowedForQuery(@Nullable QueryContext queryContext, DataSource dataSource,
      FieldConfig.IndexType indexType) {
    return queryContext == null || queryContext.isIndexUseAllowed(dataSource, indexType);
  }
}
