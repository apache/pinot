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

import com.google.common.base.Preconditions;
import org.apache.pinot.common.request.context.predicate.RegexpLikeCiPredicate;
import org.apache.pinot.common.utils.regex.Matcher;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Factory for REGEXP_LIKE_CI predicate evaluators.
 */
public class RegexpLikeCiPredicateEvaluatorFactory {
  private RegexpLikeCiPredicateEvaluatorFactory() {
  }

  /**
   * Create a new instance of dictionary based REGEXP_LIKE_CI predicate evaluator.
   *
   * @param regexpLikeCiPredicate REGEXP_LIKE_CI predicate to evaluate
   * @param dictionary Dictionary for the column
   * @param dataType Data type for the column
   * @return Dictionary based REGEXP_LIKE_CI predicate evaluator
   */
  public static BaseDictionaryBasedPredicateEvaluator newDictionaryBasedEvaluator(
      RegexpLikeCiPredicate regexpLikeCiPredicate, Dictionary dictionary, DataType dataType) {
    Preconditions.checkArgument(dataType == DataType.STRING, "Unsupported data type: " + dataType);
    return new DictionaryBasedRegexpLikeCiPredicateEvaluator(regexpLikeCiPredicate, dictionary);
  }

  /**
   * Create a new instance of raw value based REGEXP_LIKE_CI predicate evaluator.
   *
   * @param regexpLikeCiPredicate REGEXP_LIKE_CI predicate to evaluate
   * @param dataType Data type for the column
   * @return Raw value based REGEXP_LIKE_CI predicate evaluator
   */
  public static BaseRawValueBasedPredicateEvaluator newRawValueBasedEvaluator(
      RegexpLikeCiPredicate regexpLikeCiPredicate, DataType dataType) {
    Preconditions.checkArgument(dataType == DataType.STRING, "Unsupported data type: " + dataType);
    return new RawValueBasedRegexpLikeCiPredicateEvaluator(regexpLikeCiPredicate);
  }

  private static final class DictionaryBasedRegexpLikeCiPredicateEvaluator
      extends BaseDictionaryBasedPredicateEvaluator {
    // Reuse matcher to avoid excessive allocation. This is safe to do because the evaluator is always used
    // within the scope of a single thread.
    final Matcher _matcher;

    public DictionaryBasedRegexpLikeCiPredicateEvaluator(RegexpLikeCiPredicate regexpLikeCiPredicate,
        Dictionary dictionary) {
      super(regexpLikeCiPredicate, dictionary);
      _matcher = regexpLikeCiPredicate.getPattern().matcher("");
    }

    @Override
    public boolean applySV(int dictId) {
      return _matcher.reset(_dictionary.getStringValue(dictId)).find();
    }

    @Override
    public int applySV(int limit, int[] docIds, int[] values) {
      // reimplemented here to ensure applySV can be inlined
      int matches = 0;
      for (int i = 0; i < limit; i++) {
        int value = values[i];
        if (applySV(value)) {
          docIds[matches++] = docIds[i];
        }
      }
      return matches;
    }
  }

  private static final class RawValueBasedRegexpLikeCiPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    // Reuse matcher to avoid excessive allocation. This is safe to do because the evaluator is always used
    // within the scope of a single thread.
    final Matcher _matcher;

    public RawValueBasedRegexpLikeCiPredicateEvaluator(RegexpLikeCiPredicate regexpLikeCiPredicate) {
      super(regexpLikeCiPredicate);
      _matcher = regexpLikeCiPredicate.getPattern().matcher("");
    }

    @Override
    public DataType getDataType() {
      return DataType.STRING;
    }

    @Override
    public boolean applySV(String value) {
      return _matcher.reset(value).find();
    }
  }
}
