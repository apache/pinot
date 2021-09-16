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
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import java.util.regex.Pattern;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.request.context.predicate.Predicate;
import org.apache.pinot.spi.request.context.predicate.RegexpLikePredicate;


/**
 * Factory for REGEXP_LIKE predicate evaluators.
 */
public class RegexpLikePredicateEvaluatorFactory {
  private RegexpLikePredicateEvaluatorFactory() {
  }

  /**
   * Create a new instance of dictionary based REGEXP_LIKE predicate evaluator.
   *
   * @param regexpLikePredicate REGEXP_LIKE predicate to evaluate
   * @param dictionary Dictionary for the column
   * @param dataType Data type for the column
   * @return Dictionary based REGEXP_LIKE predicate evaluator
   */
  public static BaseDictionaryBasedPredicateEvaluator newDictionaryBasedEvaluator(
      RegexpLikePredicate regexpLikePredicate, Dictionary dictionary, DataType dataType) {
    Preconditions.checkArgument(dataType == DataType.STRING, "Unsupported data type: " + dataType);
    return new DictionaryBasedRegexpLikePredicateEvaluator(regexpLikePredicate, dictionary);
  }

  /**
   * Create a new instance of raw value based REGEXP_LIKE predicate evaluator.
   *
   * @param regexpLikePredicate REGEXP_LIKE predicate to evaluate
   * @param dataType Data type for the column
   * @return Raw value based REGEXP_LIKE predicate evaluator
   */
  public static BaseRawValueBasedPredicateEvaluator newRawValueBasedEvaluator(RegexpLikePredicate regexpLikePredicate,
      DataType dataType) {
    Preconditions.checkArgument(dataType == DataType.STRING, "Unsupported data type: " + dataType);
    return new RawValueBasedRegexpLikePredicateEvaluator(regexpLikePredicate);
  }

  private static final int PATTERN_FLAG = Pattern.UNICODE_CASE | Pattern.CASE_INSENSITIVE;

  private static final class DictionaryBasedRegexpLikePredicateEvaluator extends BaseDictionaryBasedPredicateEvaluator {
    final Pattern _pattern;
    final Dictionary _dictionary;
    int[] _matchingDictIds;

    public DictionaryBasedRegexpLikePredicateEvaluator(RegexpLikePredicate regexpLikePredicate, Dictionary dictionary) {
      _pattern = Pattern.compile(regexpLikePredicate.getValue(), PATTERN_FLAG);
      _dictionary = dictionary;
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.REGEXP_LIKE;
    }

    @Override
    public boolean applySV(int dictId) {
      return _pattern.matcher(_dictionary.getStringValue(dictId)).find();
    }

    @Override
    public int[] getMatchingDictIds() {
      if (_matchingDictIds == null) {
        IntList matchingDictIds = new IntArrayList();
        int dictionarySize = _dictionary.length();
        for (int dictId = 0; dictId < dictionarySize; dictId++) {
          if (applySV(dictId)) {
            matchingDictIds.add(dictId);
          }
        }
        _matchingDictIds = matchingDictIds.toIntArray();
      }
      return _matchingDictIds;
    }
  }

  private static final class RawValueBasedRegexpLikePredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final Pattern _pattern;

    public RawValueBasedRegexpLikePredicateEvaluator(RegexpLikePredicate regexpLikePredicate) {
      _pattern = Pattern.compile(regexpLikePredicate.getValue(), PATTERN_FLAG);
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.REGEXP_LIKE;
    }

    @Override
    public DataType getDataType() {
      return DataType.STRING;
    }

    @Override
    public boolean applySV(String value) {
      return _pattern.matcher(value).find();
    }
  }
}
