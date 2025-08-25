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
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.pinot.common.request.context.predicate.RegexpLikePredicate;
import org.apache.pinot.common.utils.regex.Matcher;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Factory for REGEXP_LIKE predicate evaluators.
 */
public class RegexpLikePredicateEvaluatorFactory {
  private RegexpLikePredicateEvaluatorFactory() {
  }

  /// When the cardinality of the dictionary is less than this threshold, scan the dictionary to get the matching ids.
  public static final int DICTIONARY_CARDINALITY_THRESHOLD_FOR_SCAN = 10000;

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
    Preconditions.checkArgument(dataType.getStoredType() == DataType.STRING, "Unsupported data type: " + dataType);
    if (dictionary.length() < DICTIONARY_CARDINALITY_THRESHOLD_FOR_SCAN) {
      return new DictIdBasedRegexpLikePredicateEvaluator(regexpLikePredicate, dictionary);
    } else {
      return new ScanBasedRegexpLikePredicateEvaluator(regexpLikePredicate, dictionary);
    }
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
    Preconditions.checkArgument(dataType.getStoredType() == DataType.STRING, "Unsupported data type: " + dataType);
    return new RawValueBasedRegexpLikePredicateEvaluator(regexpLikePredicate);
  }

  private static final class DictIdBasedRegexpLikePredicateEvaluator
      extends BaseDictIdBasedRegexpLikePredicateEvaluator {
    final IntSet _matchingDictIdSet;

    public DictIdBasedRegexpLikePredicateEvaluator(RegexpLikePredicate regexpLikePredicate, Dictionary dictionary) {
      super(regexpLikePredicate, dictionary);
      Matcher matcher = regexpLikePredicate.getPattern().matcher("");
      IntList matchingDictIds = new IntArrayList();
      int dictionarySize = _dictionary.length();
      for (int dictId = 0; dictId < dictionarySize; dictId++) {
        if (matcher.reset(dictionary.getStringValue(dictId)).find()) {
          matchingDictIds.add(dictId);
        }
      }
      int numMatchingDictIds = matchingDictIds.size();
      if (numMatchingDictIds == 0) {
        _alwaysFalse = true;
      } else if (dictionarySize == numMatchingDictIds) {
        _alwaysTrue = true;
      }
      _matchingDictIds = matchingDictIds.toIntArray();
      _matchingDictIdSet = new IntOpenHashSet(_matchingDictIds);
    }

    @Override
    public int getNumMatchingItems() {
      return _matchingDictIdSet.size();
    }

    @Override
    public boolean applySV(int dictId) {
      return _matchingDictIdSet.contains(dictId);
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

  private static final class ScanBasedRegexpLikePredicateEvaluator extends BaseDictionaryBasedPredicateEvaluator {
    // Reuse matcher to avoid excessive allocation. This is safe to do because the evaluator is always used
    // within the scope of a single thread.
    final Matcher _matcher;

    public ScanBasedRegexpLikePredicateEvaluator(RegexpLikePredicate regexpLikePredicate, Dictionary dictionary) {
      super(regexpLikePredicate, dictionary);
      _matcher = regexpLikePredicate.getPattern().matcher("");
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

  private static final class RawValueBasedRegexpLikePredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    // Reuse matcher to avoid excessive allocation. This is safe to do because the evaluator is always used
    // within the scope of a single thread.
    final Matcher _matcher;

    public RawValueBasedRegexpLikePredicateEvaluator(RegexpLikePredicate regexpLikePredicate) {
      super(regexpLikePredicate);
      _matcher = regexpLikePredicate.getPattern().matcher("");
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
