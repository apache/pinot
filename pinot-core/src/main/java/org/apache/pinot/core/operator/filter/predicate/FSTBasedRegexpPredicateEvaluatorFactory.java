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

import org.apache.pinot.common.request.context.predicate.RegexpLikePredicate;
import org.apache.pinot.common.utils.RegexpPatternConverterUtils;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/**
 * Factory for REGEXP_LIKE predicate evaluators when FST index is enabled.
 */
public class FSTBasedRegexpPredicateEvaluatorFactory {
  private FSTBasedRegexpPredicateEvaluatorFactory() {
  }

  /**
   * Creates a predicate evaluator which matches the regexp query pattern using FST Index available.
   *
   * @param regexpLikePredicate REGEXP_LIKE predicate to evaluate
   * @param fstIndexReader FST Index reader
   * @param dictionary Dictionary for the column
   * @return Predicate evaluator
   */
  public static BaseDictionaryBasedPredicateEvaluator newFSTBasedEvaluator(RegexpLikePredicate regexpLikePredicate,
      TextIndexReader fstIndexReader, Dictionary dictionary) {
    return new FSTBasedRegexpPredicateEvaluatorFactory.FSTBasedRegexpPredicateEvaluator(regexpLikePredicate,
        fstIndexReader, dictionary);
  }

  /**
   * Matches regexp query using FSTIndexReader.
   */
  private static class FSTBasedRegexpPredicateEvaluator extends BaseDictionaryBasedPredicateEvaluator {
    final Dictionary _dictionary;
    final ImmutableRoaringBitmap _dictIds;

    public FSTBasedRegexpPredicateEvaluator(RegexpLikePredicate regexpLikePredicate, TextIndexReader fstIndexReader,
        Dictionary dictionary) {
      super(regexpLikePredicate);
      _dictionary = dictionary;
      String searchQuery = RegexpPatternConverterUtils.regexpLikeToLuceneRegExp(regexpLikePredicate.getValue());
      _dictIds = fstIndexReader.getDictIds(searchQuery);
    }

    @Override
    public boolean isAlwaysFalse() {
      return _dictIds.isEmpty();
    }

    @Override
    public boolean isAlwaysTrue() {
      return _dictIds.getCardinality() == _dictionary.length();
    }

    @Override
    public boolean applySV(int dictId) {
      return _dictIds.contains(dictId);
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

    @Override
    public int[] getMatchingDictIds() {
      return _dictIds.toArray();
    }
  }
}
