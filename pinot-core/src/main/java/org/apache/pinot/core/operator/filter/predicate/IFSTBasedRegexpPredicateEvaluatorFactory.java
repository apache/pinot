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
 * Factory for REGEXP_LIKE predicate evaluators when IFST index is enabled for case-insensitive matching.
 */
public class IFSTBasedRegexpPredicateEvaluatorFactory {
  private IFSTBasedRegexpPredicateEvaluatorFactory() {
  }

  /**
   * Create a new instance of IFST based REGEXP_LIKE predicate evaluator for case-insensitive matching.
   *
   * @param regexpLikePredicate REGEXP_LIKE predicate to evaluate
   * @param ifstIndexReader IFST index reader
   * @param dictionary Dictionary for the column
   * @return IFST based REGEXP_LIKE predicate evaluator
   */
  public static BaseDictionaryBasedPredicateEvaluator newIFSTBasedEvaluator(RegexpLikePredicate regexpLikePredicate,
      TextIndexReader ifstIndexReader, Dictionary dictionary) {
    return new IFSTBasedRegexpPredicateEvaluator(regexpLikePredicate, ifstIndexReader, dictionary);
  }

  private static class IFSTBasedRegexpPredicateEvaluator extends BaseDictIdBasedRegexpLikePredicateEvaluator {
    final ImmutableRoaringBitmap _matchingDictIdBitmap;

    public IFSTBasedRegexpPredicateEvaluator(RegexpLikePredicate regexpLikePredicate,
        TextIndexReader ifstIndexReader, Dictionary dictionary) {
      super(regexpLikePredicate, dictionary);
      String searchQuery = RegexpPatternConverterUtils.regexpLikeToLuceneRegExp(regexpLikePredicate.getValue());
      _matchingDictIdBitmap = ifstIndexReader.getDictIds(searchQuery);
      int numMatchingDictIds = _matchingDictIdBitmap.getCardinality();
      if (numMatchingDictIds == 0) {
        _alwaysFalse = true;
      } else if (dictionary.length() == numMatchingDictIds) {
        _alwaysTrue = true;
      }
    }

    @Override
    protected int[] calculateMatchingDictIds() {
      return _matchingDictIdBitmap.toArray();
    }

    @Override
    public boolean applySV(int dictId) {
      return _matchingDictIdBitmap.contains(dictId);
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
}
