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

import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import static org.apache.pinot.common.request.context.predicate.Predicate.Type.REGEXP_LIKE;


/**
 * Factory for REGEXP_LIKE predicate evaluators when FST index is enabled.
 */
public class FSTBasedRegexpPredicateEvaluatorFactory {
  private FSTBasedRegexpPredicateEvaluatorFactory() {
  }

  /**
   * Creates a predicate evaluator which matches the regexp query pattern using
   * FST Index available. FST Index is not yet present for consuming segments,
   * so use newAutomatonBasedEvaluator for consuming segments.
   *
   * @param fstIndexReader FST Index reader
   * @param dictionary Dictionary for the column
   * @param regexpQuery input query to match
   * @return Predicate evaluator
   */
  public static BaseDictionaryBasedPredicateEvaluator newFSTBasedEvaluator(TextIndexReader fstIndexReader,
      Dictionary dictionary, String regexpQuery) {
    return new FSTBasedRegexpPredicateEvaluatorFactory.FSTBasedRegexpPredicateEvaluator(fstIndexReader, dictionary,
        regexpQuery);
  }

  /**
   * Matches regexp query using FSTIndexReader.
   */
  private static class FSTBasedRegexpPredicateEvaluator extends BaseDictionaryBasedPredicateEvaluator {
    private final Dictionary _dictionary;
    private final ImmutableRoaringBitmap _dictIds;

    public FSTBasedRegexpPredicateEvaluator(TextIndexReader fstIndexReader, Dictionary dictionary, String searchQuery) {
      _dictionary = dictionary;
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
    public Predicate.Type getPredicateType() {
      return REGEXP_LIKE;
    }

    @Override
    public boolean applySV(int dictId) {
      return _dictIds.contains(dictId);
    }

    @Override
    public int[] getMatchingDictIds() {
      return _dictIds.toArray();
    }
  }
}
