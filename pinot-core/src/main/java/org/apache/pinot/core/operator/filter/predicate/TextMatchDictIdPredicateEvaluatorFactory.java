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

import org.apache.pinot.common.request.context.predicate.TextMatchPredicate;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/// Factory for TEXT_MATCH predicate evaluators backed by a dictionary-based text index (see
/// [TextIndexReader#isBuildOnDictionary()]). The text index returns the matching dictionary ids, which the
/// standard dictionary-based filter operators (inverted index / scan) resolve to document ids. This mirrors
/// [FSTBasedRegexpPredicateEvaluatorFactory].
public class TextMatchDictIdPredicateEvaluatorFactory {
  private TextMatchDictIdPredicateEvaluatorFactory() {
  }

  /// Creates a predicate evaluator that matches the TEXT_MATCH query using the dictionary-based text index.
  ///
  /// @param textMatchPredicate TEXT_MATCH predicate to evaluate
  /// @param textIndexReader dictionary-based text index reader
  /// @param dictionary dictionary for the column
  /// @return predicate evaluator
  public static BaseDictionaryBasedPredicateEvaluator newDictIdBasedEvaluator(TextMatchPredicate textMatchPredicate,
      TextIndexReader textIndexReader, Dictionary dictionary) {
    return new TextMatchDictIdPredicateEvaluator(textMatchPredicate, textIndexReader, dictionary);
  }

  private static class TextMatchDictIdPredicateEvaluator extends BaseDictionaryBasedPredicateEvaluator {
    final ImmutableRoaringBitmap _matchingDictIdBitmap;

    public TextMatchDictIdPredicateEvaluator(TextMatchPredicate textMatchPredicate, TextIndexReader textIndexReader,
        Dictionary dictionary) {
      super(textMatchPredicate, dictionary);
      _matchingDictIdBitmap = textIndexReader.getDictIds(textMatchPredicate.getValue());
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
