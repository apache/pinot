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
package org.apache.pinot.plugin.filter.example;

import java.util.regex.Pattern;
import org.apache.pinot.core.operator.filter.predicate.BaseDictionaryBasedPredicateEvaluator;
import org.apache.pinot.segment.spi.index.reader.Dictionary;


/**
 * Dictionary-based evaluator for LIKE_ANY predicate.
 * Pre-computes matching dictionary IDs at construction time for efficient filtering.
 */
public class LikeAnyDictionaryEvaluator extends BaseDictionaryBasedPredicateEvaluator {

  private final int[] _matchingDictIds;
  private final int[] _nonMatchingDictIds;
  private final boolean[] _matchLookup;

  public LikeAnyDictionaryEvaluator(LikeAnyPredicate predicate, Pattern pattern, Dictionary dictionary) {
    super(predicate, dictionary);

    // Pre-compute matching dictionary IDs and O(1) lookup table
    int length = dictionary.length();
    int matchCount = 0;
    _matchLookup = new boolean[length];
    for (int i = 0; i < length; i++) {
      String value = dictionary.getStringValue(i);
      if (value != null && pattern.matcher(value).find()) {
        _matchLookup[i] = true;
        matchCount++;
      }
    }

    _matchingDictIds = new int[matchCount];
    _nonMatchingDictIds = new int[length - matchCount];
    int matchIdx = 0;
    int nonMatchIdx = 0;
    for (int i = 0; i < length; i++) {
      if (_matchLookup[i]) {
        _matchingDictIds[matchIdx++] = i;
      } else {
        _nonMatchingDictIds[nonMatchIdx++] = i;
      }
    }

    if (matchCount == 0) {
      _alwaysFalse = true;
    } else if (matchCount == length) {
      _alwaysTrue = true;
    }
  }

  @Override
  public int[] getMatchingDictIds() {
    return _matchingDictIds;
  }

  @Override
  public int[] getNonMatchingDictIds() {
    return _nonMatchingDictIds;
  }

  @Override
  public boolean applySV(int dictId) {
    return _matchLookup[dictId];
  }
}
