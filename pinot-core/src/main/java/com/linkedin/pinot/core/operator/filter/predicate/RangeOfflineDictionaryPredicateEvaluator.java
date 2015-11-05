/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.operator.filter.predicate;

import java.util.Arrays;

import com.linkedin.pinot.core.common.predicate.RangePredicate;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;


public class RangeOfflineDictionaryPredicateEvaluator implements PredicateEvaluator {

  private int[] matchingIds;
  private IntSet dictIdSet;

  public RangeOfflineDictionaryPredicateEvaluator(RangePredicate predicate, ImmutableDictionaryReader dictionary) {
    int rangeStartIndex = 0;
    int rangeEndIndex = 0;

    final boolean incLower = predicate.includeLowerBoundary();
    final boolean incUpper = predicate.includeUpperBoundary();
    final String lower = predicate.getLowerBoundary();
    final String upper = predicate.getUpperBoundary();

    if (lower.equals("*")) {
      rangeStartIndex = 0;
    } else {
      rangeStartIndex = dictionary.indexOf(lower);
    }
    if (upper.equals("*")) {
      rangeEndIndex = dictionary.length() - 1;
    } else {
      rangeEndIndex = dictionary.indexOf(upper);
    }

    if (rangeStartIndex < 0) {
      rangeStartIndex = -(rangeStartIndex + 1);
    } else if (!incLower && !lower.equals("*")) {
      rangeStartIndex += 1;
    }

    if (rangeEndIndex < 0) {
      rangeEndIndex = -(rangeEndIndex + 1) - 1;
    } else if (!incUpper && !upper.equals("*")) {
      rangeEndIndex -= 1;
    }

    int size = (rangeEndIndex - rangeStartIndex) + 1;
    if (size < 0) {
      size = 0;
    }

    matchingIds = new int[size];
    dictIdSet = new IntOpenHashSet(size);
    int counter = 0;
    for (int i = rangeStartIndex; i <= rangeEndIndex; i++) {
      matchingIds[counter++] = i;
      dictIdSet.add(i);
    }

  }

  @Override
  public boolean apply(int dictionaryId) {
    return dictIdSet.contains(dictionaryId);
  }

  @Override
  public boolean apply(int[] dictionaryIds) {
    for (int dictId : dictionaryIds) {
      if (dictIdSet.contains(dictId)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public int[] getMatchingDictionaryIds() {
    return matchingIds;
  }}
