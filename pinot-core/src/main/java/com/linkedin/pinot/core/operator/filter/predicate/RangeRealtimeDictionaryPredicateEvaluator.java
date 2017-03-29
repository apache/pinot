/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.linkedin.pinot.core.common.predicate.RangePredicate;
import com.linkedin.pinot.core.realtime.impl.dictionary.MutableDictionaryReader;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;


public class RangeRealtimeDictionaryPredicateEvaluator extends BasePredicateEvaluator {

  private int[] matchingIds;
  private IntSet dictIdSet;
  private RangePredicate predicate;

  public RangeRealtimeDictionaryPredicateEvaluator(RangePredicate predicate, MutableDictionaryReader dictionary) {
    this.predicate = predicate;
    List<Integer> ids = new ArrayList<Integer>();
    String rangeStart;
    String rangeEnd;

    if (dictionary.isEmpty()) {
      matchingIds = new int[0];
      return;
    }

    final boolean incLower = predicate.includeLowerBoundary();
    final boolean incUpper = predicate.includeUpperBoundary();
    final String lower = predicate.getLowerBoundary();
    final String upper = predicate.getUpperBoundary();

    if (lower.equals("*")) {
      rangeStart = dictionary.getMinVal().toString();
    } else {
      rangeStart = lower;
    }

    if (upper.equals("*")) {
      rangeEnd = dictionary.getMaxVal().toString();
    } else {
      rangeEnd = upper;
    }

    for (int dicId = 0; dicId < dictionary.length(); dicId++) {
      if (dictionary.inRange(rangeStart, rangeEnd, dicId, incLower, incUpper)) {
        ids.add(dicId);
      }
    }
    matchingIds = new int[ids.size()];
    dictIdSet = new IntOpenHashSet(ids.size());
    for (int i = 0; i < matchingIds.length; i++) {
      matchingIds[i] = ids.get(i);
      dictIdSet.add(ids.get(i));
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
  }

  @Override
  public int[] getNonMatchingDictionaryIds() {
    throw new UnsupportedOperationException("Returning non matching values is expensive for predicateType:" + predicate.getType() );
  }

  @Override
  public boolean apply(int[] dictionaryIds, int length) {
    for (int i = 0; i < length; i++) {
      int dictId = dictionaryIds[i];
      if (dictIdSet.contains(dictId)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean alwaysFalse() {
    return matchingIds.length == 0;
  }

}
