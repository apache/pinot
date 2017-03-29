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

import com.linkedin.pinot.core.common.predicate.RangePredicate;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;


public class RangeOfflineDictionaryPredicateEvaluator extends BasePredicateEvaluator {

  private int[] matchingIds;
  private RangePredicate predicate;
  private int rangeStartIndex = 0;
  private int rangeEndIndex = 0;
  int matchingSize;

  public RangeOfflineDictionaryPredicateEvaluator(RangePredicate predicate, ImmutableDictionaryReader dictionary) {
    this.predicate = predicate;

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

    matchingSize = (rangeEndIndex - rangeStartIndex) + 1;
    if (matchingSize < 0) {
      matchingSize = 0;
    }
  }

  @Override
  public boolean apply(int dictionaryId) {
    if (dictionaryId >=  rangeStartIndex && dictionaryId <= rangeEndIndex) {
      return true;
    }
    return false;
  }

  @Override
  public boolean apply(int[] dictionaryIds) {
    for (int dictId : dictionaryIds) {
      if (dictId >= rangeStartIndex && dictId <= rangeEndIndex) {
        return true;
      }
    }
    return false;
  }

  @Override
  public int[] getMatchingDictionaryIds() {
    if (matchingIds == null) {
      matchingIds = new int[matchingSize];
      int counter = 0;
      for (int i = rangeStartIndex; i <= rangeEndIndex; i++) {
        matchingIds[counter++] = i;
      }
    }
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
      if (dictId >= rangeStartIndex && dictId <= rangeEndIndex) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean alwaysFalse() {
    return ((rangeEndIndex - rangeStartIndex) + 1) <= 0;
  }
}
