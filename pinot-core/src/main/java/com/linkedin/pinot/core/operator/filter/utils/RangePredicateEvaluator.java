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
package com.linkedin.pinot.core.operator.filter.utils;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.pinot.core.common.predicate.RangePredicate;
import com.linkedin.pinot.core.realtime.impl.dictionary.MutableDictionaryReader;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


public class RangePredicateEvaluator {
  private static final RangePredicateEvaluator INSTANCE;
  static {
    INSTANCE = new RangePredicateEvaluator();
  }

  private RangePredicateEvaluator() {

  }

  public List<Integer> evalRangeDicIdsFromMutableDictionary(MutableDictionaryReader dictionary, RangePredicate predicate) {
    List<Integer> ids = new ArrayList<Integer>();
    String rangeStart = "";
    String rangeEnd = "";

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
    return ids;
  }

  /**
   * @param dictionary
   * @param predicate
   * @return array of size 2, with range start,end.
   */
  public int[] evalStartEndIndex(Dictionary dictionary, RangePredicate predicate) {
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
    return new int[] { rangeStartIndex, rangeEndIndex };
  }

  public static RangePredicateEvaluator get() {
    return INSTANCE;
  }
}
