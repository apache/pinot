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

import java.util.ArrayList;
import java.util.List;

import com.linkedin.pinot.core.common.predicate.RangePredicate;
import com.linkedin.pinot.core.realtime.impl.dictionary.MutableDictionaryReader;


public class RangeRealtimeDictionaryPredicateEvaluator extends AbstractPredicateEvaluator {

  public RangeRealtimeDictionaryPredicateEvaluator(RangePredicate predicate, MutableDictionaryReader dictionary) {
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
    for (int i = 0; i < matchingIds.length; i++) {
      matchingIds[i] = ids.get(i);
    }
  }
}
