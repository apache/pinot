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

import com.linkedin.pinot.core.common.predicate.EqPredicate;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


public class EqualsPredicateEvaluator implements PredicateEvaluator {

  private int[] matchingIds;
  private int equalsMatchDictId;

  public EqualsPredicateEvaluator(EqPredicate predicate, Dictionary dictionary) {
    equalsMatchDictId = dictionary.indexOf(predicate.getEqualsValue());
    if (equalsMatchDictId >= 0) {
      matchingIds = new int[1];
      matchingIds[0] = equalsMatchDictId;
    } else {
      matchingIds = new int[0];
    }
  }

  @Override
  public boolean apply(int dictionaryId) {
    return (dictionaryId == equalsMatchDictId);
  }

  @Override
  public boolean apply(int[] dictionaryIds) {
    if (equalsMatchDictId < 0) {
      return false;
    }
    //we cannot do binary search since the multivalue columns are not sorted in the raw segment
    for (int dictId : dictionaryIds) {
      if (dictId == equalsMatchDictId) {
        return true;
      }
    }

    return false;
  }

  @Override
  public int[] getMatchingDictionaryIds() {
    return matchingIds;
  }
}
