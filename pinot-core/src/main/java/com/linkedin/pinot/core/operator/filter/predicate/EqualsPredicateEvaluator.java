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

import com.linkedin.pinot.core.common.predicate.EqPredicate;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


public class EqualsPredicateEvaluator implements PredicateEvaluator {

  private int[] equalsMatchDicId;
  private int index;

  public EqualsPredicateEvaluator(EqPredicate predicate, Dictionary dictionary) {
    index = dictionary.indexOf(predicate.getEqualsValue());
    if (index >= 0) {
      equalsMatchDicId = new int[1];
      equalsMatchDicId[0] = index;
    } else {
      equalsMatchDicId = new int[0];
    }
  }

  @Override
  public boolean apply(int dicId) {
    return (dicId == index);
  }

  @Override
  public boolean apply(int[] dicIds) {
    if (index < 0) {
      return false;
    }
    Arrays.sort(dicIds);
    int i = Arrays.binarySearch(dicIds, index);
    return i >= 0;
  }

  @Override
  public int[] getDictionaryIds() {
    return equalsMatchDicId;
  }
}
