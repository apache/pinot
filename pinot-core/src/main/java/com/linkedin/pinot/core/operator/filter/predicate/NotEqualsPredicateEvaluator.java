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

import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.common.predicate.NEqPredicate;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


public class NotEqualsPredicateEvaluator extends BasePredicateEvaluator {

  private int neqDictValue;
  private int[] matchingDictIds;
  private Dictionary dictionary;
  private int[] nonMatchingDictIds;
   
  public NotEqualsPredicateEvaluator(NEqPredicate predicate, Dictionary dictionary) {
    this.dictionary = dictionary;
    neqDictValue = dictionary.indexOf(predicate.getNotEqualsValue());
    if (neqDictValue > -1) {
      nonMatchingDictIds = new int[] { neqDictValue };
    } else {
      nonMatchingDictIds = new int[0];
    }
  }

  @Override
  public boolean apply(int dictionaryId) {
    return neqDictValue != dictionaryId;
  }

  @Override
  public boolean apply(int[] dictionaryIds) {
    if (neqDictValue < 0) {
      return true;
    }
    //we cannot do binary search since the multivalue columns are not sorted in the raw segment
    for (int dictId : dictionaryIds) {
      if (dictId == neqDictValue) {
        return false;
      }
    }

    return true;
  }

  @Override
  public int[] getMatchingDictionaryIds() {
    //This is expensive for NOT EQ predicate, some operators need this for now. Eventually we should remove the need for exposing matching dict ids
    if (matchingDictIds == null) {
      int size;
      if (neqDictValue >= 0) {
        size = dictionary.length() - 1;
      } else {
        size = dictionary.length();
      }
      matchingDictIds = new int[size];
      int count = 0;
      for (int i = 0; i < dictionary.length(); i++) {
        if (i != neqDictValue) {
          matchingDictIds[count] = i;
          count = count + 1;
        }
      }
    }
    return matchingDictIds;

  }

  @Override
  public int[] getNonMatchingDictionaryIds() {
    return nonMatchingDictIds;
  }

  @Override
  public boolean apply(int[] dictionaryIds, int length) {
    if (neqDictValue < 0) {
      return true;
    }
    for (int i = 0; i < length; i++) {
      int dictId = dictionaryIds[i];
      if (dictId == neqDictValue) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean alwaysFalse() {
    return nonMatchingDictIds.length == dictionary.length();
  }
  
}
