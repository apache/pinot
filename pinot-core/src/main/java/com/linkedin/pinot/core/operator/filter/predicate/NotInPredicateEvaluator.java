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
import java.util.HashSet;
import java.util.Set;

import com.linkedin.pinot.core.common.predicate.NotInPredicate;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;


public class NotInPredicateEvaluator implements PredicateEvaluator {

  private int[] matchingIds;
  private Dictionary dictionary;
  private IntSet nonMatchingDictIdSet;

  public NotInPredicateEvaluator(NotInPredicate predicate, Dictionary dictionary) {
    this.dictionary = dictionary;
    final String[] notInValues = predicate.getNotInRange();
    nonMatchingDictIdSet = new IntOpenHashSet(notInValues.length);
    for (int i = 0; i < notInValues.length; i++) {
      final String notInValue = notInValues[i];
      int dictId = dictionary.indexOf(notInValue);
      if(dictId>=0){
        nonMatchingDictIdSet.add(dictId);
      }
    }
  }

  @Override
  public boolean apply(int dictionaryId) {
    return (!nonMatchingDictIdSet.contains(dictionaryId));
  }

  @Override
  public boolean apply(int[] dictionaryIds) {
    for (int dictId : dictionaryIds) {
      if (nonMatchingDictIdSet.contains(dictId)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int[] getMatchingDictionaryIds() {
    //This is expensive for NOT IN predicate, some operators need this for now. Eventually we should remove the need for exposing matching dict ids
    if (matchingIds == null) {
      int count = 0;
      matchingIds = new int[dictionary.length() - nonMatchingDictIdSet.size()];
      for (int i = 0; i < dictionary.length(); i++) {
        if (!nonMatchingDictIdSet.contains(i)) {
          matchingIds[count] = i;
          count = count + 1;
        }
      }
    }
    return matchingIds;
  }
}
