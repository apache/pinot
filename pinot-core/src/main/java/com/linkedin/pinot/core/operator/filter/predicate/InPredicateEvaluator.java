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

import java.util.Arrays;

import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.common.predicate.InPredicate;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

public class InPredicateEvaluator extends BasePredicateEvaluator{

  private int[] matchingIds;
  private IntSet dictIdSet;
  private InPredicate predicate;

  public InPredicateEvaluator(InPredicate predicate, Dictionary dictionary) {

    this.predicate = predicate;
    dictIdSet = new IntOpenHashSet();
    final String[] inValues = predicate.getInRange();
    for (final String value : inValues) {
      final int index = dictionary.indexOf(value);
      if (index >= 0) {
        dictIdSet.add(index);
      }
    }
    matchingIds = new int[dictIdSet.size()];
    int i = 0;
    for (int dictId : dictIdSet) {
      matchingIds[i++] = dictId;
    }
    Arrays.sort(matchingIds);
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
    throw new UnsupportedOperationException(
        "Returning non matching values is expensive for predicateType:" + predicate.getType());
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
    return matchingIds == null || matchingIds.length == 0;
  }
  

}
