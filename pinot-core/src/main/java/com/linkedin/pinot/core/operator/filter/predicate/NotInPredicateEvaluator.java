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

import com.linkedin.pinot.core.common.predicate.NotInPredicate;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


public class NotInPredicateEvaluator extends AbstractPredicateEvaluator {

  public NotInPredicateEvaluator(NotInPredicate predicate, Dictionary dictionary) {
    List<Integer> dictIds = new ArrayList<Integer>();
    final String[] notInValues = predicate.getNotInRange();
    final List<Integer> notInIds = new ArrayList<Integer>();
    for (final String notInValue : notInValues) {
      notInIds.add(new Integer(dictionary.indexOf(notInValue)));
    }
    for (int i = 0; i < dictionary.length(); i++) {
      if (!notInIds.contains(new Integer(i))) {
        dictIds.add(i);
      }
    }
    matchingIds = new int[dictIds.size()];
    for (int i = 0; i < matchingIds.length; i++) {
      matchingIds[i] = dictIds.get(i);
    }
  }
}
