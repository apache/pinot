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


public class AbstractPredicateEvaluator implements PredicateEvaluator {

  protected int[] matchingIds;

  public AbstractPredicateEvaluator() {

  }

  @Override
  public boolean apply(int dicId) {
    return Arrays.binarySearch(matchingIds, dicId) >= 0;
  }

  @Override
  public boolean apply(int[] dicIds) {
    if (dicIds.length < matchingIds.length) {
      for (int i = 0; i < dicIds.length; i++) {
        int index = Arrays.binarySearch(matchingIds, dicIds[i]);
        if (index >= 0) {
          return true;
        }
      }
    } else {
      for (int i = 0; i < matchingIds.length; i++) {
        int index = Arrays.binarySearch(dicIds, matchingIds[i]);
        if (index >= 0) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public int[] getDictionaryIds() {
    return matchingIds;
  }
}
