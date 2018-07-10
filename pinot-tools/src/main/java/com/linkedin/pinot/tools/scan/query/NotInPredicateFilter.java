/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.tools.scan.query;

import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import java.util.HashSet;
import java.util.List;

public class NotInPredicateFilter implements PredicateFilter {
  private static final String SEPARATOR = "\t\t";
  HashSet<Integer> _notInSet = new HashSet<>();

  public NotInPredicateFilter(Dictionary dictionaryReader, List<String> predicateValue) {
    for (String values : predicateValue) {
      for (String value : values.split(SEPARATOR)) {
        _notInSet.add(dictionaryReader.indexOf(value));
      }
    }
  }

  @Override
  public boolean apply(int dictId) {
    return (!_notInSet.contains(dictId));
  }

  @Override
  public boolean apply(int[] dictIds, int length) {
    // length <= dictIds.length
    for (int i = 0; i < length; ++i) {
      if (_notInSet.contains(dictIds[i])) {
        return false;
      }
    }
    return true;
  }
}
