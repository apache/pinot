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
package com.linkedin.pinot.tools.scan.query;

import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import java.util.List;


public class RangePredicateFilter implements PredicateFilter {
  private static final String SEPARATOR = "\t\t";
  private int _startIndex;
  private int _endIndex;
  private final Dictionary _dictionary;
  private final boolean _includeStart;
  private final boolean _includeEnd;

  public RangePredicateFilter(Dictionary dictionaryReader, List<String> predicateValue) {
    _dictionary = dictionaryReader;
    String[] values = predicateValue.get(0).split(SEPARATOR);
    final String rangeString = predicateValue.get(0).trim();

    // Trim the enclosing '[' and ']' as well.
    String start = values[0].substring(1, values[0].length());
    String end = values[1].substring(0, values[1].length() - 1);

    _includeStart = !rangeString.trim().startsWith("(") || start.equals("*");
    _includeEnd = !rangeString.trim().endsWith(")") || end.equals("*");

    if (start.equals("*")) {
      _startIndex = 0;
    } else {
      _startIndex = _dictionary.indexOf(start);
    }
    if (_startIndex < 0) {
      _startIndex = -(_startIndex + 1);
    } else if (!_includeStart) {
      _startIndex++;
    }

    if (end.equals("*")) {
      _endIndex = _dictionary.length() - 1;
    } else {
      _endIndex = _dictionary.indexOf(end);
    }
    if (_endIndex < 0) {
      _endIndex = -(_endIndex + 1) - 1;
    } else if (!_includeEnd) {
      --_endIndex;
    }
  }

  @Override
  public boolean apply(int dictId) {
    return ((dictId >= _startIndex) && (dictId <= _endIndex));
  }

  @Override
  public boolean apply(int[] dictIds, int length) {
    // length <= dictIds.length
    for (int i = 0; i < length; ++i) {
      if (apply(dictIds[i])) {
        return true;
      }
    }
    return false;
  }
}
