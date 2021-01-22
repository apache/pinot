/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.tools.scan.query;

import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.utils.CommonConstants.Query.Range;
import org.apache.pinot.core.segment.index.readers.Dictionary;


public class RangePredicateFilter implements PredicateFilter {
  private int _startIndex;
  private int _endIndex;

  public RangePredicateFilter(Dictionary dictionary, List<String> predicateValue) {
    String rangeString = predicateValue.get(0);
    boolean includeStart = rangeString.charAt(0) == Range.LOWER_INCLUSIVE;
    boolean includeEnd = rangeString.charAt(rangeString.length() - 1) == Range.UPPER_INCLUSIVE;

    // Trim the enclosing '[' and ']' as well.
    String[] split = StringUtils.split(rangeString, Range.DELIMITER);
    String start = split[0].substring(1);
    String end = split[1].substring(0, split[1].length() - 1);

    if (start.equals(Range.UNBOUNDED)) {
      _startIndex = 0;
    } else {
      _startIndex = dictionary.indexOf(start);
    }
    if (_startIndex < 0) {
      _startIndex = -(_startIndex + 1);
    } else if (!includeStart) {
      _startIndex++;
    }

    if (end.equals(Range.UNBOUNDED)) {
      _endIndex = dictionary.length() - 1;
    } else {
      _endIndex = dictionary.indexOf(end);
    }
    if (_endIndex < 0) {
      _endIndex = -(_endIndex + 1) - 1;
    } else if (!includeEnd) {
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

  public int getStartIndex() {
    return _startIndex;
  }

  public int getEndIndex() {
    return _endIndex;
  }
}
