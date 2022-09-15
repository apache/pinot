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
package org.apache.pinot.core.query.reduce.filter;

import java.util.List;
import org.apache.pinot.common.request.context.FilterContext;


/**
 * OR filter matcher.
 */
public class OrRowMatcher implements RowMatcher {
  private final RowMatcher[] _childMatchers;

  public OrRowMatcher(List<FilterContext> childFilters, ValueExtractorFactory valueExtractorFactory,
      boolean nullHandlingEnabled) {
    int numChildren = childFilters.size();
    _childMatchers = new RowMatcher[numChildren];
    for (int i = 0; i < numChildren; i++) {
      _childMatchers[i] = RowMatcherFactory.getRowMatcher(childFilters.get(i), valueExtractorFactory,
          nullHandlingEnabled);
    }
  }

  @Override
  public boolean isMatch(Object[] row) {
    for (RowMatcher childMatcher : _childMatchers) {
      if (childMatcher.isMatch(row)) {
        return true;
      }
    }
    return false;
  }
}
