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
package org.apache.pinot.core.query.reduce;

import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.core.query.reduce.filter.RowMatcher;
import org.apache.pinot.core.query.reduce.filter.RowMatcherFactory;


/**
 * Handler for HAVING clause.
 */
public class HavingFilterHandler {
  private final RowMatcher _rowMatcher;

  public HavingFilterHandler(FilterContext havingFilter, PostAggregationHandler postAggregationHandler,
      boolean nullHandlingEnabled) {
    _rowMatcher = RowMatcherFactory.getRowMatcher(havingFilter, postAggregationHandler, nullHandlingEnabled);
  }

  /**
   * Returns {@code true} if the given row matches the HAVING clause, {@code false} otherwise.
   */
  public boolean isMatch(Object[] row) {
    return _rowMatcher.isMatch(row);
  }
}
