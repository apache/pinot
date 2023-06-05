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
package org.apache.pinot.core.query.aggregation.function;

import java.util.List;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.core.query.request.context.QueryContext;


/**
 * The <code>AggFunctionQueryContext</code> class contains extracted details from QueryContext that can be used for
 * Aggregation Functions.
 */
public class AggFunctionQueryContext {
  private boolean _isNullHandlingEnabled;
  private List<OrderByExpressionContext> _orderByExpressions = null;
  private int _limit;

  /**
   * Extracts necessary fields from QueryContext
   */
  public AggFunctionQueryContext(QueryContext queryContext) {
    _isNullHandlingEnabled = queryContext.isNullHandlingEnabled();
    _orderByExpressions = queryContext.getOrderByExpressions();
    _limit = queryContext.getLimit();
  }

  /**
   * Called from Multistage AggregateOperator to set the necessary context.
   */
  public AggFunctionQueryContext(boolean isNullHandlingEnabled) {
    _isNullHandlingEnabled = isNullHandlingEnabled;
  }

  public boolean isNullHandlingEnabled() {
    return _isNullHandlingEnabled;
  }

  public List<OrderByExpressionContext> getOrderByExpressions() {
    return _orderByExpressions;
  }

  public int getLimit() {
    return _limit;
  }
}
