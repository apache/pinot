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
package org.apache.pinot.tsdb.spi.operator;

import java.util.List;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBlock;


/**
 * Every time-series operator takes in a {@link TimeSeriesBlock} and returns another {@link TimeSeriesBlock}.
 * Parent operators/callers must call {@link #nextBlock()} to get the next block from the child operators, and implement
 * {@link #getNextBlock()} to implement the business logic for their operator. Also see {@link BaseTimeSeriesPlanNode}.
 * TODO: Add common hierarchy with other operators like Multistage and Pinot core. This will likely require us to
 *   define a pinot-query-spi or add/move some abstractions to pinot-spi.
 */
public abstract class BaseTimeSeriesOperator {
  protected final List<BaseTimeSeriesOperator> _childOperators;

  public BaseTimeSeriesOperator(List<BaseTimeSeriesOperator> childOperators) {
    _childOperators = childOperators;
  }

  /**
   * Called by parent time-series operators.
   */
  public final TimeSeriesBlock nextBlock() {
    long startTime = System.currentTimeMillis();
    try {
      return getNextBlock();
    } finally {
      // TODO: add stats
    }
  }

  public List<BaseTimeSeriesOperator> getChildOperators() {
    return _childOperators;
  }

  /**
   * Time series query languages can implement their own business logic in their operators.
   */
  public abstract TimeSeriesBlock getNextBlock();

  /**
   * Name that will show up in the explain plan.
   */
  public abstract String getExplainName();
}
