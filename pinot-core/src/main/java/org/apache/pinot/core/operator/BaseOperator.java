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
package org.apache.pinot.core.operator;

import org.apache.pinot.core.common.Block;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.util.trace.TraceContext;
import org.apache.pinot.spi.exception.EarlyTerminationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Any other Pinot Operators should extend BaseOperator
 */
public abstract class BaseOperator<T extends Block> implements Operator<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseOperator.class);

  @Override
  public final T nextBlock() {
    if (Thread.interrupted()) {
      throw new EarlyTerminationException();
    }
    if (TraceContext.traceEnabled()) {
      long start = System.currentTimeMillis();
      T nextBlock = getNextBlock();
      long end = System.currentTimeMillis();
      String operatorName = getOperatorName();
      LOGGER.trace("Time spent in {}: {}", operatorName, (end - start));
      TraceContext.logTime(operatorName, (end - start));
      return nextBlock;
    } else {
      return getNextBlock();
    }
  }

  // Make it protected because we should always call nextBlock()
  protected abstract T getNextBlock();
}
