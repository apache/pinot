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

import java.util.Set;
import org.apache.pinot.core.common.Block;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.segment.spi.IndexSegment;


/**
 * A common wrapper around the segment-level operator
 */
public class SegmentOperator extends BaseOperator {
  private static final String OPERATOR_NAME = "SegmentOperator";

  private final Operator _childOperator;
  private final IndexSegment _indexSegment;
  private final Set<String> _columns;

  public SegmentOperator(Operator childOperator, IndexSegment indexSegment, Set<String> columns) {
    _childOperator = childOperator;
    _indexSegment = indexSegment;
    _columns = columns;
  }

  /**
   * Makes a call to acquire column buffers from {@link IndexSegment} before getting nextBlock from childOperator,
   * and
   * a call to release the column buffers from {@link IndexSegment} after.
   */
  @Override
  protected Block getNextBlock() {
    _indexSegment.acquire(_columns);
    Block nextBlock = _childOperator.nextBlock();
    _indexSegment.release(_columns);
    return nextBlock;
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    return _childOperator.getExecutionStatistics();
  }
}
