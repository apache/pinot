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
package org.apache.pinot.query.runtime.operator.window;

import org.apache.pinot.query.planner.plannode.WindowNode;


/**
 * Defines the window frame to be used for a window function. The 'lowerBound' and 'upperBound' indicate the frame
 * boundaries to be used. The frame can be of two types: ROWS or RANGE.
 */
public class WindowFrame {
  // Enum to denote the FRAME type, can be either ROWS or RANGE types
  private final WindowNode.WindowFrameType _type;
  // Both these bounds are relative to current row; 0 means current row, -1 means previous row, 1 means next row, etc.
  // Integer.MIN_VALUE represents UNBOUNDED PRECEDING which is only allowed for the lower bound (ensured by Calcite).
  // Integer.MAX_VALUE represents UNBOUNDED FOLLOWING which is only allowed for the upper bound (ensured by Calcite).
  private final int _lowerBound;
  private final int _upperBound;

  public WindowFrame(WindowNode.WindowFrameType type, int lowerBound, int upperBound) {
    _type = type;
    _lowerBound = lowerBound;
    _upperBound = upperBound;
  }

  public boolean isUnboundedPreceding() {
    return _lowerBound == Integer.MIN_VALUE;
  }

  public boolean isUnboundedFollowing() {
    return _upperBound == Integer.MAX_VALUE;
  }

  public boolean isLowerBoundCurrentRow() {
    return _lowerBound == 0;
  }

  public boolean isUpperBoundCurrentRow() {
    return _upperBound == 0;
  }

  public boolean isRowType() {
    return _type == WindowNode.WindowFrameType.ROWS;
  }

  public int getLowerBound() {
    return _lowerBound;
  }

  public int getUpperBound() {
    return _upperBound;
  }

  @Override
  public String toString() {
    return "WindowFrame{" + "type=" + _type + ", lowerBound=" + _lowerBound + ", upperBound=" + _upperBound + '}';
  }
}
