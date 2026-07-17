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

import javax.annotation.Nullable;
import org.apache.pinot.query.planner.plannode.WindowNode;


/**
 * Defines the window frame to be used for a window function. The 'lowerBound' and 'upperBound' indicate the frame
 * boundaries to be used. The frame can be of two types: ROWS or RANGE. Optionally an {@code EXCLUDE} clause may
 * specify a subset of rows around the current row to be excluded from the frame.
 *
 * <p>For a RANGE frame, an offset bound (e.g. {@code RANGE BETWEEN 5 PRECEDING AND 5 FOLLOWING}) is a value distance on
 * the single ORDER BY key, not a physical row count. Such an offset value is carried by
 * {@link #getLowerRangeOffset()} / {@link #getUpperRangeOffset()}, and the corresponding int bound is only a sign
 * discriminator: {@code -1} for a
 * PRECEDING offset bound and {@code +1} for a FOLLOWING offset bound (its magnitude is meaningless). UNBOUNDED and
 * CURRENT ROW RANGE bounds continue to use the {@link Integer#MIN_VALUE} / {@link Integer#MAX_VALUE} / {@code 0}
 * sentinels and carry no range offset.
 */
public class WindowFrame {
  // Enum to denote the FRAME type, can be either ROWS or RANGE types
  private final WindowNode.WindowFrameType _type;
  // Both these bounds are relative to current row; 0 means current row, -1 means previous row, 1 means next row, etc.
  // Integer.MIN_VALUE represents UNBOUNDED PRECEDING which is only allowed for the lower bound (ensured by Calcite).
  // Integer.MAX_VALUE represents UNBOUNDED FOLLOWING which is only allowed for the upper bound (ensured by Calcite).
  // For a RANGE offset bound, the int is only a sign discriminator (-1 PRECEDING, +1 FOLLOWING) and the value distance
  // lives in _lowerRangeOffset / _upperRangeOffset.
  private final int _lowerBound;
  private final int _upperBound;
  private final WindowNode.WindowExclusion _exclude;
  // Value distance for a RANGE offset PRECEDING / FOLLOWING bound; null otherwise. When non-null, this is the numeric
  // offset value (Integer / Long / Float / Double / BigDecimal) applied to the single ORDER BY key.
  @Nullable
  private final Number _lowerRangeOffset;
  @Nullable
  private final Number _upperRangeOffset;

  public WindowFrame(WindowNode.WindowFrameType type, int lowerBound, int upperBound,
      WindowNode.WindowExclusion exclude) {
    this(type, lowerBound, upperBound, exclude, null, null);
  }

  public WindowFrame(WindowNode.WindowFrameType type, int lowerBound, int upperBound,
      WindowNode.WindowExclusion exclude, @Nullable Number lowerRangeOffset, @Nullable Number upperRangeOffset) {
    _type = type;
    _lowerBound = lowerBound;
    _upperBound = upperBound;
    _exclude = exclude;
    _lowerRangeOffset = lowerRangeOffset;
    _upperRangeOffset = upperRangeOffset;
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

  public boolean isRangeType() {
    return _type == WindowNode.WindowFrameType.RANGE;
  }

  /**
   * Whether the lower bound is a value-based RANGE offset PRECEDING / FOLLOWING bound.
   */
  public boolean isLowerBoundRangeOffset() {
    return _lowerRangeOffset != null;
  }

  /**
   * Whether the upper bound is a value-based RANGE offset PRECEDING / FOLLOWING bound.
   */
  public boolean isUpperBoundRangeOffset() {
    return _upperRangeOffset != null;
  }

  /**
   * Whether this is a RANGE frame with at least one value-based offset PRECEDING / FOLLOWING bound.
   */
  public boolean isRangeOffsetFrame() {
    return isLowerBoundRangeOffset() || isUpperBoundRangeOffset();
  }

  /**
   * The value distance for the lower RANGE offset bound, or null if the lower bound is not an offset bound.
   */
  @Nullable
  public Number getLowerRangeOffset() {
    return _lowerRangeOffset;
  }

  /**
   * The value distance for the upper RANGE offset bound, or null if the upper bound is not an offset bound.
   */
  @Nullable
  public Number getUpperRangeOffset() {
    return _upperRangeOffset;
  }

  /**
   * Whether the lower RANGE offset bound is PRECEDING (only meaningful when {@link #isLowerBoundRangeOffset()}).
   */
  public boolean isLowerRangeOffsetPreceding() {
    return _lowerBound < 0;
  }

  /**
   * Whether the upper RANGE offset bound is FOLLOWING (only meaningful when {@link #isUpperBoundRangeOffset()}).
   */
  public boolean isUpperRangeOffsetFollowing() {
    return _upperBound > 0;
  }

  public int getLowerBound() {
    return _lowerBound;
  }

  public int getUpperBound() {
    return _upperBound;
  }

  public WindowNode.WindowExclusion getExclude() {
    return _exclude;
  }

  public boolean isExcludeNoOthers() {
    return _exclude == WindowNode.WindowExclusion.NO_OTHERS;
  }

  @Override
  public String toString() {
    return "WindowFrame{type=" + _type + ", lowerBound=" + _lowerBound + ", upperBound=" + _upperBound + ", exclude="
        + _exclude + '}';
  }
}
