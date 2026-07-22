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

import java.math.BigDecimal;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;


/**
 * Computes, for each row of a partition, the inclusive index range {@code [start, end]} of a value-based RANGE window
 * frame with an offset PRECEDING / FOLLOWING bound (e.g. {@code RANGE BETWEEN 5 PRECEDING AND 5 FOLLOWING}).
 *
 * <p>Unlike a ROWS frame whose bounds are physical row counts, a RANGE frame is defined over the value of the single
 * ORDER BY key: a row {@code R} belongs to the frame of the current row {@code C} iff {@code R}'s order-key value lies
 * in the value interval {@code [lo, hi]} derived from {@code C}'s value and the offsets. The interval endpoints and the
 * inequalities flip depending on the sort direction (ASC vs DESC).
 *
 * <p>The rows are assumed to be already sorted by the single ORDER BY key (the multi-stage window operator receives
 * rows sorted by the collation, and RANGE offset frames require an ORDER BY clause). Because both endpoints move
 * monotonically as the current row advances, the bounds for the whole partition are computed with a two-pointer sweep
 * in {@code O(n)} time.
 *
 * <p>NULL order-key values form their own peer group. A NULL current row's frame is the NULL peer group, extended to
 * the partition start / end when the corresponding bound is UNBOUNDED (matching PostgreSQL semantics). A non-NULL
 * current row never includes NULL rows in its frame.
 */
public final class RangeWindowFrameBounds {
  private RangeWindowFrameBounds() {
  }

  /**
   * Computes the inclusive frame bounds for every row.
   *
   * @param rows the partition rows, sorted by the single ORDER BY key
   * @param orderKeyIndex the column index of the single ORDER BY key
   * @param orderKeyStoredType the stored type of the ORDER BY key column (must be numeric)
   * @param ascending whether the ORDER BY key is sorted ascending
   * @param frame the window frame
   * @return a two-element array {@code {starts, ends}}; for row {@code i} the frame is {@code rows[starts[i]..ends[i]]}
   *         inclusive, and the frame is empty when {@code ends[i] < starts[i]}
   */
  public static int[][] compute(List<Object[]> rows, int orderKeyIndex, ColumnDataType orderKeyStoredType,
      boolean ascending, WindowFrame frame) {
    int numRows = rows.size();
    int[] starts = new int[numRows];
    int[] ends = new int[numRows];

    // Identify the (contiguous) NULL and non-NULL regions. NULLs are grouped at one end of the partition since there is
    // a single ORDER BY key with a single null direction.
    int nonNullLo = -1;
    int nonNullHi = -1;
    int nullLo = -1;
    int nullHi = -1;
    for (int i = 0; i < numRows; i++) {
      if (rows.get(i)[orderKeyIndex] == null) {
        if (nullLo < 0) {
          nullLo = i;
        }
        nullHi = i;
      } else {
        if (nonNullLo < 0) {
          nonNullLo = i;
        }
        nonNullHi = i;
      }
    }

    // UNBOUNDED PRECEDING / FOLLOWING refer to position in the collation order, so they extend the frame to the
    // partition start (index 0) / end (numRows - 1) and thus DO include leading / trailing NULL rows. CURRENT ROW and
    // value-offset bounds are value comparisons that a NULL never satisfies, so they exclude NULL rows.
    boolean unboundedPreceding = frame.isUnboundedPreceding();
    boolean unboundedFollowing = frame.isUnboundedFollowing();

    // NULL current rows: their (NULL) peer group, extended to the partition start / end for UNBOUNDED bounds.
    if (nullLo >= 0) {
      int nullStart = unboundedPreceding ? 0 : nullLo;
      int nullEnd = unboundedFollowing ? numRows - 1 : nullHi;
      for (int i = nullLo; i <= nullHi; i++) {
        starts[i] = nullStart;
        ends[i] = nullEnd;
      }
    }

    if (nonNullLo < 0) {
      // All rows have a NULL order key.
      return new int[][]{starts, ends};
    }

    // Non-NULL current rows: two-pointer sweep of the value-based bounds over the non-NULL region. An UNBOUNDED side
    // instead extends to the partition edge (index 0 / numRows - 1) to include any leading / trailing NULL rows.
    ValueBounds bounds = createValueBounds(rows, orderKeyIndex, orderKeyStoredType, ascending, frame);
    int start = nonNullLo;
    int end = nonNullLo - 1;
    for (int i = nonNullLo; i <= nonNullHi; i++) {
      if (ascending) {
        // Values increase with index: extend the end while rows stay within the upper value edge, then advance the
        // start past rows below the lower value edge.
        while (end + 1 <= nonNullHi && bounds.withinUpper(end + 1, i)) {
          end++;
        }
        while (start <= end && !bounds.withinLower(start, i)) {
          start++;
        }
      } else {
        // Values decrease with index: extend the end while rows stay within the lower value edge, then advance the
        // start past rows above the upper value edge.
        while (end + 1 <= nonNullHi && bounds.withinLower(end + 1, i)) {
          end++;
        }
        while (start <= end && !bounds.withinUpper(start, i)) {
          start++;
        }
      }
      starts[i] = unboundedPreceding ? 0 : start;
      ends[i] = unboundedFollowing ? numRows - 1 : end;
    }
    return new int[][]{starts, ends};
  }

  private static ValueBounds createValueBounds(List<Object[]> rows, int orderKeyIndex,
      ColumnDataType orderKeyStoredType, boolean ascending, WindowFrame frame) {
    switch (orderKeyStoredType) {
      case INT:
      case LONG:
        // Fast path: exact integer arithmetic with no per-row allocation. Falls back to BigDecimal for the (unusual)
        // case of a non-integral offset on an integer order key.
        if (isIntegral(frame.getLowerRangeOffset()) && isIntegral(frame.getUpperRangeOffset())) {
          return new LongValueBounds(rows, orderKeyIndex, ascending, frame);
        }
        return new BigDecimalValueBounds(rows, orderKeyIndex, ascending, frame);
      case BIG_DECIMAL:
        return new BigDecimalValueBounds(rows, orderKeyIndex, ascending, frame);
      case FLOAT:
      case DOUBLE:
        return new DoubleValueBounds(rows, orderKeyIndex, ascending, frame);
      default:
        throw new IllegalStateException(
            "RANGE window frame with offset PRECEDING / FOLLOWING is not supported for ORDER BY key stored type: "
                + orderKeyStoredType);
    }
  }

  /** Whether the offset is null (not an offset bound) or an integral value. */
  private static boolean isIntegral(@Nullable Number offset) {
    if (offset == null || offset instanceof Integer || offset instanceof Long) {
      return true;
    }
    if (offset instanceof BigDecimal) {
      return ((BigDecimal) offset).stripTrailingZeros().scale() <= 0;
    }
    double value = offset.doubleValue();
    return !Double.isInfinite(value) && value == Math.floor(value);
  }

  /** Adds two longs, saturating at {@link Long#MIN_VALUE} / {@link Long#MAX_VALUE} instead of overflowing. */
  private static long saturatingAdd(long a, long b) {
    long result = a + b;
    // Overflow occurred iff a and b share a sign that differs from the result's sign.
    if (((a ^ result) & (b ^ result)) < 0) {
      return b < 0 ? Long.MIN_VALUE : Long.MAX_VALUE;
    }
    return result;
  }

  /**
   * Evaluates whether the order-key value at a given row index falls within the lower / upper value edge of the frame
   * defined by the current row. The value edges are {@code lo = value(cur) + loDelta} and {@code hi = value(cur) +
   * hiDelta} (unless the corresponding side is UNBOUNDED, in which case that edge is always satisfied).
   */
  private interface ValueBounds {
    boolean withinLower(int index, int currentRow);

    boolean withinUpper(int index, int currentRow);
  }

  private static final class LongValueBounds implements ValueBounds {
    private final long[] _values;
    private final boolean _lowerUnbounded;
    private final boolean _upperUnbounded;
    private final long _loDelta;
    private final long _hiDelta;

    private LongValueBounds(List<Object[]> rows, int orderKeyIndex, boolean ascending, WindowFrame frame) {
      int numRows = rows.size();
      _values = new long[numRows];
      for (int i = 0; i < numRows; i++) {
        Object value = rows.get(i)[orderKeyIndex];
        _values[i] = value == null ? 0L : ((Number) value).longValue();
      }
      long lowerOffset = frame.isLowerBoundRangeOffset() ? frame.getLowerRangeOffset().longValue() : 0L;
      long upperOffset = frame.isUpperBoundRangeOffset() ? frame.getUpperRangeOffset().longValue() : 0L;
      long signedLower = frame.isLowerRangeOffsetPreceding() ? -lowerOffset : lowerOffset;
      long signedUpper = frame.isUpperRangeOffsetFollowing() ? upperOffset : -upperOffset;
      if (ascending) {
        _lowerUnbounded = frame.isUnboundedPreceding();
        _loDelta = frame.isLowerBoundCurrentRow() ? 0L : signedLower;
        _upperUnbounded = frame.isUnboundedFollowing();
        _hiDelta = frame.isUpperBoundCurrentRow() ? 0L : signedUpper;
      } else {
        _lowerUnbounded = frame.isUnboundedFollowing();
        _loDelta = frame.isUpperBoundCurrentRow() ? 0L : -signedUpper;
        _upperUnbounded = frame.isUnboundedPreceding();
        _hiDelta = frame.isLowerBoundCurrentRow() ? 0L : -signedLower;
      }
    }

    @Override
    public boolean withinLower(int index, int currentRow) {
      return _lowerUnbounded || _values[index] >= saturatingAdd(_values[currentRow], _loDelta);
    }

    @Override
    public boolean withinUpper(int index, int currentRow) {
      return _upperUnbounded || _values[index] <= saturatingAdd(_values[currentRow], _hiDelta);
    }
  }

  private static final class DoubleValueBounds implements ValueBounds {
    private final double[] _values;
    private final boolean _lowerUnbounded;
    private final boolean _upperUnbounded;
    private final double _loDelta;
    private final double _hiDelta;

    private DoubleValueBounds(List<Object[]> rows, int orderKeyIndex, boolean ascending, WindowFrame frame) {
      int numRows = rows.size();
      _values = new double[numRows];
      for (int i = 0; i < numRows; i++) {
        Object value = rows.get(i)[orderKeyIndex];
        _values[i] = value == null ? 0.0 : ((Number) value).doubleValue();
      }
      double lowerOffset = frame.isLowerBoundRangeOffset() ? frame.getLowerRangeOffset().doubleValue() : 0.0;
      double upperOffset = frame.isUpperBoundRangeOffset() ? frame.getUpperRangeOffset().doubleValue() : 0.0;
      double signedLower = frame.isLowerRangeOffsetPreceding() ? -lowerOffset : lowerOffset;
      double signedUpper = frame.isUpperRangeOffsetFollowing() ? upperOffset : -upperOffset;
      if (ascending) {
        _lowerUnbounded = frame.isUnboundedPreceding();
        _loDelta = frame.isLowerBoundCurrentRow() ? 0.0 : signedLower;
        _upperUnbounded = frame.isUnboundedFollowing();
        _hiDelta = frame.isUpperBoundCurrentRow() ? 0.0 : signedUpper;
      } else {
        _lowerUnbounded = frame.isUnboundedFollowing();
        _loDelta = frame.isUpperBoundCurrentRow() ? 0.0 : -signedUpper;
        _upperUnbounded = frame.isUnboundedPreceding();
        _hiDelta = frame.isLowerBoundCurrentRow() ? 0.0 : -signedLower;
      }
    }

    @Override
    public boolean withinLower(int index, int currentRow) {
      return _lowerUnbounded || _values[index] >= _values[currentRow] + _loDelta;
    }

    @Override
    public boolean withinUpper(int index, int currentRow) {
      return _upperUnbounded || _values[index] <= _values[currentRow] + _hiDelta;
    }
  }

  private static final class BigDecimalValueBounds implements ValueBounds {
    private final BigDecimal[] _values;
    private final boolean _lowerUnbounded;
    private final boolean _upperUnbounded;
    private final BigDecimal _loDelta;
    private final BigDecimal _hiDelta;

    private BigDecimalValueBounds(List<Object[]> rows, int orderKeyIndex, boolean ascending, WindowFrame frame) {
      int numRows = rows.size();
      _values = new BigDecimal[numRows];
      for (int i = 0; i < numRows; i++) {
        Object value = rows.get(i)[orderKeyIndex];
        _values[i] = value == null ? null : toBigDecimal((Number) value);
      }
      BigDecimal lowerOffset =
          frame.isLowerBoundRangeOffset() ? toBigDecimal(frame.getLowerRangeOffset()) : BigDecimal.ZERO;
      BigDecimal upperOffset =
          frame.isUpperBoundRangeOffset() ? toBigDecimal(frame.getUpperRangeOffset()) : BigDecimal.ZERO;
      BigDecimal signedLower = frame.isLowerRangeOffsetPreceding() ? lowerOffset.negate() : lowerOffset;
      BigDecimal signedUpper = frame.isUpperRangeOffsetFollowing() ? upperOffset : upperOffset.negate();
      if (ascending) {
        _lowerUnbounded = frame.isUnboundedPreceding();
        _loDelta = frame.isLowerBoundCurrentRow() ? BigDecimal.ZERO : signedLower;
        _upperUnbounded = frame.isUnboundedFollowing();
        _hiDelta = frame.isUpperBoundCurrentRow() ? BigDecimal.ZERO : signedUpper;
      } else {
        _lowerUnbounded = frame.isUnboundedFollowing();
        _loDelta = frame.isUpperBoundCurrentRow() ? BigDecimal.ZERO : signedUpper.negate();
        _upperUnbounded = frame.isUnboundedPreceding();
        _hiDelta = frame.isLowerBoundCurrentRow() ? BigDecimal.ZERO : signedLower.negate();
      }
    }

    @Override
    public boolean withinLower(int index, int currentRow) {
      return _lowerUnbounded || _values[index].compareTo(_values[currentRow].add(_loDelta)) >= 0;
    }

    @Override
    public boolean withinUpper(int index, int currentRow) {
      return _upperUnbounded || _values[index].compareTo(_values[currentRow].add(_hiDelta)) <= 0;
    }

    private static BigDecimal toBigDecimal(Number value) {
      if (value instanceof BigDecimal) {
        return (BigDecimal) value;
      }
      if (value instanceof Integer || value instanceof Long) {
        return BigDecimal.valueOf(value.longValue());
      }
      return BigDecimal.valueOf(value.doubleValue());
    }
  }
}
