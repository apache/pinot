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

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.plannode.WindowNode.WindowExclusion;
import org.apache.pinot.query.planner.plannode.WindowNode.WindowFrameType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Unit tests for {@link RangeWindowFrameBounds}, the O(n) two-pointer computation of value-based RANGE offset window
 * frame boundaries. The order key is always column 0. Frames are asserted as normalized inclusive index ranges, where
 * an empty frame is represented by {@code {}} (the exact start/end of an empty frame is an implementation detail).
 */
public class RangeWindowFrameBoundsTest {

  private static List<Object[]> rows(Object... orderKeyValues) {
    List<Object[]> rows = new ArrayList<>(orderKeyValues.length);
    for (Object value : orderKeyValues) {
      rows.add(new Object[]{value});
    }
    return rows;
  }

  /**
   * Builds a RANGE offset frame. {@code lowerSign} / {@code upperSign} are the int-bound discriminators (-1 PRECEDING,
   * +1 FOLLOWING, 0 CURRENT ROW, MIN_VALUE UNBOUNDED PRECEDING, MAX_VALUE UNBOUNDED FOLLOWING).
   */
  private static WindowFrame frame(int lowerSign, Number lowerOffset, int upperSign, Number upperOffset) {
    return new WindowFrame(WindowFrameType.RANGE, lowerSign, upperSign, WindowExclusion.NO_OTHERS, lowerOffset,
        upperOffset);
  }

  private static void assertFrames(List<Object[]> rows, ColumnDataType type, boolean ascending, WindowFrame frame,
      int[][] expectedFrames) {
    int[][] bounds = RangeWindowFrameBounds.compute(rows, 0, type, ascending, frame);
    int[] starts = bounds[0];
    int[] ends = bounds[1];
    int numRows = rows.size();
    assertEquals(starts.length, numRows);
    assertEquals(ends.length, numRows);
    for (int i = 0; i < numRows; i++) {
      int[] actual = starts[i] > ends[i] ? new int[]{} : new int[]{starts[i], ends[i]};
      assertEquals(actual, expectedFrames[i], "frame mismatch at row " + i);
      // Bounds should stay within the partition.
      if (starts[i] <= ends[i]) {
        assertTrue(starts[i] >= 0 && ends[i] < numRows, "out-of-range frame at row " + i);
      }
    }
  }

  @Test
  public void testAscBetweenOnePrecedingAndOneFollowing() {
    // values: 1, 2, 2, 5, 7  |  frame value interval = [v-1, v+1]
    assertFrames(rows(1, 2, 2, 5, 7), ColumnDataType.INT, true, frame(-1, 1, 1, 1),
        new int[][]{{0, 2}, {0, 2}, {0, 2}, {3, 3}, {4, 4}});
  }

  @Test
  public void testDescBetweenOnePrecedingAndOneFollowing() {
    // values sorted desc: 7, 5, 2, 2, 1  |  symmetric frame, value interval still [v-1, v+1]
    assertFrames(rows(7, 5, 2, 2, 1), ColumnDataType.INT, false, frame(-1, 1, 1, 1),
        new int[][]{{0, 0}, {1, 1}, {2, 4}, {2, 4}, {2, 4}});
  }

  @Test
  public void testAscBetweenTwoPrecedingAndCurrentRow() {
    // values: 1, 2, 3, 10  |  frame = [v-2, v]
    assertFrames(rows(1, 2, 3, 10), ColumnDataType.INT, true, frame(-1, 2, 0, null),
        new int[][]{{0, 0}, {0, 1}, {0, 2}, {3, 3}});
  }

  @Test
  public void testAscBetweenCurrentRowAndTwoFollowing() {
    // values: 1, 2, 3, 10  |  frame = [v, v+2]
    assertFrames(rows(1, 2, 3, 10), ColumnDataType.INT, true, frame(0, null, 1, 2),
        new int[][]{{0, 2}, {1, 2}, {2, 2}, {3, 3}});
  }

  @Test
  public void testAscEmptyFrameBetweenOnePrecedingAndOnePreceding() {
    // values: 1, 2, 3  |  frame = [v-1, v-1]; the first row's frame is empty
    assertFrames(rows(1, 2, 3), ColumnDataType.INT, true, frame(-1, 1, -1, 1),
        new int[][]{{}, {0, 0}, {1, 1}});
  }

  @Test
  public void testAscBothFollowingWithTrailingEmptyFrame() {
    // values: 1, 2, 3, 4  |  frame = [v+1, v+2] (excludes current row); last row's frame is empty
    assertFrames(rows(1, 2, 3, 4), ColumnDataType.INT, true, frame(1, 1, 1, 2),
        new int[][]{{1, 2}, {2, 3}, {3, 3}, {}});
  }

  @Test
  public void testAscNullsFirstBetweenOnePrecedingAndOneFollowing() {
    // values: null, null, 1, 2, 3; NULL rows form their own peer group [0,1]
    assertFrames(rows(null, null, 1, 2, 3), ColumnDataType.INT, true, frame(-1, 1, 1, 1),
        new int[][]{{0, 1}, {0, 1}, {2, 3}, {2, 4}, {3, 4}});
  }

  @Test
  public void testAscNullsFirstUnboundedPrecedingAndOneFollowing() {
    // values: null, null, 1, 2, 3; UNBOUNDED PRECEDING extends to index 0, including the leading NULL rows
    assertFrames(rows(null, null, 1, 2, 3), ColumnDataType.INT, true, frame(Integer.MIN_VALUE, null, 1, 1),
        new int[][]{{0, 1}, {0, 1}, {0, 3}, {0, 4}, {0, 4}});
  }

  @Test
  public void testAscNullsLastOnePrecedingAndUnboundedFollowing() {
    // values: 1, 2, 3, null, null; UNBOUNDED FOLLOWING extends to index n-1, including the trailing NULL rows
    assertFrames(rows(1, 2, 3, null, null), ColumnDataType.INT, true, frame(-1, 1, Integer.MAX_VALUE, null),
        new int[][]{{0, 4}, {0, 4}, {1, 4}, {3, 4}, {3, 4}});
  }

  @Test
  public void testDoubleOrderKey() {
    // values: 1.0, 1.5, 2.0, 4.0  |  frame = [v-0.5, v+0.5]
    assertFrames(rows(1.0, 1.5, 2.0, 4.0), ColumnDataType.DOUBLE, true, frame(-1, 0.5, 1, 0.5),
        new int[][]{{0, 1}, {0, 2}, {1, 2}, {3, 3}});
  }

  @Test
  public void testBigDecimalOrderKey() {
    // values (BIG_DECIMAL): 1, 2, 2, 5, 7  |  frame = [v-1, v+1]
    assertFrames(rows(new java.math.BigDecimal(1), new java.math.BigDecimal(2), new java.math.BigDecimal(2),
            new java.math.BigDecimal(5), new java.math.BigDecimal(7)), ColumnDataType.BIG_DECIMAL, true,
        frame(-1, 1, 1, 1), new int[][]{{0, 2}, {0, 2}, {0, 2}, {3, 3}, {4, 4}});
  }

  @Test
  public void testAllNullOrderKeys() {
    // All rows are NULL peers -> the frame is the whole (NULL) partition
    assertFrames(rows(null, null, null), ColumnDataType.INT, true, frame(-1, 1, 1, 1),
        new int[][]{{0, 2}, {0, 2}, {0, 2}});
  }

  @Test
  public void testDescAsymmetricOffsets() {
    // values sorted desc: 5, 4, 3, 2, 1  |  frame BETWEEN 1 PRECEDING AND 2 FOLLOWING (asymmetric).
    // A swap of the lower/upper deltas in the DESC branch would produce a different result than this.
    assertFrames(rows(5, 4, 3, 2, 1), ColumnDataType.INT, false, frame(-1, 1, 1, 2),
        new int[][]{{0, 2}, {0, 3}, {1, 4}, {2, 4}, {3, 4}});
  }

  @Test
  public void testDescBetweenTwoPrecedingAndCurrentRow() {
    // values sorted desc: 5, 4, 3, 2, 1  |  frame BETWEEN 2 PRECEDING AND CURRENT ROW.
    assertFrames(rows(5, 4, 3, 2, 1), ColumnDataType.INT, false, frame(-1, 2, 0, null),
        new int[][]{{0, 0}, {0, 1}, {0, 2}, {1, 3}, {2, 4}});
  }

  @Test
  public void testDescNullsLastUnboundedPrecedingAndOneFollowing() {
    // values sorted desc: 5, 3, 1, null, null  |  UNBOUNDED PRECEDING extends to index 0, NULLs included via the edge.
    assertFrames(rows(5, 3, 1, null, null), ColumnDataType.INT, false, frame(Integer.MIN_VALUE, null, 1, 1),
        new int[][]{{0, 0}, {0, 1}, {0, 2}, {0, 4}, {0, 4}});
  }

  @Test
  public void testLongOrderKey() {
    // LONG order key exercises the primitive long fast path.
    assertFrames(rows(1L, 2L, 2L, 5L, 7L), ColumnDataType.LONG, true, frame(-1, 1, 1, 1),
        new int[][]{{0, 2}, {0, 2}, {0, 2}, {3, 3}, {4, 4}});
  }

  @Test
  public void testFloatOrderKey() {
    // FLOAT order key exercises the double value-bounds path with Float-typed values.
    assertFrames(rows(1.0f, 1.5f, 2.0f, 4.0f), ColumnDataType.FLOAT, true, frame(-1, 0.5, 1, 0.5),
        new int[][]{{0, 1}, {0, 2}, {1, 2}, {3, 3}});
  }

  @Test
  public void testIntOrderKeyWithNonIntegralOffsetFallsBackToBigDecimal() {
    // A non-integral offset on an INT order key must fall back to exact BigDecimal arithmetic.
    // values: 1, 2, 3, 4  |  frame = [v-1.5, v+1.5]
    assertFrames(rows(1, 2, 3, 4), ColumnDataType.INT, true,
        frame(-1, new java.math.BigDecimal("1.5"), 1, new java.math.BigDecimal("1.5")),
        new int[][]{{0, 1}, {0, 2}, {1, 3}, {2, 3}});
  }

  @Test
  public void testLongOrderKeyOffsetSaturatesInsteadOfOverflowing() {
    // Order-key values near Long.MAX_VALUE with a positive offset: the upper edge must saturate at Long.MAX_VALUE
    // rather than overflowing to a negative value (which would drop rows from the frame).
    assertFrames(rows(Long.MAX_VALUE - 2, Long.MAX_VALUE - 1, Long.MAX_VALUE), ColumnDataType.LONG, true,
        frame(-1, 5, 1, 5), new int[][]{{0, 2}, {0, 2}, {0, 2}});
  }
}
