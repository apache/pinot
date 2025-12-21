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
package org.apache.pinot.common.function.scalar;

import org.apache.pinot.spi.utils.CommonConstants.NullValuePlaceHolder;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Tests for array scalar functions in ArrayFunctions class.
 * Tests both existing functions and newly added Trino-compatible functions.
 */
public class ArrayFunctionsTest {

  // Test data
  private static final int[] INT_ARRAY = {1, 2, 3, 4, 5};
  private static final int[] EMPTY_INT_ARRAY = {};
  private static final long[] LONG_ARRAY = {10L, 20L, 30L, 40L, 50L};
  private static final float[] FLOAT_ARRAY = {1.1f, 2.2f, 3.3f, 4.4f, 5.5f};
  private static final double[] DOUBLE_ARRAY = {1.1, 2.2, 3.3, 4.4, 5.5};
  private static final String[] STRING_ARRAY = {"apple", "banana", "cherry", "date", "elderberry"};
  private static final String[] EMPTY_STRING_ARRAY = {};

  /* START GENAI@CLINE */
  // Tests for arrayFirst functions
  @Test
  public void testArrayFirst() {
    // Test with valid arrays
    assertEquals(ArrayFunctions.arrayFirstInt(INT_ARRAY), 1);
    assertEquals(ArrayFunctions.arrayFirstLong(LONG_ARRAY), 10L);
    assertEquals(ArrayFunctions.arrayFirstFloat(FLOAT_ARRAY), 1.1f);
    assertEquals(ArrayFunctions.arrayFirstDouble(DOUBLE_ARRAY), 1.1);
    assertEquals(ArrayFunctions.arrayFirstString(STRING_ARRAY), "apple");

    // Test with empty arrays
    assertEquals(ArrayFunctions.arrayFirstInt(EMPTY_INT_ARRAY), NullValuePlaceHolder.INT);
    assertEquals(ArrayFunctions.arrayFirstString(EMPTY_STRING_ARRAY), NullValuePlaceHolder.STRING);

    // Test with null arrays
    assertEquals(ArrayFunctions.arrayFirstInt(null), NullValuePlaceHolder.INT);
    assertEquals(ArrayFunctions.arrayFirstString(null), NullValuePlaceHolder.STRING);
  }

  // Tests for arrayLast functions
  @Test
  public void testArrayLast() {
    // Test with valid arrays
    assertEquals(ArrayFunctions.arrayLastInt(INT_ARRAY), 5);
    assertEquals(ArrayFunctions.arrayLastLong(LONG_ARRAY), 50L);
    assertEquals(ArrayFunctions.arrayLastFloat(FLOAT_ARRAY), 5.5f);
    assertEquals(ArrayFunctions.arrayLastDouble(DOUBLE_ARRAY), 5.5);
    assertEquals(ArrayFunctions.arrayLastString(STRING_ARRAY), "elderberry");

    // Test with empty arrays
    assertEquals(ArrayFunctions.arrayLastInt(EMPTY_INT_ARRAY), NullValuePlaceHolder.INT);
    assertEquals(ArrayFunctions.arrayLastString(EMPTY_STRING_ARRAY), NullValuePlaceHolder.STRING);

    // Test with null arrays
    assertEquals(ArrayFunctions.arrayLastInt(null), NullValuePlaceHolder.INT);
    assertEquals(ArrayFunctions.arrayLastString(null), NullValuePlaceHolder.STRING);
  }

  // Tests for arrayPosition functions
  @Test
  public void testArrayPosition() {
    // Test finding existing elements (1-based indexing)
    assertEquals(ArrayFunctions.arrayPositionInt(INT_ARRAY, 1), 1L);
    assertEquals(ArrayFunctions.arrayPositionInt(INT_ARRAY, 3), 3L);
    assertEquals(ArrayFunctions.arrayPositionInt(INT_ARRAY, 5), 5L);
    assertEquals(ArrayFunctions.arrayPositionLong(LONG_ARRAY, 20L), 2L);
    assertEquals(ArrayFunctions.arrayPositionFloat(FLOAT_ARRAY, 3.3f), 3L);
    assertEquals(ArrayFunctions.arrayPositionDouble(DOUBLE_ARRAY, 4.4), 4L);
    assertEquals(ArrayFunctions.arrayPositionString(STRING_ARRAY, "banana"), 2L);

    // Test with non-existing elements
    assertEquals(ArrayFunctions.arrayPositionInt(INT_ARRAY, 10), 0L);
    assertEquals(ArrayFunctions.arrayPositionString(STRING_ARRAY, "grape"), 0L);

    // Test with null arrays
    assertEquals(ArrayFunctions.arrayPositionInt(null, 1), 0L);
    assertEquals(ArrayFunctions.arrayPositionString(null, "test"), 0L);

    // Test with null element in string array
    String[] arrayWithNull = {"a", null, "b"};
    assertEquals(ArrayFunctions.arrayPositionString(arrayWithNull, null), 2L);
  }

  // Tests for arrayMax functions
  @Test
  public void testArrayMax() {
    // Test with valid arrays
    assertEquals(ArrayFunctions.arrayMaxInt(INT_ARRAY), 5);
    assertEquals(ArrayFunctions.arrayMaxLong(LONG_ARRAY), 50L);
    assertEquals(ArrayFunctions.arrayMaxFloat(FLOAT_ARRAY), 5.5f);
    assertEquals(ArrayFunctions.arrayMaxDouble(DOUBLE_ARRAY), 5.5);
    assertEquals(ArrayFunctions.arrayMaxString(STRING_ARRAY), "elderberry");

    // Test with single element
    assertEquals(ArrayFunctions.arrayMaxInt(new int[]{42}), 42);

    // Test with negative numbers
    int[] negativeArray = {-5, -2, -10, -1};
    assertEquals(ArrayFunctions.arrayMaxInt(negativeArray), -1);

    // Test with empty arrays
    assertEquals(ArrayFunctions.arrayMaxInt(EMPTY_INT_ARRAY), NullValuePlaceHolder.INT);
    assertEquals(ArrayFunctions.arrayMaxString(EMPTY_STRING_ARRAY), NullValuePlaceHolder.STRING);

    // Test with null arrays
    assertEquals(ArrayFunctions.arrayMaxInt(null), NullValuePlaceHolder.INT);
    assertEquals(ArrayFunctions.arrayMaxString(null), NullValuePlaceHolder.STRING);
  }

  // Tests for arrayMin functions
  @Test
  public void testArrayMin() {
    // Test with valid arrays
    assertEquals(ArrayFunctions.arrayMinInt(INT_ARRAY), 1);
    assertEquals(ArrayFunctions.arrayMinLong(LONG_ARRAY), 10L);
    assertEquals(ArrayFunctions.arrayMinFloat(FLOAT_ARRAY), 1.1f);
    assertEquals(ArrayFunctions.arrayMinDouble(DOUBLE_ARRAY), 1.1);
    assertEquals(ArrayFunctions.arrayMinString(STRING_ARRAY), "apple");

    // Test with single element
    assertEquals(ArrayFunctions.arrayMinInt(new int[]{42}), 42);

    // Test with negative numbers
    int[] negativeArray = {-5, -2, -10, -1};
    assertEquals(ArrayFunctions.arrayMinInt(negativeArray), -10);

    // Test with empty arrays
    assertEquals(ArrayFunctions.arrayMinInt(EMPTY_INT_ARRAY), NullValuePlaceHolder.INT);
    assertEquals(ArrayFunctions.arrayMinString(EMPTY_STRING_ARRAY), NullValuePlaceHolder.STRING);

    // Test with null arrays
    assertEquals(ArrayFunctions.arrayMinInt(null), NullValuePlaceHolder.INT);
    assertEquals(ArrayFunctions.arrayMinString(null), NullValuePlaceHolder.STRING);
  }


  // Tests for arrayJoin functions
  @Test
  public void testArrayJoin() {
    // Test basic joining
    assertEquals(ArrayFunctions.arrayJoinInt(INT_ARRAY, ","), "1,2,3,4,5");
    assertEquals(ArrayFunctions.arrayJoinLong(LONG_ARRAY, "-"), "10-20-30-40-50");
    assertEquals(ArrayFunctions.arrayJoinString(STRING_ARRAY, " | "), "apple | banana | cherry | date | elderberry");

    // Test with different delimiter
    assertEquals(ArrayFunctions.arrayJoinInt(INT_ARRAY, " "), "1 2 3 4 5");

    // Test with empty arrays
    assertEquals(ArrayFunctions.arrayJoinInt(EMPTY_INT_ARRAY, ","), NullValuePlaceHolder.STRING);
    assertEquals(ArrayFunctions.arrayJoinString(EMPTY_STRING_ARRAY, ","), NullValuePlaceHolder.STRING);

    // Test with null arrays
    assertEquals(ArrayFunctions.arrayJoinInt(null, ","), NullValuePlaceHolder.STRING);
    assertEquals(ArrayFunctions.arrayJoinString(null, ","), NullValuePlaceHolder.STRING);

    // Test with single element
    assertEquals(ArrayFunctions.arrayJoinInt(new int[]{42}, ","), "42");
  }

  // Tests for arrayJoin with null replacement
  @Test
  public void testArrayJoinWithNullReplacement() {
    // Test with arrays containing null placeholders
    int[] arrayWithNulls = {1, NullValuePlaceHolder.INT, 3};
    assertEquals(ArrayFunctions.arrayJoinInt(arrayWithNulls, ",", "NULL"), "1,NULL,3");

    String[] stringArrayWithNulls = {"a", null, "c"};
    assertEquals(ArrayFunctions.arrayJoinString(stringArrayWithNulls, ",", "NULL"), "a,NULL,c");

    // Test with no nulls
    assertEquals(ArrayFunctions.arrayJoinInt(INT_ARRAY, ",", "NULL"), "1,2,3,4,5");

    // Test with empty arrays
    assertEquals(ArrayFunctions.arrayJoinInt(EMPTY_INT_ARRAY, ",", "NULL"), NullValuePlaceHolder.STRING);

    // Test with null arrays
    assertEquals(ArrayFunctions.arrayJoinInt(null, ",", "NULL"), NullValuePlaceHolder.STRING);
  }

  // Tests for contains functions (standard SQL name)
  @Test
  public void testContains() {
    // Test existing elements
    assertTrue(ArrayFunctions.containsInt(INT_ARRAY, 3));
    assertTrue(ArrayFunctions.containsLong(LONG_ARRAY, 30L));
    assertTrue(ArrayFunctions.containsFloat(FLOAT_ARRAY, 3.3f));
    assertTrue(ArrayFunctions.containsDouble(DOUBLE_ARRAY, 4.4));
    assertTrue(ArrayFunctions.containsString(STRING_ARRAY, "banana"));

    // Test non-existing elements
    assertFalse(ArrayFunctions.containsInt(INT_ARRAY, 10));
    assertFalse(ArrayFunctions.containsString(STRING_ARRAY, "grape"));

    // Test with null arrays
    assertFalse(ArrayFunctions.containsInt(null, 1));
    assertFalse(ArrayFunctions.containsString(null, "test"));

    // Test with empty arrays
    assertFalse(ArrayFunctions.containsInt(EMPTY_INT_ARRAY, 1));
    assertFalse(ArrayFunctions.containsString(EMPTY_STRING_ARRAY, "test"));
  }

  // Tests for elementAt functions
  @Test
  public void testElementAt() {
    // Test positive indexing (1-based)
    assertEquals(ArrayFunctions.elementAtInt(INT_ARRAY, 1), 1);
    assertEquals(ArrayFunctions.elementAtInt(INT_ARRAY, 3), 3);
    assertEquals(ArrayFunctions.elementAtInt(INT_ARRAY, 5), 5);
    assertEquals(ArrayFunctions.elementAtString(STRING_ARRAY, 2), "banana");

    // Test negative indexing (from end)
    assertEquals(ArrayFunctions.elementAtInt(INT_ARRAY, -1), 5);
    assertEquals(ArrayFunctions.elementAtInt(INT_ARRAY, -2), 4);
    assertEquals(ArrayFunctions.elementAtString(STRING_ARRAY, -1), "elderberry");

    // Test out-of-bounds access
    assertEquals(ArrayFunctions.elementAtInt(INT_ARRAY, 10), NullValuePlaceHolder.INT);
    assertEquals(ArrayFunctions.elementAtInt(INT_ARRAY, -10), NullValuePlaceHolder.INT);
    assertEquals(ArrayFunctions.elementAtString(STRING_ARRAY, 10), NullValuePlaceHolder.STRING);

    // Test index 0 (invalid)
    assertEquals(ArrayFunctions.elementAtInt(INT_ARRAY, 0), NullValuePlaceHolder.INT);
    assertEquals(ArrayFunctions.elementAtString(STRING_ARRAY, 0), NullValuePlaceHolder.STRING);

    // Test with empty arrays
    assertEquals(ArrayFunctions.elementAtInt(EMPTY_INT_ARRAY, 1), NullValuePlaceHolder.INT);
    assertEquals(ArrayFunctions.elementAtString(EMPTY_STRING_ARRAY, 1), NullValuePlaceHolder.STRING);

    // Test with null arrays
    assertEquals(ArrayFunctions.elementAtInt(null, 1), NullValuePlaceHolder.INT);
    assertEquals(ArrayFunctions.elementAtString(null, 1), NullValuePlaceHolder.STRING);
  }

  // Tests for reverse functions (standard SQL name)
  @Test
  public void testReverse() {
    // Test int array reverse
    int[] reversedInt = ArrayFunctions.reverseInt(INT_ARRAY);
    assertNotSame(reversedInt, INT_ARRAY); // Should be a clone
    assertEquals(reversedInt, new int[]{5, 4, 3, 2, 1});

    // Test string array reverse
    String[] reversedString = ArrayFunctions.reverseString(STRING_ARRAY);
    assertNotSame(reversedString, STRING_ARRAY); // Should be a clone
    assertEquals(reversedString, new String[]{"elderberry", "date", "cherry", "banana", "apple"});

    // Test long array reverse
    long[] reversedLong = ArrayFunctions.reverseLong(LONG_ARRAY);
    assertEquals(reversedLong, new long[]{50L, 40L, 30L, 20L, 10L});

    // Test with single element
    int[] singleElement = {42};
    int[] reversedSingle = ArrayFunctions.reverseInt(singleElement);
    assertEquals(reversedSingle, new int[]{42});

    // Test with empty arrays
    int[] reversedEmpty = ArrayFunctions.reverseInt(EMPTY_INT_ARRAY);
    assertEquals(reversedEmpty, EMPTY_INT_ARRAY);

    // Test with null arrays
    assertNull(ArrayFunctions.reverseLong(null));
    assertNull(ArrayFunctions.reverseFloat(null));
    assertNull(ArrayFunctions.reverseDouble(null));
  }

  // Tests for edge cases and special scenarios
  @Test
  public void testEdgeCases() {
    // Test arrays with duplicate elements
    int[] duplicateArray = {1, 2, 2, 3, 2};
    assertEquals(ArrayFunctions.arrayPositionInt(duplicateArray, 2), 2L); // Should return first occurrence
    assertEquals(ArrayFunctions.arrayMaxInt(duplicateArray), 3);
    assertEquals(ArrayFunctions.arrayMinInt(duplicateArray), 1);

    // Test string arrays with null elements
    String[] arrayWithNulls = {"a", null, "b", null, "c"};
    assertEquals(ArrayFunctions.arrayFirstString(arrayWithNulls), "a");
    assertEquals(ArrayFunctions.arrayLastString(arrayWithNulls), "c");
    assertEquals(ArrayFunctions.arrayPositionString(arrayWithNulls, null), 2L);

    // Test float/double precision
    float[] floatPrecisionArray = {1.0f, 1.0000001f, 1.0f};
    assertEquals(ArrayFunctions.arrayPositionFloat(floatPrecisionArray, 1.0f), 1L);

    // Test very large arrays (performance)
    int[] largeArray = new int[1000];
    for (int i = 0; i < 1000; i++) {
      largeArray[i] = i;
    }
    assertEquals(ArrayFunctions.arrayFirstInt(largeArray), 0);
    assertEquals(ArrayFunctions.arrayLastInt(largeArray), 999);
    assertEquals(ArrayFunctions.arrayMaxInt(largeArray), 999);
    assertEquals(ArrayFunctions.arrayMinInt(largeArray), 0);
  }

  // Test compatibility with existing arrayElementAt functions
  @Test
  public void testBackwardCompatibility() {
    // Ensure existing arrayElementAt functions still work
    assertEquals(ArrayFunctions.arrayElementAtInt(INT_ARRAY, 1), 1);
    assertEquals(ArrayFunctions.arrayElementAtInt(INT_ARRAY, 3), 3);
    assertEquals(ArrayFunctions.arrayElementAtString(STRING_ARRAY, 2), "banana");

    // Test out-of-bounds with existing functions
    assertEquals(ArrayFunctions.arrayElementAtInt(INT_ARRAY, 10), NullValuePlaceHolder.INT);
    assertEquals(ArrayFunctions.arrayElementAtString(STRING_ARRAY, 10), NullValuePlaceHolder.STRING);

    // Ensure existing arrayReverse functions still work
    int[] reversed = ArrayFunctions.arrayReverseInt(INT_ARRAY);
    assertEquals(reversed, new int[]{5, 4, 3, 2, 1});

    String[] reversedStr = ArrayFunctions.arrayReverseString(STRING_ARRAY);
    assertEquals(reversedStr, new String[]{"elderberry", "date", "cherry", "banana", "apple"});
  }

  // Test function behavior with special float/double values
  @Test
  public void testSpecialFloatingPointValues() {
    // Test with NaN, Infinity
    double[] specialDoubles = {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 1.0, 2.0};
    assertEquals(ArrayFunctions.arrayFirstDouble(specialDoubles), Double.NaN);
    assertEquals(ArrayFunctions.arrayLastDouble(specialDoubles), 2.0);

    // Test position finding with special values
    assertEquals(ArrayFunctions.arrayPositionDouble(specialDoubles, Double.POSITIVE_INFINITY), 2L);
    assertEquals(ArrayFunctions.arrayPositionDouble(specialDoubles, 1.0), 4L);

    float[] specialFloats = {Float.NaN, 1.0f, Float.POSITIVE_INFINITY};
    assertEquals(ArrayFunctions.arrayPositionFloat(specialFloats, Float.POSITIVE_INFINITY), 3L);
  }
  /* END GENAI@CLINE */
}
