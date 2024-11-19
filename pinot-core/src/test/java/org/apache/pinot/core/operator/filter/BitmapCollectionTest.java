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
package org.apache.pinot.core.operator.filter;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class BitmapCollectionTest {

  @DataProvider
  public static Object[][] andCardinalityTestCases() {
    return new Object[][]{
        {
            10, ImmutableRoaringBitmap.bitmapOf(0, 5), false,
            ImmutableRoaringBitmap.bitmapOf(0, 4), false, 1
        },
        {
            10, ImmutableRoaringBitmap.bitmapOf(0, 5), false,
            ImmutableRoaringBitmap.bitmapOf(1, 4), false, 0
        },
        {
            10, ImmutableRoaringBitmap.bitmapOf(0, 5), false,
            ImmutableRoaringBitmap.bitmapOf(), false, 0
        },
        {
            10, ImmutableRoaringBitmap.bitmapOf(), false,
            ImmutableRoaringBitmap.bitmapOf(0, 5), false, 0
        },
        {
            10, ImmutableRoaringBitmap.bitmapOf(), false,
            ImmutableRoaringBitmap.bitmapOf(), false, 0
        },
        {
            10, ImmutableRoaringBitmap.bitmapOf(0, 5), true,
            ImmutableRoaringBitmap.bitmapOf(0, 4), false, 1
        },
        {
            10, ImmutableRoaringBitmap.bitmapOf(0, 5), true,
            ImmutableRoaringBitmap.bitmapOf(1, 4), false, 2
        },
        {
            10, ImmutableRoaringBitmap.bitmapOf(0, 5), true,
            ImmutableRoaringBitmap.bitmapOf(), false, 0
        },
        {
            10, ImmutableRoaringBitmap.bitmapOf(), true,
            ImmutableRoaringBitmap.bitmapOf(0, 5), false, 2
        },
        {
            10, ImmutableRoaringBitmap.bitmapOf(), true,
            ImmutableRoaringBitmap.bitmapOf(), false, 0
        },
        {
            10, ImmutableRoaringBitmap.bitmapOf(0, 5), false,
            ImmutableRoaringBitmap.bitmapOf(0, 4), true, 1
        },
        {
            10, ImmutableRoaringBitmap.bitmapOf(0, 5), false,
            ImmutableRoaringBitmap.bitmapOf(1, 4), true, 2
        },
        {
            10, ImmutableRoaringBitmap.bitmapOf(0, 5), false,
            ImmutableRoaringBitmap.bitmapOf(), true, 2
        },
        {
            10, ImmutableRoaringBitmap.bitmapOf(), false,
            ImmutableRoaringBitmap.bitmapOf(), true, 0
        },
        {
            10, ImmutableRoaringBitmap.bitmapOf(), false,
            ImmutableRoaringBitmap.bitmapOf(0, 5), true, 0
        },
        {
            10, ImmutableRoaringBitmap.bitmapOf(0, 5), true,
            ImmutableRoaringBitmap.bitmapOf(0, 4), true, 7
        },
        {
            10, ImmutableRoaringBitmap.bitmapOf(0, 5), true,
            ImmutableRoaringBitmap.bitmapOf(1, 4), true, 6
        },
        {
            10, ImmutableRoaringBitmap.bitmapOf(0, 5), true,
            ImmutableRoaringBitmap.bitmapOf(), true, 8
        },
        {
            10, ImmutableRoaringBitmap.bitmapOf(), true,
            ImmutableRoaringBitmap.bitmapOf(0, 5), true, 8
        },
        {
            10, ImmutableRoaringBitmap.bitmapOf(), true,
            ImmutableRoaringBitmap.bitmapOf(), true, 10
        },
    };
  }

  @Test(dataProvider = "andCardinalityTestCases")
  public void testAndCardinality(int numDocs, ImmutableRoaringBitmap left, boolean leftInverted,
      ImmutableRoaringBitmap right, boolean rightInverted, int expected) {
    assertEquals(new BitmapCollection(numDocs, leftInverted, left).andCardinality(
        new BitmapCollection(numDocs, rightInverted, right)), expected);
    assertEquals(new BitmapCollection(numDocs, leftInverted, split(left)).andCardinality(
        new BitmapCollection(numDocs, rightInverted, right)), expected);
    assertEquals(new BitmapCollection(numDocs, leftInverted, left).andCardinality(
        new BitmapCollection(numDocs, rightInverted, split(right))), expected);
    assertEquals(new BitmapCollection(numDocs, leftInverted, split(left)).andCardinality(
        new BitmapCollection(numDocs, rightInverted, split(right))), expected);
  }

  @DataProvider
  public static Object[][] orCardinalityTestCases() {
    return new Object[][]{
        {
            10, ImmutableRoaringBitmap.bitmapOf(0, 5), false,
            ImmutableRoaringBitmap.bitmapOf(0, 4), false, 3
        },
        {
            10, ImmutableRoaringBitmap.bitmapOf(0, 5), false,
            ImmutableRoaringBitmap.bitmapOf(1, 4), false, 4
        },
        {
            10, ImmutableRoaringBitmap.bitmapOf(0, 5), false,
            ImmutableRoaringBitmap.bitmapOf(), false, 2
        },
        {
            10, ImmutableRoaringBitmap.bitmapOf(), false,
            ImmutableRoaringBitmap.bitmapOf(0, 5), false, 2
        },
        {
            10, ImmutableRoaringBitmap.bitmapOf(), false,
            ImmutableRoaringBitmap.bitmapOf(), false, 0
        },
        {
            10, ImmutableRoaringBitmap.bitmapOf(0, 5), true,
            ImmutableRoaringBitmap.bitmapOf(0, 4), false, 9
        },
        {
            10, ImmutableRoaringBitmap.bitmapOf(0, 5), true,
            ImmutableRoaringBitmap.bitmapOf(1, 4), false, 8
        },
        {
            10, ImmutableRoaringBitmap.bitmapOf(0, 5), true,
            ImmutableRoaringBitmap.bitmapOf(), false, 8
        },
        {
            10, ImmutableRoaringBitmap.bitmapOf(), true,
            ImmutableRoaringBitmap.bitmapOf(0, 5), false, 10
        },
        {
            10, ImmutableRoaringBitmap.bitmapOf(), true,
            ImmutableRoaringBitmap.bitmapOf(), false, 10
        },
        {
            10, ImmutableRoaringBitmap.bitmapOf(0, 5), false,
            ImmutableRoaringBitmap.bitmapOf(0, 4), true, 9
        },
        {
            10, ImmutableRoaringBitmap.bitmapOf(0, 5), false,
            ImmutableRoaringBitmap.bitmapOf(1, 4), true, 8
        },
        {
            10, ImmutableRoaringBitmap.bitmapOf(0, 5), false,
            ImmutableRoaringBitmap.bitmapOf(), true, 10
        },
        {
            10, ImmutableRoaringBitmap.bitmapOf(), false,
            ImmutableRoaringBitmap.bitmapOf(0, 5), true, 8
        },
        {
            10, ImmutableRoaringBitmap.bitmapOf(), false,
            ImmutableRoaringBitmap.bitmapOf(), true, 10
        },
        {
            10, ImmutableRoaringBitmap.bitmapOf(0, 5), true,
            ImmutableRoaringBitmap.bitmapOf(0, 4), true, 9
        },
        {
            10, ImmutableRoaringBitmap.bitmapOf(0, 5), true,
            ImmutableRoaringBitmap.bitmapOf(1, 4), true, 10
        },
        {
            10, ImmutableRoaringBitmap.bitmapOf(0, 5), true,
            ImmutableRoaringBitmap.bitmapOf(), true, 10
        },
        {
            10, ImmutableRoaringBitmap.bitmapOf(), true,
            ImmutableRoaringBitmap.bitmapOf(0, 5), true, 10
        },
        {
            10, ImmutableRoaringBitmap.bitmapOf(), true,
            ImmutableRoaringBitmap.bitmapOf(), true, 10
        },
    };
  }

  @Test(dataProvider = "orCardinalityTestCases")
  public void testOrCardinality(int numDocs, ImmutableRoaringBitmap left, boolean leftInverted,
      ImmutableRoaringBitmap right, boolean rightInverted, int expected) {
    assertEquals(new BitmapCollection(numDocs, leftInverted, left).orCardinality(
        new BitmapCollection(numDocs, rightInverted, right)), expected);
    assertEquals(new BitmapCollection(numDocs, leftInverted, split(left)).orCardinality(
        new BitmapCollection(numDocs, rightInverted, right)), expected);
    assertEquals(new BitmapCollection(numDocs, leftInverted, left).orCardinality(
        new BitmapCollection(numDocs, rightInverted, split(right))), expected);
    assertEquals(new BitmapCollection(numDocs, leftInverted, split(left)).orCardinality(
        new BitmapCollection(numDocs, rightInverted, split(right))), expected);
  }

  private ImmutableRoaringBitmap[] split(ImmutableRoaringBitmap bitmap) {
    if (bitmap.isEmpty()) {
      return new ImmutableRoaringBitmap[]{bitmap};
    }
    ImmutableRoaringBitmap[] split = new ImmutableRoaringBitmap[2];
    split[0] = bitmap;
    split[1] = ImmutableRoaringBitmap.bitmapOf(bitmap.last());
    return split;
  }
}
