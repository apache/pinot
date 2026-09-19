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
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;


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

  @Test
  public void testExcludingNulls() {
    int numDocs = 10;
    ImmutableRoaringBitmap docIds = ImmutableRoaringBitmap.bitmapOf(0, 5);
    ImmutableRoaringBitmap nullBitmap = ImmutableRoaringBitmap.bitmapOf(5, 7);

    BitmapCollection bitmaps = new BitmapCollection(numDocs, false, docIds).excludingNulls(nullBitmap);
    assertSame(bitmaps.getNullBitmap(), nullBitmap);
    assertEquals(bitmaps.getCardinality(), 1);
    assertEquals(bitmaps.reduce().toArray(), new int[]{0});

    // NOT of UNKNOWN is UNKNOWN: the inversion is the complement of the union minus the null documents
    bitmaps.invert();
    assertSame(bitmaps.getNullBitmap(), nullBitmap);
    assertEquals(bitmaps.getCardinality(), 7);
    assertEquals(bitmaps.reduce().toArray(), new int[]{1, 2, 3, 4, 6, 8, 9});

    BitmapCollection splitBitmaps = new BitmapCollection(numDocs, true, split(docIds)).excludingNulls(nullBitmap);
    assertEquals(splitBitmaps.getCardinality(), 7);
    assertEquals(splitBitmaps.reduce().toArray(), new int[]{1, 2, 3, 4, 6, 8, 9});
  }

  @Test
  public void testExcludingNoNulls() {
    BitmapCollection bitmaps = new BitmapCollection(10, false, ImmutableRoaringBitmap.bitmapOf(0, 5));
    assertSame(bitmaps.excludingNulls(null), bitmaps);
    assertSame(bitmaps.excludingNulls(ImmutableRoaringBitmap.bitmapOf()), bitmaps);
    assertNull(bitmaps.getNullBitmap());
    assertEquals(bitmaps.getCardinality(), 2);
    assertEquals(bitmaps.invert().getCardinality(), 8);
  }

  @Test
  public void testAndOrCardinalityWithNulls() {
    int numDocs = 10;
    // True on {0, 1}
    BitmapCollection left = new BitmapCollection(numDocs, false, ImmutableRoaringBitmap.bitmapOf(0, 1, 5))
        .excludingNulls(ImmutableRoaringBitmap.bitmapOf(5, 7));
    // True on {0, 4, 5, 6, 7, 8, 9}
    BitmapCollection right = new BitmapCollection(numDocs, true, ImmutableRoaringBitmap.bitmapOf(1, 2))
        .excludingNulls(ImmutableRoaringBitmap.bitmapOf(3));
    // True on {1, 5, 9}
    BitmapCollection plain = new BitmapCollection(numDocs, false, ImmutableRoaringBitmap.bitmapOf(1, 5, 9));

    assertEquals(left.andCardinality(right), 1);
    assertEquals(left.orCardinality(right), 8);
    assertEquals(left.andCardinality(plain), 1);
    assertEquals(plain.andCardinality(left), 1);
    assertEquals(left.orCardinality(plain), 4);
    assertEquals(plain.orCardinality(left), 4);
  }

  /// Checks the cardinalities against the materialized true documents over every combination of inversion and null
  /// bitmaps on either side, with nulls inside and outside the unions and shared between the sides.
  @Test
  public void testCardinalitiesMatchMaterializedTrues() {
    int numDocs = 12;
    ImmutableRoaringBitmap leftDocIds = ImmutableRoaringBitmap.bitmapOf(0, 1, 5, 6, 9);
    ImmutableRoaringBitmap rightDocIds = ImmutableRoaringBitmap.bitmapOf(1, 2, 6, 10);
    ImmutableRoaringBitmap[] nullBitmaps = {
        null, ImmutableRoaringBitmap.bitmapOf(1, 5, 7), ImmutableRoaringBitmap.bitmapOf(2, 7, 9, 11)
    };
    for (boolean leftInverted : new boolean[]{false, true}) {
      for (boolean rightInverted : new boolean[]{false, true}) {
        for (ImmutableRoaringBitmap leftNulls : nullBitmaps) {
          for (ImmutableRoaringBitmap rightNulls : nullBitmaps) {
            BitmapCollection left =
                new BitmapCollection(numDocs, leftInverted, split(leftDocIds)).excludingNulls(leftNulls);
            BitmapCollection right =
                new BitmapCollection(numDocs, rightInverted, rightDocIds).excludingNulls(rightNulls);
            ImmutableRoaringBitmap leftTrues = left.reduce();
            ImmutableRoaringBitmap rightTrues = right.reduce();
            String description = String.format("leftInverted=%s rightInverted=%s leftNulls=%s rightNulls=%s",
                leftInverted, rightInverted, leftNulls, rightNulls);

            assertEquals(left.getCardinality(), leftTrues.getCardinality(), description);
            assertEquals(right.getCardinality(), rightTrues.getCardinality(), description);
            int andCardinality = ImmutableRoaringBitmap.andCardinality(leftTrues, rightTrues);
            assertEquals(left.andCardinality(right), andCardinality, description);
            assertEquals(right.andCardinality(left), andCardinality, description);
            int orCardinality = ImmutableRoaringBitmap.orCardinality(leftTrues, rightTrues);
            assertEquals(left.orCardinality(right), orCardinality, description);
            assertEquals(right.orCardinality(left), orCardinality, description);
          }
        }
      }
    }
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
