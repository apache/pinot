/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.query.aggregation.groupby;

import com.linkedin.pinot.common.utils.Pairs.IntDoubleComparator;
import com.linkedin.pinot.common.utils.Pairs.IntDoublePair;
import com.linkedin.pinot.core.query.aggregation.groupby.DoubleGroupByResultHolder;
import com.linkedin.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;


/**
 * Test for GroupByResultHolder class.
 */
@Test
public class DoubleGroupByResultHolderTest {
  private static final long RANDOM_SEED = System.nanoTime();
  private static final int INITIAL_CAPACITY = 100;
  private static final int MAX_CAPACITY = 1000;
  private static final double DEFAULT_VALUE = -1;

  double[] _expected;

  /**
   * Initial setup:
   * - Initialize the '_expected' array with random double values.
   */
  @BeforeSuite
  void setup() {
    Random random = new Random(RANDOM_SEED);
    _expected = new double[MAX_CAPACITY];

    for (int i = 0; i < MAX_CAPACITY; i++) {
      double value = random.nextDouble();
      _expected[i] = value;
    }
  }

  /**
   * This test is for the GroupByResultHolder.SetValueForKey() api.
   * - Sets a random set of values in the result holder.
   * - Asserts that the values returned by the result holder are as expected.
   */
  @Test
  void testSetValueForKey() {
    GroupByResultHolder resultHolder =
        new DoubleGroupByResultHolder(INITIAL_CAPACITY, MAX_CAPACITY, MAX_CAPACITY, DEFAULT_VALUE);

    for (int i = 0; i < INITIAL_CAPACITY; i++) {
      resultHolder.setValueForKey(i, _expected[i]);
    }
    testValues(resultHolder, _expected, 0, INITIAL_CAPACITY);
  }

  /**
   * This test is for the GroupByResultHolder.EnsureCapacity api.
   * - Fills the the result holder with a set of values.
   * - Calls ensureCapacity to expand the result holder size.
   * - Checks that the expanded unfilled portion of the result holder contains {@ref #DEFAULT_VALUE}
   * - Fills the rest of the resultHolder, and ensures all values are returned as expected.
   */
  @Test
  void testEnsureCapacity() {
    GroupByResultHolder resultHolder =
        new DoubleGroupByResultHolder(INITIAL_CAPACITY, MAX_CAPACITY, MAX_CAPACITY, DEFAULT_VALUE);

    for (int i = 0; i < INITIAL_CAPACITY; i++) {
      resultHolder.setValueForKey(i, _expected[i]);
    }

    resultHolder.ensureCapacity(MAX_CAPACITY);
    for (int i = INITIAL_CAPACITY; i < MAX_CAPACITY; i++) {
      double actual = resultHolder.getDoubleResult(i);
      Assert.assertEquals(actual, DEFAULT_VALUE,
          "Default Value mis-match: Actual: " + actual + " Expected: " + DEFAULT_VALUE + " Random seed: "
              + RANDOM_SEED);

      resultHolder.setValueForKey(i, _expected[i]);
    }

    testValues(resultHolder, _expected, 0, MAX_CAPACITY);
  }

  /**
   * Test for 'trimResults' method, with sort order from max to min.
   */
  @Test
  void testMaxTrimResults() {
    testTrimResults(false);
  }

  /**
   * Test for 'trimResults' method, with sort order from min to max.
   */
  @Test
  void testMinTrimResults() {
    testTrimResults(true);
  }

  /**
   * Helper method to test values within resultHolder against the provided expected values array.
   *
   * @param resultHolder
   * @param expected
   * @param start
   * @param end
   */
  private void testValues(GroupByResultHolder resultHolder, double[] expected, int start, int end) {
    for (int i = start; i < end; i++) {
      double actual = resultHolder.getDoubleResult(i);
      Assert.assertEquals(actual, expected[i],
          "Value mis-match: Actual: " + actual + " Expected: " + expected[i] + " Random seed: " + RANDOM_SEED);
    }
  }

  /**
   * Helper method for testing trim results.
   * Sets a max size on the result holder and adds more values than the max size of the result holder.
   * Then asserts that the right results survive after trimming.
   *
   * @param minOrder
   */
  void testTrimResults(final boolean minOrder) {
    GroupByResultHolder resultHolder =
        new DoubleGroupByResultHolder(INITIAL_CAPACITY, MAX_CAPACITY, INITIAL_CAPACITY, DEFAULT_VALUE, minOrder);
    List<IntDoublePair> expected = new ArrayList<>(MAX_CAPACITY);

    for (int i = 0; i < INITIAL_CAPACITY; i++) {
      resultHolder.setValueForKey(i, _expected[i]);
      expected.add(new IntDoublePair(i, _expected[i]));
    }

    // This will trigger the transition from array based to map based storage within the result holder.
    resultHolder.ensureCapacity(MAX_CAPACITY);

    for (int i = INITIAL_CAPACITY; i < MAX_CAPACITY; i++) {
      resultHolder.setValueForKey(i, _expected[i]);
      expected.add(new IntDoublePair(i, _expected[i]));
    }

    // Trim the results.
    resultHolder.trimResults();

    // Sort the input
    Collections.sort(expected, new IntDoubleComparator(!minOrder));

    // Ensure that all the correct group keys survive after trimming.
    for (int i = 0; i < INITIAL_CAPACITY; i++) {
      IntDoublePair pair = expected.get(i);
      Assert.assertEquals(resultHolder.getDoubleResult(pair.getIntValue()), pair.getDoubleValue());
    }
  }
}
