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

import com.linkedin.pinot.common.utils.Pairs;
import com.linkedin.pinot.common.utils.Pairs.IntObjectPair;
import com.linkedin.pinot.core.query.aggregation.function.customobject.AvgPair;
import com.linkedin.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import com.linkedin.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit test for {@link ObjectGroupByResultHolder}
 */
public class ObjectGroupByResultHolderTest {
  private static final long RANDOM_SEED = System.currentTimeMillis();
  private static final Random RANDOM = new Random(RANDOM_SEED);
  private static final String ERROR_MESSAGE = "Random seed: " + RANDOM_SEED;

  private static final int INITIAL_CAPACITY = 100;
  private static final int MAX_CAPACITY = 1000;

  /**
   * Tests that trimming of results with max ordering is correct.
   */
  @Test
  public void testMaxTrimResults() {
    testTrimResults(false);
  }

  /**
   * Tests that trimming of results with min ordering is correct.
   */
  @Test
  public void testMinTrimResults() {
    testTrimResults(true);
  }

  /**
   * Helper method for testing trimming of results.
   * @param minOrder Min ordering if true, max ordering else.
   */
  private void testTrimResults(boolean minOrder) {
    GroupByResultHolder resultHolder =
        new ObjectGroupByResultHolder(INITIAL_CAPACITY, MAX_CAPACITY, INITIAL_CAPACITY, minOrder);
    List<IntObjectPair<AvgPair>> expectedIntObjectPairs = new ArrayList<>(MAX_CAPACITY);

    for (int i = 0; i < INITIAL_CAPACITY; i++) {
      AvgPair avgPair = new AvgPair(RANDOM.nextDouble(), (long) RANDOM.nextInt(100));
      IntObjectPair<AvgPair> intObjectPair = new IntObjectPair<>(i, avgPair);
      expectedIntObjectPairs.add(intObjectPair);
      resultHolder.setValueForKey(i, avgPair);
    }

    // This will trigger switch from array based storage to map based storage.
    resultHolder.ensureCapacity(MAX_CAPACITY);

    for (int i = INITIAL_CAPACITY; i < MAX_CAPACITY; i++) {
      AvgPair avgPair = new AvgPair(RANDOM.nextDouble(), (long) RANDOM.nextInt(100));
      IntObjectPair<AvgPair> intObjectPair = new IntObjectPair<>(i, avgPair);
      expectedIntObjectPairs.add(intObjectPair);
      resultHolder.setValueForKey(i, avgPair);
    }

    Collections.sort(expectedIntObjectPairs, new Pairs.IntObjectComparator(!minOrder));

    // Trim the results.
    Assert.assertEquals(resultHolder.trimResults().length, MAX_CAPACITY - INITIAL_CAPACITY, ERROR_MESSAGE);

    // Ensure that all the correct group keys survive after trimming.
    for (int i = 0; i < INITIAL_CAPACITY; i++) {
      IntObjectPair intObjectPair = expectedIntObjectPairs.get(i);
      AvgPair actualAvgPair = resultHolder.getResult(intObjectPair.getIntValue());
      AvgPair expectedAvgPair = (AvgPair) intObjectPair.getObjectValue();

      Assert.assertEquals(actualAvgPair.getSum(), expectedAvgPair.getSum(), ERROR_MESSAGE);
      Assert.assertEquals(actualAvgPair.getCount(), expectedAvgPair.getCount(), ERROR_MESSAGE);
    }
  }
}
