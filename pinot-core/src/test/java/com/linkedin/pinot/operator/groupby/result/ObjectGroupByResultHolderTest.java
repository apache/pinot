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
package com.linkedin.pinot.operator.groupby.result;

import com.linkedin.pinot.common.utils.Pairs;
import com.linkedin.pinot.common.utils.Pairs.IntObjectPair;
import com.linkedin.pinot.core.operator.aggregation.groupby.GroupByResultHolder;
import com.linkedin.pinot.core.operator.aggregation.groupby.ObjectGroupByResultHolder;
import com.linkedin.pinot.core.query.aggregation.function.AvgAggregationFunction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit test for {@link ObjectGroupByResultHolder}
 */
public class ObjectGroupByResultHolderTest {

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
  private void testTrimResults(final boolean minOrder) {
    long seed = System.nanoTime();
    Random random = new Random(seed);

    GroupByResultHolder resultHolder = new ObjectGroupByResultHolder(INITIAL_CAPACITY, INITIAL_CAPACITY, minOrder);
    List<IntObjectPair> expected = new ArrayList<>(MAX_CAPACITY);

    for (int i = 0; i < INITIAL_CAPACITY; i++) {
      AvgAggregationFunction.AvgPair avgPair =
          new AvgAggregationFunction.AvgPair((double) random.nextInt(1000), (long) random.nextInt(100));
      IntObjectPair value = new IntObjectPair(i, avgPair);
      expected.add(value);
      resultHolder.setValueForKey(i, avgPair);
    }

    // This will trigger switch from array based storage to map based storage.
    resultHolder.ensureCapacity(MAX_CAPACITY);

    for (int i = INITIAL_CAPACITY; i < MAX_CAPACITY; i++) {
      AvgAggregationFunction.AvgPair avgPair =
          new AvgAggregationFunction.AvgPair((double) random.nextInt(1000), (long) random.nextInt(100));
      IntObjectPair value = new IntObjectPair(i, avgPair);
      expected.add(value);
      resultHolder.setValueForKey(i, avgPair);
    }

    Collections.sort(expected, new Pairs.IntObjectComparator(!minOrder));

    // Trim the results to INITIAL_CAPACITY.
    resultHolder.trimResults(INITIAL_CAPACITY);

    // Ensure that all the correct group keys survive after trimming.
    for (int i = 0; i < INITIAL_CAPACITY; i++) {
      IntObjectPair pair = expected.get(i);
      AvgAggregationFunction.AvgPair exp = (AvgAggregationFunction.AvgPair) pair.getObjectValue();
      AvgAggregationFunction.AvgPair act = resultHolder.getResult(pair.getIntValue());

      Assert.assertEquals(act.getFirst(), exp.getFirst());
      Assert.assertEquals(act.getSecond(), exp.getSecond());
    }
  }
}
