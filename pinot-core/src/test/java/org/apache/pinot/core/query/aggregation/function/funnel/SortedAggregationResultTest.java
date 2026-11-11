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
package org.apache.pinot.core.query.aggregation.function.funnel;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SortedAggregationResultTest {

  @Test
  public void testMultiKeyExtractResultDoesNotDoubleCount() {
    // Two entities (primary key 0 and 1), each completing both steps.
    // Expected: stepCounters = [2, 2] (one completion per entity per step).
    SortedAggregationResult result = new SortedAggregationResult(2, 2);
    result.addMultiKey(0, new int[]{0, 10});
    result.addMultiKey(1, new int[]{0, 10});
    result.addMultiKey(0, new int[]{1, 20});
    result.addMultiKey(1, new int[]{1, 20});

    LongArrayList counts = result.extractResult();
    Assert.assertEquals(counts.getLong(0), 2L, "step 0 count");
    Assert.assertEquals(counts.getLong(1), 2L, "step 1 count");
  }

  @Test
  public void testMultiKeySecondaryKeysWithinPrimaryGroup() {
    // Primary key 0 with two secondary keys: (0,10) and (0,20).
    // (0,10) completes both steps; (0,20) completes only step 0.
    // Expected: stepCounters = [2, 1].
    SortedAggregationResult result = new SortedAggregationResult(2, 2);
    result.addMultiKey(0, new int[]{0, 10});
    result.addMultiKey(1, new int[]{0, 10});
    result.addMultiKey(0, new int[]{0, 20});

    LongArrayList counts = result.extractResult();
    Assert.assertEquals(counts.getLong(0), 2L, "step 0 count");
    Assert.assertEquals(counts.getLong(1), 1L, "step 1 count");
  }
}
