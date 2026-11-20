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
package org.apache.pinot.segment.local.aggregator;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import java.util.List;
import org.apache.pinot.segment.local.utils.HyperLogLogPlusUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class DistinctCountHLLPlusValueAggregatorTest {

  @Test
  public void testMaxAggregatedValueByteSizeForSerializedValues() {
    // Default p and sp, which differ from the ones of the serialized values
    DistinctCountHLLPlusValueAggregator aggregator = new DistinctCountHLLPlusValueAggregator(List.of());
    int p = 12;
    int sp = 20;

    HyperLogLogPlus value = aggregator.getInitialAggregatedValue(serialize(aggregator, p, sp, 0, 10));
    value = aggregator.applyRawValue(value, serialize(aggregator, p, sp, 10, 2000));
    aggregator.getInitialAggregatedValue(serialize(aggregator, p, sp, 0, 5));

    int maxByteSize = aggregator.getMaxAggregatedValueByteSize();
    assertEquals(maxByteSize, HyperLogLogPlusUtils.maxByteSize(p, sp));
    assertTrue(aggregator.serializeAggregatedValue(value).length <= maxByteSize);
  }

  private static byte[] serialize(DistinctCountHLLPlusValueAggregator aggregator, int p, int sp, int start, int end) {
    HyperLogLogPlus hllPlus = new HyperLogLogPlus(p, sp);
    for (int i = start; i < end; i++) {
      hllPlus.offer(i);
    }
    return aggregator.serializeAggregatedValue(hllPlus);
  }
}
