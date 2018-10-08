/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.startree.v2;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.common.ObjectSerDeUtils;
import com.linkedin.pinot.core.data.aggregator.DistinctCountHLLValueAggregator;
import com.linkedin.pinot.core.data.aggregator.ValueAggregator;
import java.util.Random;

import static org.testng.Assert.*;


public class PreAggregatedDistinctCountHLLStarTreeV2Test extends BaseStarTreeV2Test<Object, HyperLogLog> {
  // Use non-default log2m
  private static final int LOG2M = 7;

  @Override
  ValueAggregator<Object, HyperLogLog> getValueAggregator() {
    return new DistinctCountHLLValueAggregator();
  }

  @Override
  DataType getRawValueType() {
    return DataType.BYTES;
  }

  @Override
  Object getRandomRawValue(Random random) {
    HyperLogLog hyperLogLog = new HyperLogLog(LOG2M);
    hyperLogLog.offer(random.nextInt(100));
    hyperLogLog.offer(random.nextInt(100));
    return ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.serialize(hyperLogLog);
  }

  @Override
  void assertAggregatedValue(HyperLogLog starTreeResult, HyperLogLog nonStarTreeResult) {
    assertEquals(starTreeResult.cardinality(), nonStarTreeResult.cardinality());
  }
}
