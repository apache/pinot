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
import com.linkedin.pinot.core.data.aggregator.DistinctCountHLLValueAggregator;
import com.linkedin.pinot.core.data.aggregator.ValueAggregator;
import java.util.Random;

import static org.testng.Assert.*;


public class DistinctCountHLLStarTreeV2Test extends BaseStarTreeV2Test<Object, HyperLogLog> {

  @Override
  ValueAggregator<Object, HyperLogLog> getValueAggregator() {
    return new DistinctCountHLLValueAggregator();
  }

  @Override
  DataType getRawValueType() {
    return DataType.INT;
  }

  @Override
  Object getRandomRawValue(Random random) {
    return random.nextInt(100);
  }

  @Override
  void assertAggregatedValue(HyperLogLog starTreeResult, HyperLogLog nonStarTreeResult) {
    assertEquals(starTreeResult.cardinality(), nonStarTreeResult.cardinality());
  }
}
