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
package org.apache.pinot.core.startree.v2;

import java.util.Random;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.segment.local.aggregator.MinMaxRangeValueAggregator;
import org.apache.pinot.segment.local.aggregator.ValueAggregator;
import org.apache.pinot.segment.local.customobject.MinMaxRangePair;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;

import static org.testng.Assert.assertEquals;


public class PreAggregatedMinMaxRangeStarTreeV2Test extends BaseStarTreeV2Test<Object, MinMaxRangePair> {

  @Override
  ValueAggregator<Object, MinMaxRangePair> getValueAggregator() {
    return new MinMaxRangeValueAggregator();
  }

  @Override
  DataType getRawValueType() {
    return DataType.BYTES;
  }

  @Override
  Object getRandomRawValue(Random random) {
    long value1 = random.nextInt();
    long value2 = random.nextInt();
    return ObjectSerDeUtils.MIN_MAX_RANGE_PAIR_SER_DE.serialize(
        new MinMaxRangePair(Math.min(value1, value2), Math.max(value1, value2)));
  }

  @Override
  protected void assertAggregatedValue(MinMaxRangePair starTreeResult, MinMaxRangePair nonStarTreeResult) {
    assertEquals(starTreeResult.getMin(), nonStarTreeResult.getMin(), 1e-5);
    assertEquals(starTreeResult.getMax(), nonStarTreeResult.getMax(), 1e-5);
  }

  @Override
  protected FieldConfig.CompressionCodec getCompressionCodec() {
    return FieldConfig.CompressionCodec.LZ4;
  }
}
