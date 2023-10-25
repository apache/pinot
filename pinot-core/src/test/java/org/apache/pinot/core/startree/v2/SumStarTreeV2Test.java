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
import org.apache.pinot.segment.local.aggregator.SumValueAggregator;
import org.apache.pinot.segment.local.aggregator.ValueAggregator;
import org.apache.pinot.spi.data.FieldSpec.DataType;

import static org.testng.Assert.assertEquals;


public class SumStarTreeV2Test extends BaseStarTreeV2Test<Number, Double> {

  @Override
  ValueAggregator<Number, Double> getValueAggregator() {
    return new SumValueAggregator();
  }

  @Override
  DataType getRawValueType() {
    return DataType.INT;
  }

  @Override
  Number getRandomRawValue(Random random) {
    return random.nextInt();
  }

  @Override
  protected void assertAggregatedValue(Double starTreeResult, Double nonStarTreeResult) {
    assertEquals(starTreeResult, nonStarTreeResult, 1e-5);
  }
}
