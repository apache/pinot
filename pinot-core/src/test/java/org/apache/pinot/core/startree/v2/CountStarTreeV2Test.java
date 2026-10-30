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
import org.apache.pinot.segment.local.aggregator.CountValueAggregator;
import org.apache.pinot.segment.local.aggregator.ValueAggregator;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class CountStarTreeV2Test extends BaseStarTreeV2Test<Object, Long> {

  @Test
  public void testNonOverlappingOrRangesWithAnAdditionalPredicate()
      throws Exception {
    testQuery(String.format("SELECT COUNT(*) FROM %s WHERE %s != 5 AND (%s < 10 OR %s > 90)",
        TABLE_NAME, DIMENSION1, DIMENSION1, DIMENSION1));
  }

  @Override
  ValueAggregator<Object, Long> getValueAggregator() {
    return new CountValueAggregator();
  }

  @Override
  DataType getRawValueType() {
    return null;
  }

  @Override
  Void getRandomRawValue(Random random) {
    return null;
  }

  @Override
  protected void assertAggregatedValue(Long starTreeResult, Long nonStarTreeResult) {
    assertEquals(starTreeResult, nonStarTreeResult);
  }
}
