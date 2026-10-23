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

import it.unimi.dsi.fastutil.objects.ObjectSet;
import java.util.Random;
import org.apache.pinot.segment.local.aggregator.ArrayAggDistinctValueAggregator;
import org.apache.pinot.segment.local.aggregator.ValueAggregator;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;

import static org.testng.Assert.assertEquals;


/// Same as [ArrayAggStarTreeV2Test] but over a BYTES source column, whose raw values are structurally identical to
/// the serialized-set cells the star-tree stores — the case that requires the pre-aggregation provenance marker.
public class ArrayAggBytesStarTreeV2Test extends BaseStarTreeV2Test<Object, ObjectSet<Object>> {

  @Override
  ValueAggregator<Object, ObjectSet<Object>> getValueAggregator() {
    return new ArrayAggDistinctValueAggregator();
  }

  @Override
  String getAggregation(AggregationFunctionType aggregationType) {
    // arrayAgg requires the data type literal and the distinct flag; only distinct arrayAgg is star-tree eligible.
    return "arrayAgg(m, 'BYTES', true)";
  }

  @Override
  DataType getRawValueType() {
    return DataType.BYTES;
  }

  @Override
  Object getRandomRawValue(Random random) {
    int value = random.nextInt(50);
    return new byte[]{(byte) value, (byte) (value + 1)};
  }

  @Override
  void assertAggregatedValue(ObjectSet<Object> starTreeResult, ObjectSet<Object> nonStarTreeResult) {
    // Distinct arrayAgg is order-independent; the aggregator normalizes byte[] elements to ByteArray, so the sets
    // compare by content.
    assertEquals(starTreeResult, nonStarTreeResult);
  }
}
