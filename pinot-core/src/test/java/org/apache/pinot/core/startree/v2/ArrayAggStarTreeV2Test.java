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


/// Exercises the distinct `arrayAgg` [ArrayAggDistinctValueAggregator] through the full star-tree build and read path,
/// comparing the star-tree-aggregated distinct set against the raw scan for the same query. Uses a low-cardinality
/// LONG column so the distinct sets overlap heavily and dedup on merge is actually exercised.
public class ArrayAggStarTreeV2Test extends BaseStarTreeV2Test<Object, ObjectSet<Object>> {

  @Override
  ValueAggregator<Object, ObjectSet<Object>> getValueAggregator() {
    return new ArrayAggDistinctValueAggregator();
  }

  @Override
  String getAggregation(AggregationFunctionType aggregationType) {
    // arrayAgg requires the data type literal and the distinct flag; only distinct arrayAgg is star-tree eligible.
    return "arrayAgg(m, 'LONG', true)";
  }

  @Override
  DataType getRawValueType() {
    return DataType.LONG;
  }

  @Override
  Object getRandomRawValue(Random random) {
    return (long) random.nextInt(50);
  }

  @Override
  void assertAggregatedValue(ObjectSet<Object> starTreeResult, ObjectSet<Object> nonStarTreeResult) {
    // Distinct arrayAgg is order-independent; comparing the sets directly is the correct equality.
    assertEquals(starTreeResult, nonStarTreeResult);
  }
}
