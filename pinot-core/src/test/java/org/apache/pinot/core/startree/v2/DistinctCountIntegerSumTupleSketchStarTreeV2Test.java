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
import org.apache.datasketches.tuple.Sketch;
import org.apache.datasketches.tuple.aninteger.IntegerSketch;
import org.apache.datasketches.tuple.aninteger.IntegerSummary;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.segment.local.aggregator.IntegerTupleSketchValueAggregator;
import org.apache.pinot.segment.local.aggregator.ValueAggregator;
import org.apache.pinot.spi.data.FieldSpec.DataType;

import static org.testng.Assert.assertEquals;


public class DistinctCountIntegerSumTupleSketchStarTreeV2Test
    extends BaseStarTreeV2Test<byte[], Sketch<IntegerSummary>> {

  @Override
  ValueAggregator<byte[], Sketch<IntegerSummary>> getValueAggregator() {
    return new IntegerTupleSketchValueAggregator(IntegerSummary.Mode.Sum);
  }

  @Override
  DataType getRawValueType() {
    return DataType.BYTES;
  }

  @Override
  byte[] getRandomRawValue(Random random) {
    IntegerSketch is = new IntegerSketch(4, IntegerSummary.Mode.Sum);
    is.update(random.nextInt(100), random.nextInt(100));
    return ObjectSerDeUtils.DATA_SKETCH_INT_TUPLE_SER_DE.serialize(is.compact());
  }

  @Override
  void assertAggregatedValue(Sketch<IntegerSummary> starTreeResult, Sketch<IntegerSummary> nonStarTreeResult) {
    assertEquals(starTreeResult.getEstimate(), nonStarTreeResult.getEstimate());
  }
}
