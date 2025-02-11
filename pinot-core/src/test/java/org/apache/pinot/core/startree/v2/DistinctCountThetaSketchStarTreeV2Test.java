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

import java.util.Collections;
import java.util.Random;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Union;
import org.apache.pinot.segment.local.aggregator.DistinctCountThetaSketchValueAggregator;
import org.apache.pinot.segment.local.aggregator.ValueAggregator;
import org.apache.pinot.spi.data.FieldSpec.DataType;

import static org.testng.Assert.assertEquals;


public class DistinctCountThetaSketchStarTreeV2Test extends BaseStarTreeV2Test<Object, Object> {

  @Override
  ValueAggregator<Object, Object> getValueAggregator() {
    return new DistinctCountThetaSketchValueAggregator(Collections.emptyList());
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
  void assertAggregatedValue(Object starTreeResult, Object nonStarTreeResult) {
    // Use error at (lgK=14, stddev=2) from:
    // https://datasketches.apache.org/docs/Theta/ThetaErrorTable.html
    double delta = (1 << 14) * 0.01563;
    assertEquals(toSketch(starTreeResult).getEstimate(), toSketch(nonStarTreeResult).getEstimate(), delta);
  }

  private Sketch toSketch(Object value) {
    if (value instanceof Union) {
      return ((Union) value).getResult();
    } else if (value instanceof Sketch) {
      return (Sketch) value;
    } else {
      throw new IllegalStateException(
          "Unsupported data type for Theta Sketch aggregation: " + value.getClass().getSimpleName());
    }
  }
}
