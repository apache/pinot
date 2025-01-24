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
package org.apache.pinot.core.segment.processing.aggregator;

import java.util.Map;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * Class to merge KLL doubles sketch for minion merge/rollup tasks.
 */
public class PercentileKLLSketchAggregator implements ValueAggregator {

  /**
   * Given two kll doubles sketches, return the aggregated kll doubles sketches
   * @return aggregated sketch given two kll doubles sketches
   */
  @Override
  public Object aggregate(Object value1, Object value2, Map<String, String> functionParameters) {
    try {
      String kParam = functionParameters.get(Constants.KLL_DOUBLE_SKETCH_K);

      int sketchKValue;

      // Check if nominal entries values match
      if (kParam != null) {
        sketchKValue = Integer.parseInt(kParam);
      } else {
        // If the functionParameters don't have an explicit K use the default value for K
        sketchKValue = CommonConstants.Helix.DEFAULT_KLL_SKETCH_K;
      }

      KllDoublesSketch first = ObjectSerDeUtils.KLL_SKETCH_SER_DE.deserialize((byte[]) value1);
      KllDoublesSketch second = ObjectSerDeUtils.KLL_SKETCH_SER_DE.deserialize((byte[]) value2);
      KllDoublesSketch union = KllDoublesSketch.newHeapInstance(sketchKValue);
      union.merge(first);
      union.merge(second);
      return ObjectSerDeUtils.KLL_SKETCH_SER_DE.serialize(union);
    } catch (SketchesArgumentException e) {
      throw new RuntimeException(e);
    }
  }
}
