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

import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Union;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.spi.utils.CommonConstants;


public class DistinctCountThetaSketchAggregator implements ValueAggregator {

  private final Union _union;

  public DistinctCountThetaSketchAggregator() {
    // TODO: Handle configurable nominal entries
    _union = Union.builder().setNominalEntries(CommonConstants.Helix.DEFAULT_THETA_SKETCH_NOMINAL_ENTRIES).buildUnion();
  }

  @Override
  public Object aggregate(Object value1, Object value2) {
    Sketch first = ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.deserialize((byte[]) value1);
    Sketch second = ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.deserialize((byte[]) value2);
    Sketch result = _union.union(first, second);
    return ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.serialize(result);
  }
}
