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

import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.cpc.CpcUnion;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.spi.utils.CommonConstants;


public class DistinctCountCPCAggregator implements ValueAggregator {

  public DistinctCountCPCAggregator() {
  }

  @Override
  public Object aggregate(Object value1, Object value2) {
    CpcSketch first = ObjectSerDeUtils.DATA_SKETCH_CPC_SER_DE.deserialize((byte[]) value1);
    CpcSketch second = ObjectSerDeUtils.DATA_SKETCH_CPC_SER_DE.deserialize((byte[]) value2);
    CpcSketch result;
    if (first == null && second == null) {
      result = new CpcSketch(CommonConstants.Helix.DEFAULT_CPC_SKETCH_LGK);
    } else if (second == null) {
      result = first;
    } else if (first == null) {
      result = second;
    } else {
      CpcUnion union = new CpcUnion(CommonConstants.Helix.DEFAULT_CPC_SKETCH_LGK);
      union.update(first);
      union.update(second);
      result = union.getResult();
    }
    return ObjectSerDeUtils.DATA_SKETCH_CPC_SER_DE.serialize(result);
  }
}
