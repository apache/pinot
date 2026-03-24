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
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.cpc.CpcUnion;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.spi.utils.CommonConstants;


public class DistinctCountCPCSketchAggregator implements ValueAggregator {

  public DistinctCountCPCSketchAggregator() {
  }

  @Override
  public Object aggregate(Object value1, Object value2, Map<String, String> functionParameters) {
    byte[] bytes1 = (byte[]) value1;
    byte[] bytes2 = (byte[]) value2;

    // Empty byte arrays represent the default null value for BYTES columns.
    // When both are empty, produce a serialized empty sketch so the stored value
    // is always a valid sketch and does not propagate byte[0] into merged segments.
    if (bytes1.length == 0 && bytes2.length == 0) {
      String lgKParam = functionParameters.get(Constants.CPCSKETCH_LGK_KEY);
      int lgK = lgKParam != null ? Integer.parseInt(lgKParam) : CommonConstants.Helix.DEFAULT_CPC_SKETCH_LGK;
      return ObjectSerDeUtils.DATA_SKETCH_CPC_SER_DE.serialize(new CpcSketch(lgK));
    }
    if (bytes1.length == 0) {
      return bytes2;
    }
    if (bytes2.length == 0) {
      return bytes1;
    }

    CpcSketch first = ObjectSerDeUtils.DATA_SKETCH_CPC_SER_DE.deserialize(bytes1);
    CpcSketch second = ObjectSerDeUtils.DATA_SKETCH_CPC_SER_DE.deserialize(bytes2);
    CpcUnion union;

    String lgKParam = functionParameters.get(Constants.CPCSKETCH_LGK_KEY);
    if (lgKParam != null) {
      union = new CpcUnion(Integer.parseInt(lgKParam));
    } else {
      // If the functionParameters don't have an explicit lgK value set,
      // use the default value for nominal entries
      union = new CpcUnion(CommonConstants.Helix.DEFAULT_CPC_SKETCH_LGK);
    }
    union.update(first);
    union.update(second);
    return ObjectSerDeUtils.DATA_SKETCH_CPC_SER_DE.serialize(union.getResult());
  }
}
