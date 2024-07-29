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

import com.dynatrace.hash4j.distinctcount.UltraLogLog;
import org.apache.pinot.core.common.ObjectSerDeUtils;


public class DistinctCountULLAggregator implements ValueAggregator {
  @Override
  public Object aggregate(Object value1, Object value2) {
    UltraLogLog first = ObjectSerDeUtils.ULTRA_LOG_LOG_OBJECT_SER_DE.deserialize((byte[]) value1);
    UltraLogLog second = ObjectSerDeUtils.ULTRA_LOG_LOG_OBJECT_SER_DE.deserialize((byte[]) value2);
    // add to the one with a larger P and return that
    if (first.getP() >= second.getP()) {
      first.add(second);
      return ObjectSerDeUtils.ULTRA_LOG_LOG_OBJECT_SER_DE.serialize(first);
    } else {
      second.add(first);
      return ObjectSerDeUtils.ULTRA_LOG_LOG_OBJECT_SER_DE.serialize(second);
    }
  }
}
