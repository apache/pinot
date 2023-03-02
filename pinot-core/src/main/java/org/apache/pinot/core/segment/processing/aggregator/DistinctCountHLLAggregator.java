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

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import org.apache.pinot.core.common.ObjectSerDeUtils;


public class DistinctCountHLLAggregator implements ValueAggregator {
  @Override
  public Object aggregate(Object value1, Object value2) {
    try {
      HyperLogLog first = ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize((byte[]) value1);
      HyperLogLog second = ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize((byte[]) value2);
      first.addAll(second);
      return ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.serialize(first);
    } catch (CardinalityMergeException e) {
      throw new RuntimeException(e);
    }
  }
}
