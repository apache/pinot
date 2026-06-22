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
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.segment.local.customobject.AvgPair;


/// Aggregator for merging serialized [AvgPair] (sum + count) values during segment processing
/// (e.g. MergeRollupTask / RealtimeToOfflineSegmentsTask).
///
/// The column must store a serialized `AvgPair`, i.e. a `BYTES` column produced by an `AVG` ingestion
/// aggregation (or star-tree). Merging adds the sums and counts so the average stays correct across
/// multiple rollup levels — averaging already-averaged scalars would be wrong. The final `sum / count`
/// division happens at query time in `AvgAggregationFunction`, which already reads the same serialized
/// `AvgPair` format.
public class AvgValueAggregator implements ValueAggregator {

  /// Merges two serialized `AvgPair` values into one. An empty `byte[]` (the default null value for a `BYTES`
  /// column) is treated as a missing value, so the other side is returned unchanged; if both are empty, the
  /// empty value is returned.
  @Override
  public Object aggregate(Object value1, Object value2, Map<String, String> functionParameters) {
    byte[] bytes1 = (byte[]) value1;
    byte[] bytes2 = (byte[]) value2;

    // Treat empty byte arrays (default null value for BYTES columns) as missing values
    if (bytes1.length == 0) {
      return bytes2;
    } else if (bytes2.length == 0) {
      return bytes1;
    }

    AvgPair first = ObjectSerDeUtils.AVG_PAIR_SER_DE.deserialize(bytes1);
    AvgPair second = ObjectSerDeUtils.AVG_PAIR_SER_DE.deserialize(bytes2);
    first.apply(second);
    return ObjectSerDeUtils.AVG_PAIR_SER_DE.serialize(first);
  }
}
