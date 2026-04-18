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

import com.tdunning.math.stats.TDigest;
import java.util.Map;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.function.PercentileTDigestAggregationFunction;
import org.apache.pinot.segment.spi.Constants;


/**
 * Aggregator for merging serialized TDigest sketches during segment processing
 * (e.g., MergeAndRollup). Handles both {@code PERCENTILETDIGEST} and
 * {@code PERCENTILERAWTDIGEST} aggregation types
 */
public class PercentileTDigestAggregator implements ValueAggregator {

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

    int compression = getCompression(functionParameters);
    TDigest first = ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(bytes1);
    TDigest second = ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(bytes2);
    TDigest merged = TDigest.createMergingDigest(compression);
    merged.add(first);
    merged.add(second);
    return ObjectSerDeUtils.TDIGEST_SER_DE.serialize(merged);
  }

  private int getCompression(Map<String, String> functionParameters) {
    String compressionParam = functionParameters.get(Constants.PERCENTILETDIGEST_COMPRESSION_FACTOR_KEY);
    return compressionParam != null
        ? Integer.parseInt(compressionParam)
        : PercentileTDigestAggregationFunction.DEFAULT_TDIGEST_COMPRESSION;
  }
}
