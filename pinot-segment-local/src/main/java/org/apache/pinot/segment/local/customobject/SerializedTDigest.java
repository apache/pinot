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
package org.apache.pinot.segment.local.customobject;

import com.tdunning.math.stats.TDigest;
import org.apache.pinot.segment.local.utils.CustomSerDeUtils;
import org.apache.pinot.spi.utils.BytesUtils;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Serialized and comparable version of TDigest. Compares TDigest for a specific percentile value.
 */
public class SerializedTDigest implements Comparable<SerializedTDigest> {
  private final double _percentile;
  private final TDigest _tDigest;

  public SerializedTDigest(TDigest tDigest, double percentile) {
    _tDigest = tDigest;
    _percentile = percentile;
  }

  @Override
  public int compareTo(SerializedTDigest other) {
    checkArgument(other._percentile == _percentile, "Percentile number doesn't match!");
    return Double.compare(_tDigest.quantile(_percentile / 100.0), other._tDigest.quantile(_percentile / 100.0));
  }

  @Override
  public String toString() {
    return BytesUtils.toHexString(CustomSerDeUtils.TDIGEST_SER_DE.serialize(_tDigest));
  }
}
