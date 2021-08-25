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

import org.apache.pinot.segment.local.utils.CustomSerDeUtils;
import org.apache.pinot.spi.utils.BytesUtils;

import static com.google.common.base.Preconditions.checkArgument;


/**
 * Serialized and comparable version of QuantileDigest. Compares QuantileDigest for a specific percentile value.
 */
public class SerializedQuantileDigest implements Comparable<SerializedQuantileDigest> {
  private final double _percentile;
  private final QuantileDigest _quantileDigest;

  public SerializedQuantileDigest(QuantileDigest quantileDigest, double percentile) {
    _quantileDigest = quantileDigest;
    _percentile = percentile / 100.0;
  }

  @Override
  public int compareTo(SerializedQuantileDigest other) {
    checkArgument(other._percentile == _percentile, "Percentile number doesn't match!");
    return Long.compare(_quantileDigest.getQuantile(_percentile),
        other._quantileDigest.getQuantile(_percentile));
  }

  @Override
  public String toString() {
    return BytesUtils.toHexString(CustomSerDeUtils.QUANTILE_DIGEST_SER_DE.serialize(_quantileDigest));
  }
}
