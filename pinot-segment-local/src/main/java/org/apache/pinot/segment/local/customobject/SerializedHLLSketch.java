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

import org.apache.datasketches.hll.HllSketch;
import org.apache.pinot.segment.local.utils.CustomSerDeUtils;
import org.apache.pinot.spi.utils.BytesUtils;


public class SerializedHLLSketch implements Comparable<SerializedHLLSketch> {
  private final HllSketch _hyperLogLog;

  public SerializedHLLSketch(HllSketch hyperLogLog) {
    _hyperLogLog = hyperLogLog;
  }

  @Override
  public int compareTo(SerializedHLLSketch other) {
    return Long.compare((long) _hyperLogLog.getEstimate(), (long) other._hyperLogLog.getEstimate());
  }

  @Override
  public String toString() {
    return BytesUtils.toHexString(CustomSerDeUtils.HYPER_LOG_LOG_SKETCH_SER_DE.serialize(_hyperLogLog));
  }
}
