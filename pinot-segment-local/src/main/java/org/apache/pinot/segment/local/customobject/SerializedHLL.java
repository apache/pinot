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

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import org.apache.pinot.segment.local.utils.CustomSerDeUtils;
import org.apache.pinot.spi.utils.BytesUtils;


public class SerializedHLL implements Comparable<SerializedHLL> {
  private final HyperLogLog _hyperLogLog;

  public SerializedHLL(HyperLogLog hyperLogLog) {
    _hyperLogLog = hyperLogLog;
  }

  @Override
  public int compareTo(SerializedHLL other) {
    return Long.compare(_hyperLogLog.cardinality(), other._hyperLogLog.cardinality());
  }

  @Override
  public String toString() {
    return BytesUtils.toHexString(CustomSerDeUtils.HYPER_LOG_LOG_SER_DE.serialize(_hyperLogLog));
  }
}
