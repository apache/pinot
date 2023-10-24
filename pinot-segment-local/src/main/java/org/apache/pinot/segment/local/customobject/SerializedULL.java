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

import com.dynatrace.hash4j.distinctcount.UltraLogLog;
import java.util.Base64;


public class SerializedULL implements Comparable<SerializedULL> {
  private final UltraLogLog _ull;

  public SerializedULL(UltraLogLog ull) {
    _ull = ull;
  }

  @Override
  public int compareTo(SerializedULL other) {
    return Double.compare(_ull.getDistinctCountEstimate(), other._ull.getDistinctCountEstimate());
  }

  @Override
  public String toString() {
    // Note this returns the ULL state, so it's easier for someone to load with just UltraLogLog.wrap
    return Base64.getEncoder().encodeToString(_ull.getState());
  }
}
