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
package org.apache.pinot.spi.utils;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.cardinality.RegisterSet;
import java.io.IOException;


public class HyperLogLogUtils {
  private HyperLogLogUtils() {
  }

  /**
   * Returns the byte size of the given HyperLogLog.
   */
  public static int byteSize(HyperLogLog value)
      throws IOException {
    return value.getBytes().length;
  }

  /**
   * Returns the byte size of hyperloglog of a given log2m.
   */
  public static int byteSize(int log2m) {
    return ((RegisterSet.getSizeForCount(1 << log2m) + 2) * Integer.BYTES);
  }
}
