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

import org.apache.pinot.spi.exception.EarlyTerminationException;
import org.apache.pinot.spi.trace.Tracing;


public class LoopUtils {

  private LoopUtils() {
  }
  public static final int MAX_ENTRIES_KEYS_MERGED_PER_INTERRUPTION_CHECK_MASK = 0b1_1111_1111_1111;

  // Check for thread interruption, every time after merging 8192 keys
  public static void checkMergePhaseInterruption(int mergedKeys) {
    if ((mergedKeys & MAX_ENTRIES_KEYS_MERGED_PER_INTERRUPTION_CHECK_MASK) == 0
        && (Tracing.ThreadAccountantOps.isInterrupted())) {
      throw new EarlyTerminationException();
    }
  }
}
