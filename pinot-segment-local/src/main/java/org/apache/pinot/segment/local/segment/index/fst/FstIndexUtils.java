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
package org.apache.pinot.segment.local.segment.index.fst;

import org.apache.pinot.segment.spi.memory.PinotDataBuffer;


/**
 * Utilities for handling legacy native FST indexes during the Lucene-only transition.
 */
public final class FstIndexUtils {
  private static final int LEGACY_NATIVE_FST_MAGIC = ('\\' << 24) | ('f' << 16) | ('s' << 8) | 'a';

  private FstIndexUtils() {
  }

  public static boolean isLegacyNativeFst(PinotDataBuffer fstBuffer) {
    if (fstBuffer.size() < Integer.BYTES) {
      return false;
    }
    return fstBuffer.getInt(0) == LEGACY_NATIVE_FST_MAGIC;
  }
}
