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
package org.apache.pinot.segment.spi.memory.unsafe;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.EnumUtils;

/**
 * Simple singleton config for managing advanced mmap configurations
 */
public class MmapMemoryConfig {
  private MmapMemoryConfig() { }
  private static final MmapMemoryConfig INSTANCE = new MmapMemoryConfig();

  enum Advice {
    NORMAL(MmapMemory.LibC.POSIX_MADV_NORMAL),
    RANDOM(MmapMemory.LibC.POSIX_MADV_RANDOM),
    SEQUENTIAL(MmapMemory.LibC.POSIX_MADV_SEQUENTIAL),
    WILL_NEED(MmapMemory.LibC.POSIX_MADV_WILLNEED),
    DONT_NEED(MmapMemory.LibC.POSIX_MADV_DONTNEED);

    private final int _advice;

    Advice(int advice) {
      _advice = advice;
    }

    /**
     *  Get posix-compatible advice integer
     */
    public int getAdvice() {
      return _advice;
    }
  }

  /**
   * Advice to use by default after calling mmap on a region of a file.
   * Notably this is expected to be an integer corresponding to advice
   * supported by posix_madvise()
   */
  private int _defaultAdvice = -1;

  public static int getDefaultAdvice() {
    return INSTANCE._defaultAdvice;
  }

  public static void setDefaultAdvice(int advice) {
    Preconditions.checkArgument(
        advice >= 0 && advice <= 4,
        "Default advice for mmap buffers must be posix_madvise compatible (0-4): %d",
        advice
    );
    INSTANCE._defaultAdvice = advice;
  }

  public static void setDefaultAdvice(String adviceString) {
    Preconditions.checkArgument(
      EnumUtils.isValidEnum(Advice.class, adviceString),
      "Default advice for mmap buffers must match a posix_madvise compatible option: %s",
      adviceString
    );

    setDefaultAdvice(Advice.valueOf(adviceString).getAdvice());
  }
}
