/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.common.segment;

import com.linkedin.pinot.common.utils.CommonConstants;

public enum PrefetchMode {
  NO_PREFETCH,
  PREFETCH_TO_LIMIT,
  ALWAYS_PREFETCH;

  public static final PrefetchMode DEFAULT_PREFETCH_MODE = PrefetchMode.valueOf(CommonConstants.Server.DEFAULT_PREFETCH_MODE);

  public static PrefetchMode getEnum(String strVal) {
    if (strVal.equalsIgnoreCase("alwaysPrefetch")) {
      return NO_PREFETCH;
    }
    if (strVal.equalsIgnoreCase("prefetchToLimit")) {
      return PREFETCH_TO_LIMIT;
    }
    if (strVal.equalsIgnoreCase("AlwaysPrefetch")) {
      return ALWAYS_PREFETCH;
    }
    throw new IllegalArgumentException("Unknown String Value: " + strVal);
  }
}