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
package com.linkedin.pinot.startree.hll;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableBiMap;


/**
 * Utility functions to get hll field size.
 */
public class HllSizeUtils {

  private static final ImmutableBiMap<Integer, Integer> LOG2M_TO_SIZE_IN_BYTES =
      ImmutableBiMap.of(5, 32, 6, 52, 7, 96, 8, 180, 9, 352);

  public static ImmutableBiMap<Integer, Integer> getLog2mToSizeInBytes() {
    return LOG2M_TO_SIZE_IN_BYTES;
  }

  public static int getHllFieldSizeFromLog2m(int log2m) {
    Preconditions.checkArgument(LOG2M_TO_SIZE_IN_BYTES.containsKey(log2m),
        "Log2m: " + log2m + " is not in valid range.");
    return LOG2M_TO_SIZE_IN_BYTES.get(log2m);
  }

  public static int getLog2mFromHllFieldSize(int hllFieldSize) {
    Preconditions.checkArgument(LOG2M_TO_SIZE_IN_BYTES.containsValue(hllFieldSize),
        "HllFieldSize: " + hllFieldSize + " is not in valid range.");
    return LOG2M_TO_SIZE_IN_BYTES.inverse().get(hllFieldSize);
  }
}
