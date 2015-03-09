/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.query.aggregation.groupby;

public class BitHacks {
  private static int[] LogTable256 = new int[256];
  static {
    LogTable256[1] = 0;
    for (int i = 2; i < 256; i++) {
      LogTable256[i] = 1 + LogTable256[i / 2];
    }
    LogTable256[0] = -1;
  }

  public static int findLogBase2(int v) {
    int r;
    int tt;
    if ((tt = v >>> 24) > 0) {
      r = 24 + LogTable256[tt];
    } else if ((tt = v >>> 16) > 0) {
      r = 16 + LogTable256[tt];
    } else if ((tt = v >>> 8) > 0) {
      r = 8 + LogTable256[tt];
    } else {
      r = LogTable256[v];
    }
    return r;
  }

}
