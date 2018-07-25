/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.pinot.core.startree.v2;

import java.io.File;


public class StarTreeV2Util {

  public static File findFormatFile(File indexDir, String fileName) {

    // Try to find v3 file first
    File v3Dir = new File(indexDir, "v3");
    File v3File = new File(v3Dir, fileName);
    if (v3File.exists()) {
      return v3File;
    }

    // If cannot find v3 file, try to find v1 file instead
    File v1File = new File(indexDir, fileName);
    if (v1File.exists()) {
      return v1File;
    }

    return null;
  }
}
