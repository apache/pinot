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
package org.apache.pinot.core.data.manager.realtime;

import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentCompletionUtils {
  private SegmentCompletionUtils() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentCompletionUtils.class);
  // Used to create temporary segment file names
  private static final String TMP = ".tmp.";

  /**
   * Takes in a segment name, and returns a file name prefix that is used to store all attempted uploads of this
   * segment when a segment is uploaded using split commit. Each attempt has a unique file name suffix
   * @param segmentName segment name
   * @return
   */
  public static String getTmpSegmentNamePrefix(String segmentName) {
    return segmentName + TMP;
  }

  public static String generateTmpSegmentFileName(String segmentNameStr) {
    return getTmpSegmentNamePrefix(segmentNameStr) + UUID.randomUUID();
  }

  public static boolean isTmpFile(String uri) {
    String[] splits = StringUtils.splitByWholeSeparator(uri, TMP);
    if (splits.length < 2) {
      return false;
    }
    try {
      UUID.fromString(splits[splits.length - 1]);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }
}
