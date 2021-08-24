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
package org.apache.pinot.controller.util;

import java.util.UUID;
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
  public static String getSegmentNamePrefix(String segmentName) {
    return segmentName + TMP;
  }

  public static String generateSegmentFileName(String segmentNameStr) {
    return getSegmentNamePrefix(segmentNameStr) + UUID.randomUUID().toString();
  }
}
