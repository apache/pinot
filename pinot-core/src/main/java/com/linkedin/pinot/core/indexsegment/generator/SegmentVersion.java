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
package com.linkedin.pinot.core.indexsegment.generator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public enum SegmentVersion {


  v1 (1),
  v2 (2),//Changed the forward index format to use bitpacking library instead of custombitset format.

  // v3 supports writing all the indexes in a single file
  v3 (3);

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentVersion.class);

  public static SegmentVersion DEFAULT_TABLE_VERSION = SegmentVersion.v3;
  public static SegmentVersion DEFAULT_SERVER_VERSION = SegmentVersion.v3;

  int versionNumber;
  SegmentVersion(int versionNum) {
    this.versionNumber = versionNum;
  }

  /**
   * Compares two segment versions
   * @return returns &lt; 0 if lhs &lt; rhs, 0 if lhs == rhs and  &gt; 0 if lhs &gt; rhs
   */
  public static int compare(SegmentVersion lhs, SegmentVersion rhs) {
    if (lhs.versionNumber == rhs.versionNumber) {
      return 0;
    }
    return (lhs.versionNumber < rhs.versionNumber) ? -1 : 1;
  }

  /**
   * Get the segment format version from string or return the default value
   * @param inputVersion input segment format version to read
   * @param defaultVal default value to use
   * @return SegmentVersion for inputVersion of defaultVal if inputVersion is empty or bad value
   */
  public static SegmentVersion fromString(String inputVersion, SegmentVersion defaultVal) {
    if (inputVersion == null) {
      return defaultVal;
    }
    try {
      return SegmentVersion.valueOf(inputVersion);
    } catch (IllegalArgumentException e) {
      LOGGER.error("Invalid argument for segment version input: {}, Returning default value: {}",
          inputVersion, defaultVal);
      return defaultVal;
    }
  }
}
