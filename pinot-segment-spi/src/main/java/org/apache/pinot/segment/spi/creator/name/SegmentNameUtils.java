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
package org.apache.pinot.segment.spi.creator.name;

import java.util.regex.Pattern;


/**
 * Utils for segment names.
 */
public class SegmentNameUtils {
  // According to the invalid name pattern below, `|` is safer than `,` as the segment name separator.
  public static final char SEGMENT_NAME_SEPARATOR = '|';
  private static final Pattern INVALID_SEGMENT_NAME_REGEX = Pattern.compile(".*[\\\\/:\\*?\"<>|].*");

  private SegmentNameUtils() {
  }

  /**
   * A handy util to validate if segment name is valid.
   *
   * @param partialOrFullSegmentName provide partial or full segment name
   */
  public static void validatePartialOrFullSegmentName(String partialOrFullSegmentName) {
    if (INVALID_SEGMENT_NAME_REGEX.matcher(partialOrFullSegmentName).matches()) {
      throw new IllegalArgumentException("Invalid partial or full segment name: " + partialOrFullSegmentName);
    }
  }
}
