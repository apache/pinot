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


/// Helpers for temporary split-commit upload names.
///
/// New uploads use `{segment}.tmp.{instanceId}` so a same-server HOLD/retry overwrites one object while two replicas
/// keep distinct keys. Older servers wrote `{segment}.tmp.{UUID}`; leftover recognition stays in {@link #isTmpFile}.
public class SegmentCompletionUtils {
  private SegmentCompletionUtils() {
  }

  // Used to create temporary segment file names
  private static final String TMP = ".tmp.";

  /// Takes in a segment name, and returns a file name prefix that is used to store all attempted uploads of this
  /// segment when a segment is uploaded using split commit.
  /// @param segmentName segment name
  /// @return temporary segment file name prefix
  public static String getTmpSegmentNamePrefix(String segmentName) {
    return segmentName + TMP;
  }

  /// Mints a leftover-style UUID temp name. Prefer {@link #generateTmpSegmentFileName(String, String)} for new uploads.
  public static String generateTmpSegmentFileName(String segmentNameStr) {
    return generateTmpSegmentFileName(segmentNameStr, UUID.randomUUID().toString());
  }

  /// Returns `{segment}.tmp.{instanceId}` so retries from one server reuse a single deep-store key.
  public static String generateTmpSegmentFileName(String segmentNameStr, String instanceId) {
    if (StringUtils.isBlank(segmentNameStr)) {
      throw new IllegalArgumentException("segmentName is required");
    }
    if (!isPathSafeTmpSuffix(instanceId)) {
      throw new IllegalArgumentException("instanceId must be a non-empty path-safe identifier: " + instanceId);
    }
    return getTmpSegmentNamePrefix(segmentNameStr) + instanceId;
  }

  public static boolean isTmpFile(String uri) {
    String[] splits = StringUtils.splitByWholeSeparator(uri, TMP);
    if (splits.length < 2) {
      return false;
    }
    // Accept leftover UUID temps and {segment}.tmp.{instanceId}. Reject empty or path-like suffixes.
    return isPathSafeTmpSuffix(splits[splits.length - 1]);
  }

  private static boolean isPathSafeTmpSuffix(String suffix) {
    return StringUtils.isNotBlank(suffix) && !suffix.contains("/") && !suffix.contains("\\");
  }
}
