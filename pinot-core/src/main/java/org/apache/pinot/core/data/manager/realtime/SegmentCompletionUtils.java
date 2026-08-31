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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;


public class SegmentCompletionUtils {
  private SegmentCompletionUtils() {
  }

  // Used to create temporary segment file names
  private static final String TMP = ".tmp.";

  /// Takes in a segment name, and returns the file name prefix used for temporary split-commit upload generations.
  /// @param segmentName segment name
  /// @return temporary segment file name prefix
  public static String getTmpSegmentNamePrefix(String segmentName) {
    return segmentName + TMP;
  }

  public static String generateTmpSegmentFileName(String segmentNameStr) {
    return generateTmpSegmentFileName(segmentNameStr, UUID.randomUUID());
  }

  public static String generateTmpSegmentFileName(String segmentNameStr, UUID segmentBuildId) {
    return getTmpSegmentNamePrefix(segmentNameStr) + segmentBuildId.toString();
  }

  /// Returns a stable upload ID for a producer, segment, and logical segment version.
  public static UUID generateUploadId(String producerId, String segmentName, String segmentVersion) {
    byte[] producerIdBytes = producerId.getBytes(StandardCharsets.UTF_8);
    byte[] segmentNameBytes = segmentName.getBytes(StandardCharsets.UTF_8);
    byte[] segmentVersionBytes = segmentVersion.getBytes(StandardCharsets.UTF_8);
    ByteBuffer identity = ByteBuffer.allocate(Integer.BYTES * 3 + producerIdBytes.length + segmentNameBytes.length
        + segmentVersionBytes.length);
    identity.putInt(producerIdBytes.length).put(producerIdBytes);
    identity.putInt(segmentNameBytes.length).put(segmentNameBytes);
    identity.putInt(segmentVersionBytes.length).put(segmentVersionBytes);
    byte[] digest;
    try {
      digest = MessageDigest.getInstance("SHA-256").digest(identity.array());
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError("SHA-256 must be available", e);
    }
    // RFC 9562 UUID version 8 reserves this layout for application-defined names. Keep the RFC 4122 variant bits.
    digest[6] = (byte) ((digest[6] & 0x0f) | 0x80);
    digest[8] = (byte) ((digest[8] & 0x3f) | 0x80);
    ByteBuffer uuidBytes = ByteBuffer.wrap(digest);
    return new UUID(uuidBytes.getLong(), uuidBytes.getLong());
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
