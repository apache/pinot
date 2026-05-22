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
package org.apache.pinot.spi.utils;

import java.nio.ByteBuffer;
import java.util.UUID;


/// Utilities for the 16-byte big-endian binary form of [UUID] (RFC 4122). Bytes 0..7 are the
/// most-significant 64 bits, bytes 8..15 are the least-significant 64 bits.
public class UuidUtils {
  private UuidUtils() {
  }

  /// Converts `uuid` to its 16-byte big-endian RFC 4122 form.
  public static byte[] toBytes(UUID uuid) {
    return ByteBuffer.allocate(16)
        .putLong(uuid.getMostSignificantBits())
        .putLong(uuid.getLeastSignificantBits())
        .array();
  }

  /// Converts a 16-byte big-endian RFC 4122 form back to [UUID]. Throws [IllegalArgumentException] if
  /// `bytes` is not exactly 16 bytes long.
  public static UUID fromBytes(byte[] bytes) {
    if (bytes.length != 16) {
      throw new IllegalArgumentException(
          "Cannot convert byte[" + bytes.length + "] to UUID; expected length 16");
    }
    ByteBuffer buf = ByteBuffer.wrap(bytes);
    return new UUID(buf.getLong(), buf.getLong());
  }
}
