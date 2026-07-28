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

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;


/// Utilities for Pinot's logical UUID type.
///
/// UUID values are externally represented as canonical lowercase RFC 4122 strings and internally represented as
/// fixed-width 16-byte values. Methods here are invoked on a per-value basis on hot paths, so callers must not pass
/// `null` values (there are no defensive null checks).
public class UuidUtils {
  private UuidUtils() {
  }

  public static final int UUID_NUM_BYTES = 16;

  // Gregorian-to-Unix offset in 100-nanosecond units. Matches RFC 4122 / RFC 9562.
  // This is the count of 100-ns intervals between 1582-10-15T00:00:00Z and 1970-01-01T00:00:00Z.
  private static final long GREGORIAN_TO_UNIX_OFFSET_100NS = 0x01b21dd213814000L;

  public static byte[] nullUuidBytes() {
    return new byte[UUID_NUM_BYTES];
  }

  public static byte[] toBytes(long mostSignificantBits, long leastSignificantBits) {
    byte[] uuidBytes = new byte[UUID_NUM_BYTES];
    writeLong(uuidBytes, 0, mostSignificantBits);
    writeLong(uuidBytes, Long.BYTES, leastSignificantBits);
    return uuidBytes;
  }

  public static byte[] toBytes(UUID uuid) {
    return toBytes(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
  }

  public static byte[] toBytes(String uuidString) {
    UUID uuid;
    try {
      uuid = UUID.fromString(uuidString);
    } catch (Exception e) {
      try {
        // Try parsing the string as hex-encoded bytes
        byte[] bytes = BytesUtils.toBytes(uuidString);
        if (bytes.length == UUID_NUM_BYTES) {
          return bytes;
        }
      } catch (Exception ignore) {
      }
      throw new IllegalArgumentException("Invalid UUID value: '" + uuidString + "'", e);
    }
    String canonical = uuid.toString();
    if (!canonical.equalsIgnoreCase(uuidString)) {
      throw new IllegalArgumentException(
          "Invalid UUID value: '" + uuidString + "'. Expected RFC 4122 format: " + canonical);
    }
    return toBytes(uuid);
  }

  public static byte[] toBytes(byte[] uuidBytes) {
    // Caller owns the buffer; validate the width and return it as-is (no defensive copy).
    validateLength(uuidBytes);
    return uuidBytes;
  }

  public static byte[] toBytes(ByteArray uuidBytes) {
    return toBytes(uuidBytes.getBytes());
  }

  public static byte[] toBytes(Object value) {
    if (value instanceof UUID) {
      return toBytes((UUID) value);
    }
    if (value instanceof byte[]) {
      return toBytes((byte[]) value);
    }
    if (value instanceof ByteArray) {
      return toBytes((ByteArray) value);
    }
    if (value instanceof CharSequence) {
      return toBytes(value.toString());
    }
    throw new IllegalArgumentException(
        "Cannot convert value: '" + value + "' to UUID bytes, unsupported type: " + value.getClass());
  }

  public static UUID toUUID(byte[] uuidBytes) {
    validateLength(uuidBytes);
    return toUUID(getMostSignificantBitsInternal(uuidBytes), getLeastSignificantBitsInternal(uuidBytes));
  }

  /// Alias of [#toUUID(byte[])], retained for binary compatibility with existing callers.
  public static UUID fromBytes(byte[] uuidBytes) {
    return toUUID(uuidBytes);
  }

  public static UUID toUUID(ByteArray uuidBytes) {
    return toUUID(uuidBytes.getBytes());
  }

  public static UUID toUUID(String uuidString) {
    return toUUID(toBytes(uuidString));
  }

  public static UUID toUUID(long mostSignificantBits, long leastSignificantBits) {
    return new UUID(mostSignificantBits, leastSignificantBits);
  }

  public static UUID toUUID(Object value) {
    if (value instanceof UUID) {
      return (UUID) value;
    }
    if (value instanceof byte[]) {
      return toUUID((byte[]) value);
    }
    if (value instanceof ByteArray) {
      return toUUID((ByteArray) value);
    }
    if (value instanceof CharSequence) {
      return toUUID(value.toString());
    }
    throw new IllegalArgumentException(
        "Cannot convert value: '" + value + "' to UUID, unsupported type: " + value.getClass());
  }

  public static String toString(byte[] uuidBytes) {
    return toUUID(uuidBytes).toString();
  }

  public static String toString(ByteArray uuidBytes) {
    return toString(uuidBytes.getBytes());
  }

  public static String toString(UUID uuid) {
    return uuid.toString();
  }

  public static String toString(long mostSignificantBits, long leastSignificantBits) {
    return toUUID(mostSignificantBits, leastSignificantBits).toString();
  }

  public static long getMostSignificantBits(byte[] uuidBytes) {
    validateLength(uuidBytes);
    return getMostSignificantBitsInternal(uuidBytes);
  }

  public static long getLeastSignificantBits(byte[] uuidBytes) {
    validateLength(uuidBytes);
    return getLeastSignificantBitsInternal(uuidBytes);
  }

  public static boolean equals(byte[] left, byte[] right) {
    if (left == right) {
      return true;
    }
    validateLength(left);
    validateLength(right);
    return getMostSignificantBitsInternal(left) == getMostSignificantBitsInternal(right)
        && getLeastSignificantBitsInternal(left) == getLeastSignificantBitsInternal(right);
  }

  public static boolean equals(ByteArray left, ByteArray right) {
    if (left == right) {
      return true;
    }
    return equals(left.getBytes(), right.getBytes());
  }

  public static boolean equals(long leftMostSignificantBits, long leftLeastSignificantBits,
      long rightMostSignificantBits, long rightLeastSignificantBits) {
    return leftMostSignificantBits == rightMostSignificantBits && leftLeastSignificantBits == rightLeastSignificantBits;
  }

  public static int hashCode(byte[] uuidBytes) {
    validateLength(uuidBytes);
    return hashCode(getMostSignificantBitsInternal(uuidBytes), getLeastSignificantBitsInternal(uuidBytes));
  }

  public static int hashCode(ByteArray uuidBytes) {
    return hashCode(uuidBytes.getBytes());
  }

  public static int hashCode(long mostSignificantBits, long leastSignificantBits) {
    return updateHash(updateHash(1, mostSignificantBits), leastSignificantBits);
  }

  public static int compare(byte[] left, byte[] right) {
    if (left == right) {
      return 0;
    }
    validateLength(left);
    validateLength(right);
    return compare(getMostSignificantBitsInternal(left), getLeastSignificantBitsInternal(left),
        getMostSignificantBitsInternal(right), getLeastSignificantBitsInternal(right));
  }

  public static int compare(ByteArray left, ByteArray right) {
    if (left == right) {
      return 0;
    }
    return compare(left.getBytes(), right.getBytes());
  }

  public static int compare(long leftMostSignificantBits, long leftLeastSignificantBits, long rightMostSignificantBits,
      long rightLeastSignificantBits) {
    int mostSignificantBitsComparison = Long.compareUnsigned(leftMostSignificantBits, rightMostSignificantBits);
    if (mostSignificantBitsComparison != 0) {
      return mostSignificantBitsComparison;
    }
    return Long.compareUnsigned(leftLeastSignificantBits, rightLeastSignificantBits);
  }

  public static boolean isUuid(String uuidString) {
    if (uuidString == null) {
      return false;
    }
    try {
      toBytes(uuidString);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  public static boolean isUuid(byte[] uuidBytes) {
    return uuidBytes != null && uuidBytes.length == UUID_NUM_BYTES;
  }

  public static boolean isUuid(ByteArray uuidBytes) {
    return uuidBytes != null && isUuid(uuidBytes.getBytes());
  }

  public static boolean isUuid(Object value) {
    if (value == null) {
      return false;
    }
    if (value instanceof UUID) {
      return true;
    }
    if (value instanceof byte[]) {
      return isUuid((byte[]) value);
    }
    if (value instanceof ByteArray) {
      return isUuid((ByteArray) value);
    }
    if (value instanceof CharSequence) {
      return isUuid(value.toString());
    }
    return false;
  }

  /// Returns a random RFC 4122 version-4 UUID. Equivalent to [UUID#randomUUID()] and uses the same
  /// cryptographically-strong source of randomness.
  public static UUID randomV4() {
    return UUID.randomUUID();
  }

  /// Returns a random RFC 9562 version-7 UUID. The leading 48 bits encode the current Unix time in milliseconds
  /// (big-endian), making v7 UUIDs k-sortable and friendly to time-ordered storage. Within a single millisecond,
  /// 74 bits of randomness disambiguate concurrent generations; this implementation does not guarantee strict
  /// monotonic ordering within the same millisecond.
  public static UUID randomV7() {
    long unixMillis = System.currentTimeMillis() & 0xFFFFFFFFFFFFL;
    ThreadLocalRandom random = ThreadLocalRandom.current();
    // MSB layout: [unix_ts_ms (48)] [ver=0b0111 (4)] [rand_a (12)]
    long msb = (unixMillis << 16) | 0x7000L | (random.nextLong() & 0x0FFFL);
    // LSB layout: [var=0b10 (2)] [rand_b (62)]
    long lsb = 0x8000000000000000L | (random.nextLong() & 0x3FFFFFFFFFFFFFFFL);
    return new UUID(msb, lsb);
  }

  /// Returns the 4-bit version field of the UUID (0-15). Common values: 1 (Gregorian time-based), 3 (MD5
  /// name-based), 4 (random), 5 (SHA-1 name-based), 6 (reordered Gregorian time-based), 7 (Unix time-based),
  /// 8 (custom). The nil UUID returns 0.
  public static int getVersion(UUID uuid) {
    return (int) ((uuid.getMostSignificantBits() >>> 12) & 0xFL);
  }

  public static int getVersion(byte[] uuidBytes) {
    validateLength(uuidBytes);
    return (int) ((getMostSignificantBitsInternal(uuidBytes) >>> 12) & 0xFL);
  }

  /// Extracts the embedded timestamp from a time-based UUID (versions 1, 6, or 7) and returns it as Unix epoch
  /// milliseconds.
  ///
  /// @throws IllegalArgumentException if the UUID is not time-based (version 1, 6, or 7).
  public static long getTimestampMillis(UUID uuid) {
    return getTimestampMillisInternal(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
  }

  public static long getTimestampMillis(byte[] uuidBytes) {
    validateLength(uuidBytes);
    return getTimestampMillisInternal(getMostSignificantBitsInternal(uuidBytes),
        getLeastSignificantBitsInternal(uuidBytes));
  }

  private static long getTimestampMillisInternal(long msb, long lsb) {
    int version = (int) ((msb >>> 12) & 0xFL);
    switch (version) {
      case 7:
        // Upper 48 bits of MSB are the Unix milliseconds, big-endian.
        return msb >>> 16;
      case 6: {
        // RFC 9562 v6 layout: [time_high (32) | time_mid (16) | ver (4) | time_low (12)] in MSB.
        long gregorian100Ns = ((msb >>> 32) << 28) | (((msb >>> 16) & 0xFFFFL) << 12) | (msb & 0x0FFFL);
        return (gregorian100Ns - GREGORIAN_TO_UNIX_OFFSET_100NS) / 10_000L;
      }
      case 1: {
        // RFC 4122 v1 layout: [time_low (32) | time_mid (16) | ver+time_high (16)] in MSB.
        long gregorian100Ns =
            (msb >>> 32) | (((msb >>> 16) & 0xFFFFL) << 32) | ((msb & 0x0FFFL) << 48);
        return (gregorian100Ns - GREGORIAN_TO_UNIX_OFFSET_100NS) / 10_000L;
      }
      default:
        throw new IllegalArgumentException("UUID version " + version + " is not time-based; only versions 1, 6, "
            + "and 7 carry an embedded timestamp");
    }
  }

  private static void validateLength(byte[] uuidBytes) {
    if (uuidBytes.length != UUID_NUM_BYTES) {
      throw new IllegalArgumentException(
          "Invalid UUID byte length: " + uuidBytes.length + ", expected: " + UUID_NUM_BYTES);
    }
  }

  static long getMostSignificantBitsInternal(byte[] uuidBytes) {
    return readLong(uuidBytes, 0);
  }

  static long getLeastSignificantBitsInternal(byte[] uuidBytes) {
    return readLong(uuidBytes, Long.BYTES);
  }

  private static int updateHash(int hashCode, long value) {
    for (int shift = Long.SIZE - Byte.SIZE; shift >= 0; shift -= Byte.SIZE) {
      hashCode = 31 * hashCode + (byte) (value >>> shift);
    }
    return hashCode;
  }

  private static long readLong(byte[] bytes, int offset) {
    return ((long) bytes[offset] & 0xFF) << 56
        | ((long) bytes[offset + 1] & 0xFF) << 48
        | ((long) bytes[offset + 2] & 0xFF) << 40
        | ((long) bytes[offset + 3] & 0xFF) << 32
        | ((long) bytes[offset + 4] & 0xFF) << 24
        | ((long) bytes[offset + 5] & 0xFF) << 16
        | ((long) bytes[offset + 6] & 0xFF) << 8
        | (long) bytes[offset + 7] & 0xFF;
  }

  private static void writeLong(byte[] bytes, int offset, long value) {
    bytes[offset] = (byte) (value >>> 56);
    bytes[offset + 1] = (byte) (value >>> 48);
    bytes[offset + 2] = (byte) (value >>> 40);
    bytes[offset + 3] = (byte) (value >>> 32);
    bytes[offset + 4] = (byte) (value >>> 24);
    bytes[offset + 5] = (byte) (value >>> 16);
    bytes[offset + 6] = (byte) (value >>> 8);
    bytes[offset + 7] = (byte) value;
  }
}
