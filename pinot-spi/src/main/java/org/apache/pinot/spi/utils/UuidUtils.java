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

import java.util.Arrays;
import java.util.UUID;


/**
 * Utilities for Pinot's logical UUID type.
 *
 * <p>UUID values are externally represented as canonical lowercase RFC 4122 strings and internally represented as
 * fixed-width 16-byte values.
 */
public final class UuidUtils {
  public static final int UUID_NUM_BYTES = 16;
  private static final byte[] NULL_UUID_BYTES = new byte[UUID_NUM_BYTES];

  private UuidUtils() {
  }

  public static byte[] nullUuidBytes() {
    return Arrays.copyOf(NULL_UUID_BYTES, UUID_NUM_BYTES);
  }

  @Deprecated
  public static byte[] nilUuidBytes() {
    return nullUuidBytes();
  }

  public static byte[] toBytes(long mostSignificantBits, long leastSignificantBits) {
    byte[] uuidBytes = new byte[UUID_NUM_BYTES];
    writeLong(uuidBytes, 0, mostSignificantBits);
    writeLong(uuidBytes, Long.BYTES, leastSignificantBits);
    return uuidBytes;
  }

  public static byte[] toBytes(UUID uuid) {
    validateNotNull(uuid, "UUID bytes");
    return toBytes(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
  }

  public static byte[] toBytes(String uuidString) {
    UUID uuid;
    try {
      uuid = UUID.fromString(uuidString);
    } catch (Exception e) {
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
    validateNotNull(uuidBytes, "UUID bytes");
    validateLength(uuidBytes);
    return Arrays.copyOf(uuidBytes, uuidBytes.length);
  }

  public static byte[] toBytes(ByteArray uuidBytes) {
    validateNotNull(uuidBytes, "UUID bytes");
    return toBytes(uuidBytes.getBytes());
  }

  public static byte[] toBytes(Object value) {
    validateNotNull(value, "UUID bytes");
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
    validateNotNull(uuidBytes, "UUID");
    validateLength(uuidBytes);
    return toUUID(getMostSignificantBitsInternal(uuidBytes), getLeastSignificantBitsInternal(uuidBytes));
  }

  public static UUID toUUID(ByteArray uuidBytes) {
    validateNotNull(uuidBytes, "UUID");
    return toUUID(uuidBytes.getBytes());
  }

  public static UUID toUUID(String uuidString) {
    return toUUID(toBytes(uuidString));
  }

  public static UUID toUUID(long mostSignificantBits, long leastSignificantBits) {
    return new UUID(mostSignificantBits, leastSignificantBits);
  }

  public static UUID toUUID(Object value) {
    validateNotNull(value, "UUID");
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

  /**
   * Immutable UUID key optimized for Pinot execution hot paths.
   *
   * <p>The key stores UUID values as two primitive longs to avoid repeated {@code ByteArray} allocation and
   * byte-by-byte equality/hashing while preserving Pinot's canonical 16-byte representation at the edges.
   */
  public static final class UuidKey implements Comparable<UuidKey> {
    private final long _mostSignificantBits;
    private final long _leastSignificantBits;
    private final int _hashCode;

    private UuidKey(long mostSignificantBits, long leastSignificantBits) {
      _mostSignificantBits = mostSignificantBits;
      _leastSignificantBits = leastSignificantBits;
      _hashCode = UuidUtils.hashCode(mostSignificantBits, leastSignificantBits);
    }

    public static UuidKey fromLongs(long mostSignificantBits, long leastSignificantBits) {
      return new UuidKey(mostSignificantBits, leastSignificantBits);
    }

    public static UuidKey fromBytes(byte[] uuidBytes) {
      validateNotNull(uuidBytes, "UUID");
      validateLength(uuidBytes);
      return fromLongs(getMostSignificantBitsInternal(uuidBytes), getLeastSignificantBitsInternal(uuidBytes));
    }

    public static UuidKey fromByteArray(ByteArray uuidBytes) {
      validateNotNull(uuidBytes, "UUID");
      return fromBytes(uuidBytes.getBytes());
    }

    public static UuidKey fromUUID(UUID uuid) {
      validateNotNull(uuid, "UUID");
      return fromLongs(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    public static UuidKey fromObject(Object value) {
      validateNotNull(value, "UUID");
      if (value instanceof UuidKey) {
        return (UuidKey) value;
      }
      if (value instanceof UUID) {
        return fromUUID((UUID) value);
      }
      if (value instanceof byte[]) {
        return fromBytes((byte[]) value);
      }
      if (value instanceof ByteArray) {
        return fromByteArray((ByteArray) value);
      }
      if (value instanceof CharSequence) {
        return fromUUID(UuidUtils.toUUID(value.toString()));
      }
      throw new IllegalArgumentException(
          "Cannot convert value: '" + value + "' to UUID key, unsupported type: " + value.getClass());
    }

    public long getMostSignificantBits() {
      return _mostSignificantBits;
    }

    public long getLeastSignificantBits() {
      return _leastSignificantBits;
    }

    public byte[] toBytes() {
      return UuidUtils.toBytes(_mostSignificantBits, _leastSignificantBits);
    }

    public ByteArray toByteArray() {
      return new ByteArray(toBytes());
    }

    public UUID toUUID() {
      return UuidUtils.toUUID(_mostSignificantBits, _leastSignificantBits);
    }

    @Override
    public int compareTo(UuidKey other) {
      return UuidUtils.compare(_mostSignificantBits, _leastSignificantBits, other._mostSignificantBits,
          other._leastSignificantBits);
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof UuidKey)) {
        return false;
      }
      UuidKey otherUuidKey = (UuidKey) other;
      return UuidUtils.equals(_mostSignificantBits, _leastSignificantBits, otherUuidKey._mostSignificantBits,
          otherUuidKey._leastSignificantBits);
    }

    @Override
    public int hashCode() {
      return _hashCode;
    }

    @Override
    public String toString() {
      return toUUID().toString();
    }
  }

  public static long getMostSignificantBits(byte[] uuidBytes) {
    validateNotNull(uuidBytes, "UUID");
    validateLength(uuidBytes);
    return getMostSignificantBitsInternal(uuidBytes);
  }

  public static long getLeastSignificantBits(byte[] uuidBytes) {
    validateNotNull(uuidBytes, "UUID");
    validateLength(uuidBytes);
    return getLeastSignificantBitsInternal(uuidBytes);
  }

  public static boolean equals(byte[] left, byte[] right) {
    if (left == right) {
      return true;
    }
    validateNotNull(left, "UUID bytes");
    validateNotNull(right, "UUID bytes");
    validateLength(left);
    validateLength(right);
    return getMostSignificantBitsInternal(left) == getMostSignificantBitsInternal(right)
        && getLeastSignificantBitsInternal(left) == getLeastSignificantBitsInternal(right);
  }

  public static boolean equals(ByteArray left, ByteArray right) {
    if (left == right) {
      return true;
    }
    validateNotNull(left, "UUID bytes");
    validateNotNull(right, "UUID bytes");
    return equals(left.getBytes(), right.getBytes());
  }

  public static boolean equals(long leftMostSignificantBits, long leftLeastSignificantBits,
      long rightMostSignificantBits, long rightLeastSignificantBits) {
    return leftMostSignificantBits == rightMostSignificantBits && leftLeastSignificantBits == rightLeastSignificantBits;
  }

  public static int hashCode(byte[] uuidBytes) {
    validateNotNull(uuidBytes, "UUID");
    validateLength(uuidBytes);
    return hashCode(getMostSignificantBitsInternal(uuidBytes), getLeastSignificantBitsInternal(uuidBytes));
  }

  public static int hashCode(ByteArray uuidBytes) {
    validateNotNull(uuidBytes, "UUID");
    return hashCode(uuidBytes.getBytes());
  }

  public static int hashCode(long mostSignificantBits, long leastSignificantBits) {
    return updateHash(updateHash(1, mostSignificantBits), leastSignificantBits);
  }

  public static int compare(byte[] left, byte[] right) {
    if (left == right) {
      return 0;
    }
    validateNotNull(left, "UUID bytes");
    validateNotNull(right, "UUID bytes");
    validateLength(left);
    validateLength(right);
    return compare(getMostSignificantBitsInternal(left), getLeastSignificantBitsInternal(left),
        getMostSignificantBitsInternal(right), getLeastSignificantBitsInternal(right));
  }

  public static int compare(ByteArray left, ByteArray right) {
    if (left == right) {
      return 0;
    }
    validateNotNull(left, "UUID bytes");
    validateNotNull(right, "UUID bytes");
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
    if (uuidBytes == null) {
      return false;
    }
    try {
      validateLength(uuidBytes);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
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

  private static void validateLength(byte[] uuidBytes) {
    if (uuidBytes.length != UUID_NUM_BYTES) {
      throw new IllegalArgumentException(
          "Invalid UUID byte length: " + uuidBytes.length + ", expected: " + UUID_NUM_BYTES);
    }
  }

  private static long getMostSignificantBitsInternal(byte[] uuidBytes) {
    return readLong(uuidBytes, 0);
  }

  private static long getLeastSignificantBitsInternal(byte[] uuidBytes) {
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

  private static void validateNotNull(Object value, String targetType) {
    if (value == null) {
      throw new IllegalArgumentException("Cannot convert null value to " + targetType);
    }
  }
}
