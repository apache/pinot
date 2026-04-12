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

  public static byte[] toBytes(UUID uuid) {
    validateNotNull(uuid, "UUID bytes");
    ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[UUID_NUM_BYTES]);
    byteBuffer.putLong(uuid.getMostSignificantBits());
    byteBuffer.putLong(uuid.getLeastSignificantBits());
    return byteBuffer.array();
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
    ByteBuffer byteBuffer = ByteBuffer.wrap(uuidBytes);
    return new UUID(byteBuffer.getLong(), byteBuffer.getLong());
  }

  public static UUID toUUID(ByteArray uuidBytes) {
    validateNotNull(uuidBytes, "UUID");
    return toUUID(uuidBytes.getBytes());
  }

  public static UUID toUUID(String uuidString) {
    return toUUID(toBytes(uuidString));
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

  private static void validateNotNull(Object value, String targetType) {
    if (value == null) {
      throw new IllegalArgumentException("Cannot convert null value to " + targetType);
    }
  }
}
