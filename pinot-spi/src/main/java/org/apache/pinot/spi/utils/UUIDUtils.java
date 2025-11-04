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


/**
 * Utility class for UUID serialization and deserialization.
 * UUIDs are stored as fixed 16-byte arrays (most significant bits + least significant bits).
 */
public class UUIDUtils {
  private UUIDUtils() {
  }

  /**
   * Returns the number of bytes in a serialized UUID (always 16 bytes).
   */
  public static int byteSize() {
    return 16;
  }

  /**
   * Validates if a string is a valid UUID format.
   * @param str the string to validate
   * @return true if the string is a valid UUID, false otherwise
   */
  public static boolean isValidUUID(String str) {
    if (str == null || str.isEmpty()) {
      return false;
    }
    try {
      UUID.fromString(str);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  /**
   * Serializes a UUID string to a 16-byte array.
   * Accepts both standard UUID format (with dashes) and 32-character hex format (without dashes).
   * This is the recommended method for general UUID string conversion.
   *
   * @param uuidString the UUID string in standard format (e.g., "550e8400-e29b-41d4-a716-446655440000")
   *                   or hex format (e.g., "550e8400e29b41d4a716446655440000")
   * @return 16-byte array containing the serialized UUID, or empty array if input is null/empty
   * @throws IllegalArgumentException if the string is not a valid UUID or hex string
   */
  public static byte[] serializeFlexible(String uuidString) {
    if (uuidString == null || uuidString.isEmpty()) {
      return new byte[0];
    }

    // Check if it's a 32-character hex string (UUID without dashes)
    if (uuidString.length() == 32 && uuidString.matches("[0-9A-Fa-f]+")) {
      // Hex format - convert to bytes directly
      return BytesUtils.toBytes(uuidString);
    } else {
      // Standard UUID format - use standard serialization
      return serialize(uuidString);
    }
  }

  /**
   * Serializes a UUID string to a 16-byte array (strict mode).
   * Only accepts standard UUID format with dashes.
   *
   * @param uuidString the UUID string in standard format (e.g., "550e8400-e29b-41d4-a716-446655440000")
   * @return 16-byte array containing the serialized UUID
   * @throws IllegalArgumentException if the string is not a valid UUID
   */
  public static byte[] serialize(String uuidString) {
    if (uuidString == null) {
      throw new IllegalArgumentException("UUID string cannot be null");
    }
    try {
      UUID uuid = UUID.fromString(uuidString);
      return serialize(uuid);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid UUID string: " + uuidString, e);
    }
  }

  /**
   * Serializes a UUID object to a 16-byte array.
   * The format is: 8 bytes for most significant bits + 8 bytes for least significant bits (big-endian).
   * @param uuid the UUID object to serialize
   * @return 16-byte array containing the serialized UUID
   */
  public static byte[] serialize(UUID uuid) {
    if (uuid == null) {
      throw new IllegalArgumentException("UUID cannot be null");
    }
    ByteBuffer buffer = ByteBuffer.wrap(new byte[16]);
    buffer.putLong(uuid.getMostSignificantBits());
    buffer.putLong(uuid.getLeastSignificantBits());
    return buffer.array();
  }

  /**
   * Deserializes a 16-byte array to a UUID object.
   * @param bytes the 16-byte array containing the serialized UUID
   * @return the deserialized UUID object
   * @throws IllegalArgumentException if the byte array is null or not 16 bytes
   */
  public static UUID deserialize(byte[] bytes) {
    if (bytes == null) {
      throw new IllegalArgumentException("Byte array cannot be null");
    }
    if (bytes.length != 16) {
      throw new IllegalArgumentException("UUID byte array must be exactly 16 bytes, got " + bytes.length);
    }
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    long mostSigBits = buffer.getLong();
    long leastSigBits = buffer.getLong();
    return new UUID(mostSigBits, leastSigBits);
  }

  /**
   * Deserializes a 16-byte array to a UUID string in standard format.
   * @param bytes the 16-byte array containing the serialized UUID
   * @return the UUID string in standard format (e.g., "550e8400-e29b-41d4-a716-446655440000")
   * @throws IllegalArgumentException if the byte array is null or not 16 bytes
   */
  public static String deserializeToString(byte[] bytes) {
    return deserialize(bytes).toString();
  }

  /**
   * Converts a ByteArray wrapper to a UUID string.
   * @param byteArray the ByteArray wrapper containing the serialized UUID
   * @return the UUID string in standard format
   */
  public static String toString(ByteArray byteArray) {
    if (byteArray == null) {
      throw new IllegalArgumentException("ByteArray cannot be null");
    }
    return deserializeToString(byteArray.getBytes());
  }

  /**
   * Converts a byte array to a UUID string.
   * This is a convenience method that handles null and invalid lengths gracefully for display purposes.
   * @param bytes the byte array
   * @return the UUID string, or a hex representation if not a valid UUID
   */
  public static String toStringSafe(byte[] bytes) {
    if (bytes == null) {
      return "null";
    }
    if (bytes.length != 16) {
      return BytesUtils.toHexString(bytes);
    }
    try {
      return deserializeToString(bytes);
    } catch (Exception e) {
      return BytesUtils.toHexString(bytes);
    }
  }
}
