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
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class UUIDUtilsTest {

  @Test
  public void testSerializeDeserializeString() {
    String uuidString = "550e8400-e29b-41d4-a716-446655440000";
    byte[] serialized = UUIDUtils.serialize(uuidString);

    // Verify it's 16 bytes
    assertEquals(serialized.length, 16);

    // Verify round-trip
    String deserialized = UUIDUtils.deserializeToString(serialized);
    assertEquals(deserialized, uuidString);
  }

  @Test
  public void testSerializeDeserializeUUID() {
    UUID uuid = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
    byte[] serialized = UUIDUtils.serialize(uuid);

    // Verify it's 16 bytes
    assertEquals(serialized.length, 16);

    // Verify round-trip
    UUID deserialized = UUIDUtils.deserialize(serialized);
    assertEquals(deserialized, uuid);
  }

  @Test
  public void testSerializeConsistency() {
    String uuidString = "550e8400-e29b-41d4-a716-446655440000";
    UUID uuid = UUID.fromString(uuidString);

    byte[] serializedFromString = UUIDUtils.serialize(uuidString);
    byte[] serializedFromUUID = UUIDUtils.serialize(uuid);

    // Both methods should produce identical results
    assertEquals(serializedFromString, serializedFromUUID);
  }

  @Test
  public void testIsValidUUID() {
    // Valid UUIDs
    assertTrue(UUIDUtils.isValidUUID("550e8400-e29b-41d4-a716-446655440000"));
    assertTrue(UUIDUtils.isValidUUID("00000000-0000-0000-0000-000000000000"));
    assertTrue(UUIDUtils.isValidUUID("ffffffff-ffff-ffff-ffff-ffffffffffff"));

    // Invalid UUIDs
    assertFalse(UUIDUtils.isValidUUID("invalid-uuid"));
    assertFalse(UUIDUtils.isValidUUID("550e8400-e29b-41d4-a716"));
    assertFalse(UUIDUtils.isValidUUID(""));
    assertFalse(UUIDUtils.isValidUUID(null));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testSerializeInvalidString() {
    UUIDUtils.serialize("invalid-uuid");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testDeserializeInvalidLength() {
    byte[] invalidBytes = new byte[10]; // Wrong length
    UUIDUtils.deserialize(invalidBytes);
  }

  @Test
  public void testToStringSafe() {
    // Valid UUID bytes
    String uuidString = "550e8400-e29b-41d4-a716-446655440000";
    byte[] validBytes = UUIDUtils.serialize(uuidString);
    assertEquals(UUIDUtils.toStringSafe(validBytes), uuidString);

    // Invalid bytes (wrong length) - should return hex string
    byte[] invalidBytes = new byte[10];
    String result = UUIDUtils.toStringSafe(invalidBytes);
    assertNotNull(result);
    // Should return hex representation, not throw exception
    assertFalse(result.equals(uuidString));

    // Empty bytes - returns empty hex string
    byte[] emptyBytes = new byte[0];
    String emptyResult = UUIDUtils.toStringSafe(emptyBytes);
    assertNotNull(emptyResult);

    // Null bytes
    assertEquals(UUIDUtils.toStringSafe(null), "null");
  }

  @Test
  public void testZeroUUID() {
    UUID zeroUUID = new UUID(0L, 0L);
    byte[] serialized = UUIDUtils.serialize(zeroUUID);
    assertEquals(serialized.length, 16);

    UUID deserialized = UUIDUtils.deserialize(serialized);
    assertEquals(deserialized, zeroUUID);
    assertEquals(deserialized.toString(), "00000000-0000-0000-0000-000000000000");
  }

  @Test
  public void testMaxUUID() {
    UUID maxUUID = new UUID(-1L, -1L);
    byte[] serialized = UUIDUtils.serialize(maxUUID);
    assertEquals(serialized.length, 16);

    UUID deserialized = UUIDUtils.deserialize(serialized);
    assertEquals(deserialized, maxUUID);
    assertEquals(deserialized.toString(), "ffffffff-ffff-ffff-ffff-ffffffffffff");
  }

  @Test
  public void testMultipleUUIDs() {
    // Test with multiple different UUIDs to ensure no collision
    String[] uuidStrings = {
        "550e8400-e29b-41d4-a716-446655440000",
        "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
        "6ba7b811-9dad-11d1-80b4-00c04fd430c8",
        "00000000-0000-0000-0000-000000000001",
        "ffffffff-ffff-ffff-ffff-fffffffffffe"
    };

    for (String uuidString : uuidStrings) {
      byte[] serialized = UUIDUtils.serialize(uuidString);
      String deserialized = UUIDUtils.deserializeToString(serialized);
      assertEquals(deserialized, uuidString);
    }
  }
}
