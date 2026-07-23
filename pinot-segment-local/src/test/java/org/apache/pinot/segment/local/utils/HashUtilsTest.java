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
package org.apache.pinot.segment.local.utils;

import java.util.UUID;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.PinotMd5Mode;
import org.apache.pinot.spi.utils.UuidUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class HashUtilsTest {
  @Test
  public void testHashPlainValues() {
    assertEquals(BytesUtils.toHexString(HashUtils.hashMD5("hello world".getBytes())),
        "5eb63bbbe01eeed093cb22bb8f5acdc3");
    assertEquals(BytesUtils.toHexString(HashUtils.hashMurmur3("hello world".getBytes())),
        "0e617feb46603f53b163eb607d4697ab");
    assertEquals(BytesUtils.toHexString(HashUtils.hashXXHash("hello world".getBytes())),
        "45ab6734b21e6968");
    assertEquals(BytesUtils.toHexString(HashUtils.hashXXH128("hello world".getBytes())),
        "df8d09e93f874900a99b8775cc15b6c7");
  }

  @Test
  public void testHashUUID() {
    // Test happy cases: when all UUID values are valid
    testHashUUID(new UUID[]{UUID.randomUUID()});
    testHashUUID(new UUID[]{UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()});
    testHashUUIDNormalizedPrimaryKeyValues();

    // Test failure scenario when there's a non-null invalid uuid value
    PrimaryKey invalidUUIDs = new PrimaryKey(new String[]{"some-random-string"});
    byte[] hashResult = HashUtils.hashUUID(invalidUUIDs);
    // In case of failures, each element is prepended with length
    byte[] expectedResult = invalidUUIDs.asBytes();
    assertEquals(hashResult, expectedResult);
    // Test failure scenario when one of the values is null
    try {
      PrimaryKey pKeyWithNull = new PrimaryKey(new String[]{UUID.randomUUID().toString(), null});
      HashUtils.hashUUID(pKeyWithNull);
      fail("Should have thrown an exception");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Found null value"));
    }
  }

  @Test
  public void testHashUUIDNormalizedPrimaryKeyValues() {
    UUID firstUuid = UUID.randomUUID();
    UUID secondUuid = UUID.randomUUID();

    byte[] expectedHash = HashUtils.hashUUID(new PrimaryKey(new Object[]{firstUuid, secondUuid}));
    assertEquals(HashUtils.hashUUID(new PrimaryKey(
        new Object[]{new ByteArray(UuidUtils.toBytes(firstUuid)), new ByteArray(UuidUtils.toBytes(secondUuid))})),
        expectedHash);
    assertEquals(HashUtils.hashUUID(
        new PrimaryKey(new Object[]{UuidUtils.toBytes(firstUuid), UuidUtils.toBytes(secondUuid)})), expectedHash);
  }

  /**
   * Regression: {@link HashUtils#hashUUID} must accept non-canonical-but-Java-parseable UUID strings
   * (e.g. {@code "1-2-3-4-5"}) so existing upsert tables using {@code HashFunction.UUID} keep producing
   * the same hash before and after this PR. {@link UuidUtils#toBytes(String)} is strict and would reject
   * such inputs — for the legacy hash path we route String inputs through {@code UUID.fromString} directly
   * to preserve the lenient pre-PR behavior.
   */
  @Test
  public void testHashUUIDLenientForNonCanonicalStrings() {
    String nonCanonical = "1-2-3-4-5";
    UUID expected = UUID.fromString(nonCanonical);

    byte[] hashResult = HashUtils.hashUUID(new PrimaryKey(new Object[]{nonCanonical}));
    assertEquals(hashResult.length, 16);

    long msb = 0;
    long lsb = 0;
    for (int i = 0; i < 8; i++) {
      msb = (msb << 8) | (hashResult[i] & 0xFF);
    }
    for (int i = 8; i < 16; i++) {
      lsb = (lsb << 8) | (hashResult[i] & 0xFF);
    }
    assertEquals(new UUID(msb, lsb), expected,
        "hashUUID(\"1-2-3-4-5\") must equal UUID.fromString output for backward compatibility");
  }

  /**
   * Multi-column PK regression: each element must independently round-trip through the lenient
   * {@code UUID.fromString} path. A composite PK mixing one non-canonical and one canonical UUID must
   * produce a 32-byte concatenation where each 16-byte half matches its corresponding
   * {@code UUID.fromString} output.
   */
  @Test
  public void testHashUUIDLenientForMultiColumnPrimaryKey() {
    String nonCanonical = "1-2-3-4-5";
    String canonical = "550e8400-e29b-41d4-a716-446655440000";
    UUID expectedFirst = UUID.fromString(nonCanonical);
    UUID expectedSecond = UUID.fromString(canonical);

    byte[] hashResult = HashUtils.hashUUID(new PrimaryKey(new Object[]{nonCanonical, canonical}));
    assertEquals(hashResult.length, 32, "Two-column PK must produce 32 bytes (16 per UUID)");

    assertEquals(reconstructUuid(hashResult, 0), expectedFirst,
        "First half must match UUID.fromString of the non-canonical input");
    assertEquals(reconstructUuid(hashResult, 16), expectedSecond,
        "Second half must match UUID.fromString of the canonical input");
  }

  private static UUID reconstructUuid(byte[] hashBytes, int offset) {
    long msb = 0;
    long lsb = 0;
    for (int i = 0; i < 8; i++) {
      msb = (msb << 8) | (hashBytes[offset + i] & 0xFF);
    }
    for (int i = 0; i < 8; i++) {
      lsb = (lsb << 8) | (hashBytes[offset + 8 + i] & 0xFF);
    }
    return new UUID(msb, lsb);
  }

  @Test
  public void testHashPrimaryKeyWithMd5Disabled() {
    PrimaryKey primaryKey = new PrimaryKey(new Object[]{"hello world"});
    try {
      PinotMd5Mode.setPinotMd5Disabled(true);
      IllegalStateException exception =
          expectThrows(IllegalStateException.class, () -> HashUtils.hashPrimaryKey(primaryKey, HashFunction.MD5));
      assertTrue(exception.getMessage().contains(CommonConstants.CONFIG_OF_PINOT_MD5_DISABLED));
    } finally {
      PinotMd5Mode.setPinotMd5Disabled(false);
    }
  }

  private void testHashUUID(UUID[] uuids) {
    byte[] convertedBytes = HashUtils.hashUUID(new PrimaryKey(uuids));
    // After hashing, each UUID should take 16 bytes.
    assertEquals(convertedBytes.length, 16 * uuids.length);
    // Below we reconstruct each UUID from the reduced 16-byte representation, and ensure it is the same as the input.
    int convertedByteIndex = 0;
    int uuidIndex = 0;
    while (convertedByteIndex < convertedBytes.length) {
      long msb = 0;
      long lsb = 0;
      for (int i = 0; i < 8; i++, convertedByteIndex++) {
        msb = (msb << 8) | (convertedBytes[convertedByteIndex] & 0xFF);
      }
      for (int i = 0; i < 8; i++, convertedByteIndex++) {
        lsb = (lsb << 8) | (convertedBytes[convertedByteIndex] & 0xFF);
      }
      UUID reconstructedUUID = new UUID(msb, lsb);
      assertEquals(reconstructedUUID, uuids[uuidIndex]);
      uuidIndex++;
    }
  }
}
