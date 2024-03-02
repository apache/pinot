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

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.pinot.spi.utils.BytesUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class HashUtilsTest {
  @Test
  public void testHashPlainValues() {
    Assert.assertEquals(BytesUtils.toHexString(HashUtils.hashMD5("hello world".getBytes())),
        "5eb63bbbe01eeed093cb22bb8f5acdc3");
    Assert.assertEquals(BytesUtils.toHexString(HashUtils.hashMurmur3("hello world".getBytes())),
        "0e617feb46603f53b163eb607d4697ab");
  }

  @Test
  public void testHashUUIDv4() {
    testHashUUIDv4(new UUID[]{UUID.randomUUID()});
    testHashUUIDv4(new UUID[]{UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()});
  }

  private void testHashUUIDv4(UUID[] uuids) {
    StringBuilder concatenatedUUID = new StringBuilder();
    for (UUID uuid : uuids) {
      concatenatedUUID.append(uuid);
    }
    byte[] inputBytes = concatenatedUUID.toString().getBytes(StandardCharsets.UTF_8);
    // Ensure test data is valid. Each UUID in string form should be 36 bytes.
    Assert.assertEquals(inputBytes.length, 36 * uuids.length);
    byte[] convertedBytes = HashUtils.hashUUIDv4(inputBytes);
    // After hashing, each UUID should take 16 bytes.
    Assert.assertEquals(convertedBytes.length, 16 * uuids.length);
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
      Assert.assertEquals(reconstructedUUID, uuids[uuidIndex]);
      uuidIndex++;
    }
  }
}
