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
package org.apache.pinot.common.function.scalar;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class HashFunctionsTest {

  @Test
  public void testShaHash() {
    String input = "testString";
    assertEquals(HashFunctions.sha(input.getBytes()), "956265657d0b637ef65b9b59f9f858eecf55ed6a");
  }

  @Test
  public void testSha2224Hash() {
    String input = "testString";
    assertEquals(HashFunctions.sha224(input.getBytes()),
        "bb54d1095764bff72b570dcdc3172ed6d1b26695494528a0059c95ae");
  }

  @Test
  public void testSha256Hash() {
    String input = "testString";
    assertEquals(HashFunctions.sha256(input.getBytes()),
        "4acf0b39d9c4766709a3689f553ac01ab550545ffa4544dfc0b2cea82fba02a3");
  }

  @Test
  public void testSha512Hash() {
    String input = "testString";
    assertEquals(HashFunctions.sha512(input.getBytes()),
        "c48af5a7f6d4a851fc8a434eed638ab1a6ef68e19dbcae894ac67c9fbc5bcb01"
            + "82b8e7123b3df3c9e4dcb7690c23103f03dc17f54352071ceb2a4eb204b26b91");
  }

  @Test
  public void testMd2Hash() {
    String input = "testString";
    assertEquals(HashFunctions.md2(input.getBytes()), "466c453913ba0d8325f96b2d47984fb5");
  }

  @Test
  public void testMd5Hash() {
    String input = "testString";
    assertEquals(HashFunctions.md5(input.getBytes()), "536788f4dbdffeecfbb8f350a941eea3");
  }

  @Test
  public void testMurmurHash2() {
    String input = "testString";
    assertEquals(HashFunctions.murmurHash2(input.getBytes()), -534425817);
  }

  @Test
  public void testMurmurHash2UTF8() {
    String input = "testString";
    assertEquals(HashFunctions.murmurHash2UTF8(input), -534425817);
  }

  @Test
  public void testMurmurHash2Bit64() {
    String input = "testString";
    assertEquals(HashFunctions.murmurHash2Bit64(input.getBytes()), 3907736674355139845L);
    assertEquals(HashFunctions.murmurHash2Bit64(input.getBytes(), 12345), -2138976126980760436L);
  }

  @Test
  public void testMurmurHash3Bit32() {
    String input = "testString";
    assertEquals(HashFunctions.murmurHash3Bit32(input.getBytes(), 0), -1435605585);
  }

  @Test
  public void testMurmurHash3Bit64() {
    String input = "testString";
    assertEquals(HashFunctions.murmurHash3Bit64(input.getBytes(), 0), -3652179990542706350L);
  }

  @Test
  public void testMurmurHash3Bit128() {
    String input = "testString";
    assertEquals(HashFunctions.murmurHash3Bit128(input.getBytes(), 0),
        new byte[]{82, -103, -23, 15, -90, -39, 80, -51, 15, 73, -81, -28, 111, -21, -78, 108});
  }

  @Test
  public void testMurmurHash3X64Bit32() {
    String input = "testString";
    assertEquals(HashFunctions.murmurHash3X64Bit32(input.getBytes(), 0), -1096986291);
  }

  @Test
  public void testMurmurHash3X64Bit64() {
    String input = "testString";
    assertEquals(HashFunctions.murmurHash3X64Bit64(input.getBytes(), 0), -1096986291L);
  }

  @Test
  public void testMurmurHash3X64Bit128() {
    String input = "testString";
    assertEquals(HashFunctions.murmurHash3X64Bit128(input.getBytes(), 0),
        new byte[]{-66, -99, 81, 77, -7, 29, 124, 76, 42, 38, -34, -42, -92, -83, 83, 13});
  }

  @Test
  public void testAdler32() {
    String input = "testString";
    assertEquals(HashFunctions.adler32(input.getBytes()), 392102968);
  }

  @Test
  public void testCrc32() {
    String input = "testString";
    assertEquals(HashFunctions.crc32(input.getBytes()), 418708744);
  }

  @Test
  public void testCrc32c() {
    String input = "testString";
    assertEquals(HashFunctions.crc32c(input.getBytes()), -1608760557);
  }
}
