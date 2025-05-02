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

  //Strings of various lengths to cover all cases of the CityHash functions
  private static final String INPUT_LEN_3 = "abc";
  private static final String INPUT_LEN_7 = "abcdefg";
  private static final String INPUT_LEN_13 = "abcefghijklmn";
  private static final String INPUT_LEN_19 = "abcdefghijklmnopqrs";
  private static final String INPUT_LEN_26 = "abcdefghijklmnopqrstuvwxyz";
  private static final String INPUT_LEN_39 = "abcdefghijklmnopqrstuvwxyzabcefghijklmn";
  private static final String INPUT_LEN_78 =
          "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz";
  private static final String INPUT_LEN_156 = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"
          + "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz";

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

  @Test
  public void testCityHash32() {
    assertEquals(HashFunctions.cityHash32(INPUT_LEN_3.getBytes()), 795041479);
    assertEquals(HashFunctions.cityHash32(INPUT_LEN_7.getBytes()), 568243927);
    assertEquals(HashFunctions.cityHash32(INPUT_LEN_13.getBytes()), -1658103047);
    assertEquals(HashFunctions.cityHash32(INPUT_LEN_26.getBytes()), -1442658879);
  }

  @Test
  public void testCityHash64() {
    assertEquals(HashFunctions.cityHash64(INPUT_LEN_3.getBytes()), 2640714258260161385L);
    assertEquals(HashFunctions.cityHash64(INPUT_LEN_7.getBytes()), 4341691227789030229L);
    assertEquals(HashFunctions.cityHash64(INPUT_LEN_13.getBytes()), 2219442073233419101L);
    assertEquals(HashFunctions.cityHash64(INPUT_LEN_19.getBytes()), -7582989256905268791L);
    assertEquals(HashFunctions.cityHash64(INPUT_LEN_39.getBytes()), 2649092397580160289L);
    assertEquals(HashFunctions.cityHash64(INPUT_LEN_78.getBytes()), 4095282343956748170L);
  }

  @Test
  public void testCityHash64WithSeed() {
    assertEquals(HashFunctions.cityHash64(INPUT_LEN_3.getBytes(), 10), -5393534126371324712L);
    assertEquals(HashFunctions.cityHash64(INPUT_LEN_19.getBytes(), 10), 6644130553114817940L);
    assertEquals(HashFunctions.cityHash64(INPUT_LEN_39.getBytes(), 10), 4996239180729703879L);
    assertEquals(HashFunctions.cityHash64(INPUT_LEN_78.getBytes(), 10), 7728698838656230245L);
  }

  @Test
  public void testCityHash64WithSeeds() {
    assertEquals(HashFunctions.cityHash64(INPUT_LEN_3.getBytes(), 10438437, 399389834),
            1535010434850606895L);
    assertEquals(HashFunctions.cityHash64(INPUT_LEN_19.getBytes(), 10438437, 399389834),
            3720925692744621932L);
    assertEquals(HashFunctions.cityHash64(INPUT_LEN_39.getBytes(), 10438437, 399389834),
            2833639334569022972L);
    assertEquals(HashFunctions.cityHash64(INPUT_LEN_78.getBytes(), 10438437, 399389834),
            8613575199633737553L);
  }

  @Test
  public void testCityHash128() {
    assertEquals(HashFunctions.cityHash128(INPUT_LEN_3.getBytes()),
            new byte[] {57, -128, -78, -81, -46, 18, 108, 4, -96, -123, -16, -112, 19, 2, -98, 69});
    assertEquals(HashFunctions.cityHash128(INPUT_LEN_39.getBytes()),
            new byte[] {0, -44, 40, 93, -71, 96, 80, -45, -82, 111, -109, -116, 25, 32, 98, -7});
    assertEquals(HashFunctions.cityHash128(INPUT_LEN_156.getBytes()),
            new byte[] {114, 8, 23, 53, -124, 83, -28, 35, 27, -117, -59, 121, -108, -20, 59, 115});
  }
}
