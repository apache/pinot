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
package org.apache.pinot.spi.utils.hash;

import org.testng.annotations.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;


public class FnvHashFunctionsTest {

  @Test
  public void testFnv132GoldenVectors() {
    assertEquals(FnvHashFunctions.fnv1Hash32("".getBytes(UTF_8)), 0x811c9dc5);
    assertEquals(FnvHashFunctions.fnv1Hash32("a".getBytes(UTF_8)), 0x050c5d7e);
    assertEquals(FnvHashFunctions.fnv1Hash32("ab".getBytes(UTF_8)), 0x70772d38);
    assertEquals(FnvHashFunctions.fnv1Hash32("abc".getBytes(UTF_8)), 0x439c2f4b);
  }

  @Test
  public void testFnv1a32GoldenVectors() {
    assertEquals(FnvHashFunctions.fnv1aHash32("".getBytes(UTF_8)), 0x811c9dc5);
    assertEquals(FnvHashFunctions.fnv1aHash32("a".getBytes(UTF_8)), 0xe40c292c);
    assertEquals(FnvHashFunctions.fnv1aHash32("ab".getBytes(UTF_8)), 0x4d2505ca);
    assertEquals(FnvHashFunctions.fnv1aHash32("abc".getBytes(UTF_8)), 0x1a47e90b);
  }

  @Test
  public void testFnv164GoldenVectors() {
    assertEquals(FnvHashFunctions.fnv1Hash64("".getBytes(UTF_8)), 0xcbf29ce484222325L);
    assertEquals(FnvHashFunctions.fnv1Hash64("a".getBytes(UTF_8)), 0xaf63bd4c8601b7beL);
    assertEquals(FnvHashFunctions.fnv1Hash64("ab".getBytes(UTF_8)), 0x08326707b4eb37b8L);
    assertEquals(FnvHashFunctions.fnv1Hash64("abc".getBytes(UTF_8)), 0xd8dcca186bafadcbL);
  }

  @Test
  public void testFnv1a64GoldenVectors() {
    assertEquals(FnvHashFunctions.fnv1aHash64("".getBytes(UTF_8)), 0xcbf29ce484222325L);
    assertEquals(FnvHashFunctions.fnv1aHash64("a".getBytes(UTF_8)), 0xaf63dc4c8601ec8cL);
    assertEquals(FnvHashFunctions.fnv1aHash64("ab".getBytes(UTF_8)), 0x089c4407b545986aL);
    assertEquals(FnvHashFunctions.fnv1aHash64("abc".getBytes(UTF_8)), 0xe71fa2190541574bL);
  }

  @Test
  public void testVariantParsingIsCaseInsensitive() {
    assertEquals(FnvHashFunctions.Variant.fromString("fnv1_32"), FnvHashFunctions.Variant.FNV1_32);
    assertEquals(FnvHashFunctions.Variant.fromString("FnV1A_32"), FnvHashFunctions.Variant.FNV1A_32);
    assertEquals(FnvHashFunctions.Variant.fromString("FNV1_64"), FnvHashFunctions.Variant.FNV1_64);
    assertEquals(FnvHashFunctions.Variant.fromString("fnv1a_64"), FnvHashFunctions.Variant.FNV1A_64);
  }

  @Test
  public void testVariantParsingTrimsWhitespace() {
    assertEquals(FnvHashFunctions.Variant.fromString(" fnv1_32 "), FnvHashFunctions.Variant.FNV1_32);
    assertEquals(FnvHashFunctions.Variant.fromString("\tfnv1a_64\n"), FnvHashFunctions.Variant.FNV1A_64);
  }

  @Test
  public void testVariantParsingRejectsNullAndInvalidValues() {
    IllegalArgumentException nullException =
        expectThrows(IllegalArgumentException.class, () -> FnvHashFunctions.Variant.fromString(null));
    assertEquals(nullException.getMessage(),
        "FNV variant must be one of fnv1_32, fnv1a_32, fnv1_64 or fnv1a_64, but was: null");

    IllegalArgumentException invalidException =
        expectThrows(IllegalArgumentException.class, () -> FnvHashFunctions.Variant.fromString("fnv2_32"));
    assertEquals(invalidException.getMessage(),
        "FNV variant must be one of fnv1_32, fnv1a_32, fnv1_64 or fnv1a_64, but was: 'fnv2_32'");
  }
}
