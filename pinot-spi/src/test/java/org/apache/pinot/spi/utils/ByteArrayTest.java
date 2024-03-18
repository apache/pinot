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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class ByteArrayTest {
  private static final FALFInterner<byte[]> BYTE_INTERNER = new FALFInterner<>(2, Arrays::hashCode);

  @Test(description = "hash code may have been used for partitioning so must be stable")
  public void testHashCode() {
    // ensure to test below 8
    byte[] bytes = new byte[ThreadLocalRandom.current().nextInt(8)];
    ThreadLocalRandom.current().nextBytes(bytes);
    assertEquals(Arrays.hashCode(bytes), new ByteArray(bytes).hashCode());
    for (int i = 0; i < 10_000; i++) {
      bytes = new byte[ThreadLocalRandom.current().nextInt(2048)];
      ThreadLocalRandom.current().nextBytes(bytes);
      assertEquals(Arrays.hashCode(bytes), new ByteArray(bytes).hashCode());
    }
  }

  @Test(description = "hash code may have been used for partitioning so must be stable")
  public void testHashCodeWithInterning() {
    // ensure to test below 8
    byte[] bytes = new byte[ThreadLocalRandom.current().nextInt(8)];
    ThreadLocalRandom.current().nextBytes(bytes);
    assertEquals(Arrays.hashCode(bytes), new ByteArray(bytes, BYTE_INTERNER).hashCode());
    for (int i = 0; i < 10_000; i++) {
      bytes = new byte[ThreadLocalRandom.current().nextInt(2048)];
      ThreadLocalRandom.current().nextBytes(bytes);
      assertEquals(Arrays.hashCode(bytes), new ByteArray(bytes, BYTE_INTERNER).hashCode());
    }
  }

  @Test
  public void testCompare() {
    byte[] foo = "foo".getBytes(StandardCharsets.UTF_8);
    assertEquals(ByteArray.compare(foo, foo), 0);
    assertEquals(ByteArray.compare(foo, Arrays.copyOf(foo, foo.length)), 0);
    assertTrue(ByteArray.compare(foo, Arrays.copyOf(foo, foo.length - 1)) > 0);
    assertTrue(ByteArray.compare(foo, Arrays.copyOf(foo, foo.length + 1)) < 0);
    byte[] bar = "bar".getBytes(StandardCharsets.UTF_8);
    assertTrue(ByteArray.compare(foo, bar) > 0);
    assertTrue(ByteArray.compare(bar, foo) < 0);
    assertTrue(ByteArray.compare(Arrays.copyOf(bar, bar.length - 1), foo) < 0);
    assertTrue(ByteArray.compare(Arrays.copyOf(bar, bar.length + 1), foo) < 0);
  }

  @Test
  public void testCompareAtOffset() {
    byte[] foo = "00000foo".getBytes(StandardCharsets.UTF_8);
    assertEquals(ByteArray.compare(foo, 5, 8, foo, 5, 8), 0);
    assertEquals(ByteArray.compare(foo, 5, 8, Arrays.copyOf(foo, foo.length), 5, 8), 0);
    assertTrue(ByteArray.compare(foo, 5, 8, Arrays.copyOf(foo, foo.length - 1), 5, 7) > 0);
    assertTrue(ByteArray.compare(foo, 5, 8, Arrays.copyOf(foo, foo.length + 1), 5, 9) < 0);
    byte[] bar = "000bar".getBytes(StandardCharsets.UTF_8);
    assertTrue(ByteArray.compare(foo, 5, 8, bar, 3, 6) > 0);
    assertTrue(ByteArray.compare(bar, 3, 6, foo, 5, 8) < 0);
    assertTrue(ByteArray.compare(Arrays.copyOf(bar, bar.length - 1), 3, 5, foo, 5, 8) < 0);
    assertTrue(ByteArray.compare(Arrays.copyOf(bar, bar.length + 1), 3, 7, foo, 5, 8) < 0);
  }

  @Test(expectedExceptions = ArrayIndexOutOfBoundsException.class)
  public void testCompareOutOfBounds() {
    byte[] foo = "foo".getBytes(StandardCharsets.UTF_8);
    byte[] bar = "bar".getBytes(StandardCharsets.UTF_8);
    ByteArray.compare(foo, 3, 5, bar, 0, 3);
  }
}
