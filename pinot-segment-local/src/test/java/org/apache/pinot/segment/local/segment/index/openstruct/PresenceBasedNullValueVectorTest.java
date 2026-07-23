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
package org.apache.pinot.segment.local.segment.index.openstruct;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class PresenceBasedNullValueVectorTest {

  @Test
  public void testIsNullReturnsInverseOfPresence() {
    MutableRoaringBitmap presence = new MutableRoaringBitmap();
    presence.add(0);
    presence.add(3);
    presence.add(7);
    PresenceBasedNullValueVector vec = new PresenceBasedNullValueVector(presence, 10);

    assertFalse(vec.isNull(0));
    assertTrue(vec.isNull(1));
    assertTrue(vec.isNull(2));
    assertFalse(vec.isNull(3));
    assertTrue(vec.isNull(4));
    assertFalse(vec.isNull(7));
    assertTrue(vec.isNull(9));
  }

  @Test
  public void testGetNullBitmapComputesComplement() {
    MutableRoaringBitmap presence = new MutableRoaringBitmap();
    presence.add(1);
    presence.add(4);
    PresenceBasedNullValueVector vec = new PresenceBasedNullValueVector(presence, 6);

    ImmutableRoaringBitmap nullBitmap = vec.getNullBitmap();
    // Null docs: 0, 2, 3, 5 (complement of {1, 4} in [0, 6))
    assertEquals(nullBitmap.getCardinality(), 4);
    assertTrue(nullBitmap.contains(0));
    assertFalse(nullBitmap.contains(1));
    assertTrue(nullBitmap.contains(2));
    assertTrue(nullBitmap.contains(3));
    assertFalse(nullBitmap.contains(4));
    assertTrue(nullBitmap.contains(5));
  }

  @Test
  public void testEmptyPresenceBitmapAllDocsNull() {
    MutableRoaringBitmap presence = new MutableRoaringBitmap();
    PresenceBasedNullValueVector vec = new PresenceBasedNullValueVector(presence, 5);

    for (int i = 0; i < 5; i++) {
      assertTrue(vec.isNull(i));
    }
    assertEquals(vec.getNullBitmap().getCardinality(), 5);
  }

  @Test
  public void testFullPresenceBitmapNoDocsNull() {
    MutableRoaringBitmap presence = new MutableRoaringBitmap();
    presence.add(0L, 8);
    PresenceBasedNullValueVector vec = new PresenceBasedNullValueVector(presence, 8);

    for (int i = 0; i < 8; i++) {
      assertFalse(vec.isNull(i));
    }
    assertEquals(vec.getNullBitmap().getCardinality(), 0);
  }

  @Test
  public void testZeroDocs() {
    PresenceBasedNullValueVector vec = new PresenceBasedNullValueVector(new MutableRoaringBitmap(), 0);
    assertEquals(vec.getNullBitmap().getCardinality(), 0);
  }
}
