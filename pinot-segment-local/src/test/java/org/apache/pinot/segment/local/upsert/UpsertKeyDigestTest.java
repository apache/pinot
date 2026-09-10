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
package org.apache.pinot.segment.local.upsert;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.utils.ByteArray;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class UpsertKeyDigestTest {
  private static final Object KEY_1 = new PrimaryKey(new Object[]{"k1"});
  private static final Object KEY_2 = new ByteArray(new byte[]{1, 2, 3, 4});
  private static final Object KEY_3 = new PrimaryKey(new Object[]{7, "x"});

  @Test
  public void testOrderIndependentAndReversible() {
    UpsertKeyDigest first = new UpsertKeyDigest();
    UpsertKeyDigest second = new UpsertKeyDigest();
    first.add(KEY_1, 100, false);
    first.add(KEY_2, 200L, false);
    first.add(KEY_3, "cv", false);
    second.add(KEY_3, "cv", false);
    second.add(KEY_1, 100, false);
    second.add(KEY_2, 200L, false);
    assertTrue(first.freeze().sameEntries(second.freeze()));
    assertEquals(first.freeze().entries(), 3);
    assertNotEquals(first.freeze().total(), 0);

    // A missed update shows, and applying it in either shape converges again.
    first.update(KEY_1, 100, false, 150, false);
    assertFalse(first.freeze().sameEntries(second.freeze()));
    second.remove(KEY_1, 100, false);
    second.add(KEY_1, 150, false);
    assertTrue(first.freeze().sameEntries(second.freeze()));

    // Removing everything returns to the empty digest.
    first.remove(KEY_1, 150, false);
    first.remove(KEY_2, 200L, false);
    first.remove(KEY_3, "cv", false);
    UpsertKeyDigest.Mark empty = first.freeze();
    assertEquals(empty.total(), 0);
    assertEquals(empty.entries(), 0);
    assertTrue(empty.sameEntries(new UpsertKeyDigest().freeze()));
  }

  @Test
  public void testTombstonesContributeNothing() {
    UpsertKeyDigest digest = new UpsertKeyDigest();
    digest.add(KEY_1, 100, true);
    assertEquals(digest.freeze().total(), 0);
    assertEquals(digest.freeze().entries(), 0);

    // A delete applied over a live entry equals removing it; a live record over a tombstone equals adding it.
    UpsertKeyDigest deleted = new UpsertKeyDigest();
    deleted.add(KEY_2, 5, false);
    deleted.update(KEY_2, 5, false, 6, true);
    assertTrue(deleted.freeze().sameEntries(digest.freeze()));
    deleted.update(KEY_2, 6, true, 7, false);
    UpsertKeyDigest added = new UpsertKeyDigest();
    added.add(KEY_2, 7, false);
    assertTrue(deleted.freeze().sameEntries(added.freeze()));
    assertEquals(deleted.freeze().entries(), 1);
  }

  @Test
  public void testComparisonValueTypesAreDistinct() {
    Set<Long> hashes = new HashSet<>();
    long keyHash = UpsertKeyDigest.hashKey(KEY_1);
    Comparable[] values = new Comparable[]{
        1, 1L, 1.0f, 1.0d, "1", new BigDecimal("1"), new ByteArray(new byte[]{1}),
        new ComparisonColumns(new Comparable[]{1, null}, 0), new ComparisonColumns(new Comparable[]{null, 1}, 1),
        new ComparisonColumns(new Comparable[]{1}, 0)
    };
    for (Comparable value : values) {
      assertTrue(hashes.add(UpsertKeyDigest.entryHash(keyHash, value)), "collision for " + value);
    }
    assertTrue(hashes.add(UpsertKeyDigest.entryHash(keyHash, null)));
    // Same value, different key.
    assertNotEquals(UpsertKeyDigest.entryHash(UpsertKeyDigest.hashKey(KEY_3), 1),
        UpsertKeyDigest.entryHash(keyHash, 1));
    // Deterministic for equal inputs built separately.
    assertEquals(UpsertKeyDigest.hashKey(new PrimaryKey(new Object[]{"k1"})), keyHash);
    assertEquals(UpsertKeyDigest.entryHash(keyHash, new ComparisonColumns(new Comparable[]{1, null}, 0)),
        UpsertKeyDigest.entryHash(keyHash, new ComparisonColumns(new Comparable[]{1, null}, 1)));
  }

  @Test
  public void testFreezeIsUnstableDuringSegmentOperations() {
    UpsertKeyDigest digest = new UpsertKeyDigest();
    assertTrue(digest.freeze().stable());
    digest.beginUnstable();
    digest.add(KEY_1, 1, false);
    assertFalse(digest.freeze().stable());
    digest.endUnstable();
    assertTrue(digest.freeze().stable());
    digest.beginUnstable();
    digest.beginUnstable();
    digest.endUnstable();
    assertFalse(digest.freeze().stable());
    digest.endUnstable();
    assertTrue(digest.freeze().stable());
  }

  @Test
  public void testReportRequiresStableUnchangedMarks() {
    UpsertKeyDigest digest = new UpsertKeyDigest();
    digest.add(KEY_1, 10, false);
    UpsertKeyDigest.Mark start = digest.freeze();
    digest.add(KEY_2, 20, false);
    UpsertKeyDigest.Mark end = digest.freeze();

    UpsertSnapshotMetadata.KeyDigest changed = UpsertSnapshotMetadata.KeyDigest.from(start, end);
    assertFalse(changed.stable());
    assertNull(changed.total());
    assertNull(changed.buckets());
    assertEquals(changed.entries(), 1);

    UpsertSnapshotMetadata.KeyDigest stable = UpsertSnapshotMetadata.KeyDigest.from(end, digest.freeze());
    assertTrue(stable.stable());
    assertEquals(stable.algorithm(), UpsertKeyDigest.ALGORITHM);
    assertEquals(stable.total(), UpsertKeyDigest.toHex(end.total()));
    assertEquals(UpsertKeyDigest.decodeBuckets(stable.buckets()), end.buckets());
    assertEquals(stable.entries(), 2);
    assertEquals(UpsertKeyDigest.toHex(0), UpsertKeyDigest.ZERO_TOTAL);
    assertEquals(UpsertKeyDigest.decodeBuckets(UpsertKeyDigest.encodeBuckets(new long[UpsertKeyDigest.BUCKETS])),
        new long[UpsertKeyDigest.BUCKETS]);
  }
}
