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
package org.apache.pinot.core.query.utils.idset;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import java.io.IOException;
import java.util.Random;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


public class IdSetTest {
  private static final int NUM_VALUES = 10000;
  private static final int MAX_VALUE = 20000;
  private static final Random RANDOM = new Random();

  private final IntOpenHashSet _intSet = new IntOpenHashSet();
  private final LongOpenHashSet _longSet = new LongOpenHashSet();
  private final IdSet _intIdSet =
      IdSets.create(DataType.INT, IdSets.DEFAULT_SIZE_THRESHOLD_IN_BYTES, NUM_VALUES, IdSets.DEFAULT_FPP);
  private final IdSet _longIdSet =
      IdSets.create(DataType.LONG, IdSets.DEFAULT_SIZE_THRESHOLD_IN_BYTES, NUM_VALUES, IdSets.DEFAULT_FPP);
  private final IdSet _intBloomFilterIdSet = IdSets.create(DataType.INT, 0, NUM_VALUES, IdSets.DEFAULT_FPP);
  private final IdSet _longBloomFilterIdSet = IdSets.create(DataType.LONG, 0, NUM_VALUES, IdSets.DEFAULT_FPP);

  @BeforeClass
  public void setUp() {
    for (int i = 0; i < NUM_VALUES; i++) {
      int intId = RANDOM.nextInt(MAX_VALUE);
      long longId = intId + (long) Integer.MAX_VALUE;
      _intSet.add(intId);
      _longSet.add(longId);
      _intIdSet.add(intId);
      _longIdSet.add(longId);
      _intBloomFilterIdSet.add(intId);
      _longBloomFilterIdSet.add(longId);
    }
  }

  @Test
  public void testIdSet() {
    testEmptyIdSet(IdSets.emptyIdSet());
    assertEquals(_intIdSet.getType(), IdSet.Type.ROARING_BITMAP);
    testIntIdSet(_intIdSet);
    assertEquals(_longIdSet.getType(), IdSet.Type.ROARING_64_NAVIGABLE_MAP);
    testLongIdSet(_longIdSet);
    assertEquals(_intBloomFilterIdSet.getType(), IdSet.Type.BLOOM_FILTER);
    testIntIdSet(_intBloomFilterIdSet);
    assertEquals(_longBloomFilterIdSet.getType(), IdSet.Type.BLOOM_FILTER);
    testLongIdSet(_longBloomFilterIdSet);
  }

  private void testEmptyIdSet(IdSet emptyIdSet) {
    assertEquals(emptyIdSet.getType(), IdSet.Type.EMPTY);
  }

  private void testIntIdSet(IdSet intIdSet) {
    if (intIdSet.getType() == IdSet.Type.ROARING_BITMAP) {
      for (int i = 0; i < NUM_VALUES; i++) {
        int intId = RANDOM.nextInt(MAX_VALUE);
        assertEquals(intIdSet.contains(intId), _intSet.contains(intId));
      }
    } else {
      assertEquals(intIdSet.getType(), IdSet.Type.BLOOM_FILTER);
      for (int i = 0; i < NUM_VALUES; i++) {
        int intId = RANDOM.nextInt(MAX_VALUE);
        if (_intIdSet.contains(intId)) {
          assertTrue(intIdSet.contains(intId));
        }
      }
    }
  }

  private void testLongIdSet(IdSet longIdSet) {
    if (longIdSet.getType() == IdSet.Type.ROARING_64_NAVIGABLE_MAP) {
      for (int i = 0; i < NUM_VALUES; i++) {
        long longId = RANDOM.nextInt(MAX_VALUE) + (long) Integer.MAX_VALUE;
        assertEquals(longIdSet.contains(longId), _longSet.contains(longId));
      }
    } else {
      assertEquals(longIdSet.getType(), IdSet.Type.BLOOM_FILTER);
      for (int i = 0; i < NUM_VALUES; i++) {
        long longId = RANDOM.nextInt(MAX_VALUE) + (long) Integer.MAX_VALUE;
        if (_longSet.contains(longId)) {
          assertTrue(longIdSet.contains(longId));
        }
      }
    }
  }

  @Test
  public void testSerDe() throws IOException {
    assertEquals(IdSets.fromBytes(IdSets.emptyIdSet().toBytes()), IdSets.emptyIdSet());
    assertEquals(IdSets.fromBase64String(IdSets.emptyIdSet().toBase64String()), IdSets.emptyIdSet());

    assertEquals(IdSets.fromBytes(_intIdSet.toBytes()), _intIdSet);
    assertEquals(IdSets.fromBase64String(_intIdSet.toBase64String()), _intIdSet);

    assertEquals(IdSets.fromBytes(_longIdSet.toBytes()), _longIdSet);
    assertEquals(IdSets.fromBase64String(_longIdSet.toBase64String()), _longIdSet);

    assertEquals(IdSets.fromBytes(_intBloomFilterIdSet.toBytes()), _intBloomFilterIdSet);
    assertEquals(IdSets.fromBase64String(_intBloomFilterIdSet.toBase64String()), _intBloomFilterIdSet);

    assertEquals(IdSets.fromBytes(_longBloomFilterIdSet.toBytes()), _longBloomFilterIdSet);
    assertEquals(IdSets.fromBase64String(_longBloomFilterIdSet.toBase64String()), _longBloomFilterIdSet);
  }

  @Test
  public void testMerge() throws IOException {
    // Merge with empty IdSet
    assertSame(IdSets.merge(IdSets.emptyIdSet(), IdSets.emptyIdSet()), IdSets.emptyIdSet());
    assertSame(IdSets.merge(_intIdSet, IdSets.emptyIdSet()), _intIdSet);
    assertSame(IdSets.merge(IdSets.emptyIdSet(), _intIdSet), _intIdSet);
    assertSame(IdSets.merge(_longIdSet, IdSets.emptyIdSet()), _longIdSet);
    assertSame(IdSets.merge(IdSets.emptyIdSet(), _longIdSet), _longIdSet);
    assertSame(IdSets.merge(_intBloomFilterIdSet, IdSets.emptyIdSet()), _intBloomFilterIdSet);
    assertSame(IdSets.merge(IdSets.emptyIdSet(), _intBloomFilterIdSet), _intBloomFilterIdSet);
    assertSame(IdSets.merge(_longBloomFilterIdSet, IdSets.emptyIdSet()), _longBloomFilterIdSet);
    assertSame(IdSets.merge(IdSets.emptyIdSet(), _longBloomFilterIdSet), _longBloomFilterIdSet);

    // Merge 2 IdSets of the same type
    {
      IdSet mergedIdSet = IdSets.merge(IdSets.fromBytes(_intIdSet.toBytes()), IdSets.fromBytes(_intIdSet.toBytes()));
      assertEquals(mergedIdSet, _intIdSet);
    }
    {
      IdSet mergedIdSet = IdSets.merge(IdSets.fromBytes(_longIdSet.toBytes()), IdSets.fromBytes(_longIdSet.toBytes()));
      assertEquals(mergedIdSet, _longIdSet);
    }
    {
      IdSet mergedIdSet = IdSets.merge(IdSets.fromBytes(_intBloomFilterIdSet.toBytes()),
          IdSets.fromBytes(_intBloomFilterIdSet.toBytes()));
      assertEquals(mergedIdSet, _intBloomFilterIdSet);
    }
    {
      IdSet mergedIdSet = IdSets.merge(IdSets.fromBytes(_longBloomFilterIdSet.toBytes()),
          IdSets.fromBytes(_longBloomFilterIdSet.toBytes()));
      assertEquals(mergedIdSet, _longBloomFilterIdSet);
    }

    // Merge with BloomFilterIdSet
    {
      IdSet mergedIdSet =
          IdSets.merge(IdSets.fromBytes(_intIdSet.toBytes()), IdSets.fromBytes(_intBloomFilterIdSet.toBytes()));
      assertEquals(mergedIdSet, _intBloomFilterIdSet);
    }
    {
      IdSet mergedIdSet =
          IdSets.merge(IdSets.fromBytes(_intBloomFilterIdSet.toBytes()), IdSets.fromBytes(_intIdSet.toBytes()));
      assertEquals(mergedIdSet, _intBloomFilterIdSet);
    }
    {
      IdSet mergedIdSet =
          IdSets.merge(IdSets.fromBytes(_longIdSet.toBytes()), IdSets.fromBytes(_longBloomFilterIdSet.toBytes()));
      assertEquals(mergedIdSet, _longBloomFilterIdSet);
    }
    {
      IdSet mergedIdSet =
          IdSets.merge(IdSets.fromBytes(_longBloomFilterIdSet.toBytes()), IdSets.fromBytes(_longIdSet.toBytes()));
      assertEquals(mergedIdSet, _longBloomFilterIdSet);
    }

    // Convert to BloomFilterIdSet
    {
      IdSet mergedIdSet = IdSets.merge(IdSets.fromBytes(_intIdSet.toBytes()), IdSets.fromBytes(_intIdSet.toBytes()), 1,
          NUM_VALUES, IdSets.DEFAULT_FPP);
      assertEquals(mergedIdSet, _intBloomFilterIdSet);
    }
    {
      IdSet mergedIdSet = IdSets.merge(IdSets.fromBytes(_longIdSet.toBytes()), IdSets.fromBytes(_longIdSet.toBytes()),
          1, NUM_VALUES, IdSets.DEFAULT_FPP);
      assertEquals(mergedIdSet, _longBloomFilterIdSet);
    }
  }
}
