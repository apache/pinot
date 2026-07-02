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
package org.apache.pinot.core.operator.docidsets;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.segment.spi.Constants;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/**
 * Unit test for {@link AndDocIdSet}, focusing on the bitmap-only intersection path.
 */
public class AndDocIdSetTest {
  private static final long RANDOM_SEED = System.currentTimeMillis();
  private static final Random RANDOM = new Random(RANDOM_SEED);
  private static final String ERROR_MESSAGE = "Random seed: " + RANDOM_SEED;
  private static final int NUM_DOCS = 100000;

  @Test
  public void testBitmapOnlyIntersection() {
    // Cover both the 2-bitmap and the 3+-bitmap paths, mixing operand shapes: dense (bitmap containers),
    // sparse (array containers), run-optimized, and a serialized buffer-backed ImmutableRoaringBitmap (the
    // shape produced by memory-mapped index readers).
    for (int numBitmaps = 2; numBitmaps <= 4; numBitmaps++) {
      ImmutableRoaringBitmap[] bitmaps = new ImmutableRoaringBitmap[numBitmaps];
      MutableRoaringBitmap expected = null;
      for (int i = 0; i < numBitmaps; i++) {
        MutableRoaringBitmap bitmap = randomBitmap(i == 1 ? 0.001 : 0.2 + 0.2 * i);
        switch (i % 4) {
          case 1:
            // Sparse bitmap left as-is (array containers)
            bitmaps[i] = bitmap;
            break;
          case 2:
            bitmap.runOptimize();
            bitmaps[i] = bitmap;
            break;
          case 3:
            bitmaps[i] = serialize(bitmap);
            break;
          default:
            bitmaps[i] = bitmap;
            break;
        }
        expected = expected == null ? bitmaps[i].toMutableRoaringBitmap()
            : MutableRoaringBitmap.and(expected, bitmaps[i].toMutableRoaringBitmap());
      }
      ImmutableRoaringBitmap[] copies = new ImmutableRoaringBitmap[numBitmaps];
      List<BlockDocIdSet> docIdSets = new ArrayList<>(numBitmaps);
      for (int i = 0; i < numBitmaps; i++) {
        copies[i] = bitmaps[i].toMutableRoaringBitmap();
        docIdSets.add(new BitmapDocIdSet(bitmaps[i], NUM_DOCS));
      }

      int[] actualDocIds = collectDocIds(new AndDocIdSet(docIdSets, null));

      assertEquals(actualDocIds, expected.toArray(), ERROR_MESSAGE);
      // The intersection must not mutate the input bitmaps (they may be shared or backed by index buffers)
      for (int i = 0; i < numBitmaps; i++) {
        assertEquals(bitmaps[i].toMutableRoaringBitmap(), copies[i], ERROR_MESSAGE);
      }
    }
  }

  @Test
  public void testBitmapOnlyIntersectionEmptyResult() {
    MutableRoaringBitmap first = MutableRoaringBitmap.bitmapOf(1, 3, 5);
    MutableRoaringBitmap second = MutableRoaringBitmap.bitmapOf(0, 2, 4);
    List<BlockDocIdSet> docIdSets =
        List.of(new BitmapDocIdSet(first, NUM_DOCS), new BitmapDocIdSet(second, NUM_DOCS));

    int[] actualDocIds = collectDocIds(new AndDocIdSet(docIdSets, null));

    assertEquals(actualDocIds.length, 0);
  }

  private static MutableRoaringBitmap randomBitmap(double density) {
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    for (int docId = 0; docId < NUM_DOCS; docId++) {
      if (RANDOM.nextDouble() < density) {
        bitmap.add(docId);
      }
    }
    return bitmap;
  }

  private static ImmutableRoaringBitmap serialize(MutableRoaringBitmap bitmap) {
    ByteBuffer buffer = ByteBuffer.allocate(bitmap.serializedSizeInBytes());
    bitmap.serialize(buffer);
    buffer.flip();
    return new ImmutableRoaringBitmap(buffer);
  }

  private static int[] collectDocIds(BlockDocIdSet docIdSet) {
    BlockDocIdIterator iterator = docIdSet.iterator();
    List<Integer> docIds = new ArrayList<>();
    int docId;
    while ((docId = iterator.next()) != Constants.EOF) {
      docIds.add(docId);
    }
    return docIds.stream().mapToInt(Integer::intValue).toArray();
  }
}
