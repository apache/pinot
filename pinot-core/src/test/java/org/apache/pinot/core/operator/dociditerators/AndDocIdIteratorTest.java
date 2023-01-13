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
package org.apache.pinot.core.operator.dociditerators;

import org.apache.pinot.segment.spi.Constants;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class AndDocIdIteratorTest {

  @Test
  public void testAndDocIdIterator() {
    // AND result: [2, 7, 13, 15, 16, 20]
    int[] docIds1 = new int[]{0, 1, 2, 3, 5, 7, 10, 12, 13, 15, 16, 18, 20};
    int[] docIds2 = new int[]{1, 2, 4, 5, 6, 7, 9, 11, 12, 13, 15, 16, 17, 19, 20};
    int[] docIds3 = new int[]{0, 2, 3, 4, 7, 8, 10, 11, 13, 15, 16, 19, 20};

    MutableRoaringBitmap bitmap1 = new MutableRoaringBitmap();
    bitmap1.add(docIds1);
    MutableRoaringBitmap bitmap2 = new MutableRoaringBitmap();
    bitmap2.add(docIds2);
    MutableRoaringBitmap bitmap3 = new MutableRoaringBitmap();
    bitmap3.add(docIds3);
    AndDocIdIterator andDocIdIterator =
        new AndDocIdIterator(new RangelessBitmapDocIdIterator(bitmap1), new RangelessBitmapDocIdIterator(bitmap2),
            new RangelessBitmapDocIdIterator(bitmap3));

    assertEquals(andDocIdIterator.next(), 2);
    assertEquals(andDocIdIterator.next(), 7);
    assertEquals(andDocIdIterator.advance(10), 13);
    assertEquals(andDocIdIterator.advance(16), 16);
    assertEquals(andDocIdIterator.next(), 20);
    assertEquals(andDocIdIterator.next(), Constants.EOF);
  }

  @Test
  public void testAndDocIdIteratorWithInvertedChildren() {
    int numDocs = 20;
    // not = [4, 6, 8, 11, 14, 17, 19]
    int[] docIds1 = new int[]{0, 1, 2, 3, 5, 7, 10, 12, 13, 15, 16, 18, 20};
    // not = [0, 3, 8, 10, 14, 17, 18]
    int[] docIds2 = new int[]{1, 2, 4, 5, 6, 7, 9, 11, 12, 13, 15, 16, 19, 20};
    // not = [1, 5, 6, 9, 12, 14, 17, 18]
    int[] docIds3 = new int[]{0, 2, 3, 4, 7, 8, 10, 11, 13, 15, 16, 19, 20};
    int[] expected = new int[]{14, 17};

    ImmutableRoaringBitmap bitmap1 = ImmutableRoaringBitmap.bitmapOf(docIds1);
    ImmutableRoaringBitmap bitmap2 = ImmutableRoaringBitmap.bitmapOf(docIds2);
    ImmutableRoaringBitmap bitmap3 = ImmutableRoaringBitmap.bitmapOf(docIds3);
    AndDocIdIterator andDocIdIterator = new AndDocIdIterator(new InvertedBitmapDocIdIterator(bitmap1, numDocs),
        new InvertedBitmapDocIdIterator(bitmap2, numDocs), new InvertedBitmapDocIdIterator(bitmap3, numDocs));

    for (int docId : expected) {
      assertEquals(andDocIdIterator.next(), docId);
    }
    assertEquals(andDocIdIterator.next(), Constants.EOF);
  }

  @Test
  public void testAndDocIdIteratorWithMixedChildren() {
    int numDocs = 20;
    int[] docIds1 = new int[]{0, 1, 2, 3, 5, 7, 10, 12, 13, 14, 15, 16, 17, 18, 20};
    // not = [0, 3, 8, 10, 14, 17, 18]
    int[] docIds2 = new int[]{1, 2, 4, 5, 6, 7, 9, 11, 12, 13, 15, 16, 19, 20};
    // not = [1, 5, 6, 9, 12, 14, 17, 18]
    int[] docIds3 = new int[]{0, 2, 3, 4, 7, 8, 10, 11, 13, 15, 16, 19, 20};
    int[] expected = new int[]{14, 17, 18};

    ImmutableRoaringBitmap bitmap1 = ImmutableRoaringBitmap.bitmapOf(docIds1);
    ImmutableRoaringBitmap bitmap2 = ImmutableRoaringBitmap.bitmapOf(docIds2);
    ImmutableRoaringBitmap bitmap3 = ImmutableRoaringBitmap.bitmapOf(docIds3);
    AndDocIdIterator andDocIdIterator = new AndDocIdIterator(new BitmapDocIdIterator(bitmap1, numDocs),
        new InvertedBitmapDocIdIterator(bitmap2, numDocs), new InvertedBitmapDocIdIterator(bitmap3, numDocs));

    for (int docId : expected) {
      assertEquals(andDocIdIterator.next(), docId);
    }
    assertEquals(andDocIdIterator.next(), Constants.EOF);
  }
}
