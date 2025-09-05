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

import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.segment.spi.Constants;
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
    AndDocIdIterator andDocIdIterator = AndDocIdIterator.create(
        new BlockDocIdIterator[]{
            RangelessBitmapDocIdIterator.create(bitmap1, true),
            RangelessBitmapDocIdIterator.create(bitmap2, true),
            RangelessBitmapDocIdIterator.create(bitmap3, true)
    }, true);

    assertEquals(andDocIdIterator.next(), 2);
    assertEquals(andDocIdIterator.next(), 7);
    assertEquals(andDocIdIterator.advance(10), 13);
    assertEquals(andDocIdIterator.advance(16), 16);
    assertEquals(andDocIdIterator.next(), 20);
    assertEquals(andDocIdIterator.next(), Constants.EOF);
  }

  @Test
  public void testDescIteratorBasicNext() {
    // Test basic next() functionality for descending iterator
    // Expected AND result in descending order: [20, 16, 15, 13, 7, 2]
    int[] docIds1 = new int[]{0, 1, 2, 3, 5, 7, 10, 12, 13, 15, 16, 18, 20};
    int[] docIds2 = new int[]{1, 2, 4, 5, 6, 7, 9, 11, 12, 13, 15, 16, 17, 19, 20};
    int[] docIds3 = new int[]{0, 2, 3, 4, 7, 8, 10, 11, 13, 15, 16, 19, 20};

    MutableRoaringBitmap bitmap1 = new MutableRoaringBitmap();
    bitmap1.add(docIds1);
    MutableRoaringBitmap bitmap2 = new MutableRoaringBitmap();
    bitmap2.add(docIds2);
    MutableRoaringBitmap bitmap3 = new MutableRoaringBitmap();
    bitmap3.add(docIds3);

    AndDocIdIterator descIterator = AndDocIdIterator.create(
        new BlockDocIdIterator[]{
            RangelessBitmapDocIdIterator.create(bitmap1, false),
            RangelessBitmapDocIdIterator.create(bitmap2, false),
            RangelessBitmapDocIdIterator.create(bitmap3, false)
        }, false);

    // Test sequential next() calls
    assertEquals(descIterator.next(), 20);
    assertEquals(descIterator.next(), 16);
    assertEquals(descIterator.next(), 15);
    assertEquals(descIterator.next(), 13);
    assertEquals(descIterator.next(), 7);
    assertEquals(descIterator.next(), 2);
    assertEquals(descIterator.next(), Constants.EOF);
  }

  @Test
  public void testDescIteratorAdvanceToExistingDoc() {
    // Test advance() to an existing document ID
    int[] docIds1 = new int[]{0, 1, 2, 3, 5, 7, 10, 12, 13, 15, 16, 18, 20};
    int[] docIds2 = new int[]{1, 2, 4, 5, 6, 7, 9, 11, 12, 13, 15, 16, 17, 19, 20};
    int[] docIds3 = new int[]{0, 2, 3, 4, 7, 8, 10, 11, 13, 15, 16, 19, 20};

    MutableRoaringBitmap bitmap1 = new MutableRoaringBitmap();
    bitmap1.add(docIds1);
    MutableRoaringBitmap bitmap2 = new MutableRoaringBitmap();
    bitmap2.add(docIds2);
    MutableRoaringBitmap bitmap3 = new MutableRoaringBitmap();
    bitmap3.add(docIds3);

    AndDocIdIterator descIterator = AndDocIdIterator.create(
        new BlockDocIdIterator[]{
            RangelessBitmapDocIdIterator.create(bitmap1, false),
            RangelessBitmapDocIdIterator.create(bitmap2, false),
            RangelessBitmapDocIdIterator.create(bitmap3, false)
        }, false);

    // Advance to existing document 15
    assertEquals(descIterator.advance(15), 15);
    assertEquals(descIterator.next(), 13);
    assertEquals(descIterator.next(), 7);
  }

  @Test
  public void testDescIteratorAdvanceToNonExistingDoc() {
    // Test advance() to a non-existing document ID
    int[] docIds1 = new int[]{0, 1, 2, 3, 5, 7, 10, 12, 13, 15, 16, 18, 20};
    int[] docIds2 = new int[]{1, 2, 4, 5, 6, 7, 9, 11, 12, 13, 15, 16, 17, 19, 20};
    int[] docIds3 = new int[]{0, 2, 3, 4, 7, 8, 10, 11, 13, 15, 16, 19, 20};

    MutableRoaringBitmap bitmap1 = new MutableRoaringBitmap();
    bitmap1.add(docIds1);
    MutableRoaringBitmap bitmap2 = new MutableRoaringBitmap();
    bitmap2.add(docIds2);
    MutableRoaringBitmap bitmap3 = new MutableRoaringBitmap();
    bitmap3.add(docIds3);

    AndDocIdIterator descIterator = AndDocIdIterator.create(
        new BlockDocIdIterator[]{
            RangelessBitmapDocIdIterator.create(bitmap1, false),
            RangelessBitmapDocIdIterator.create(bitmap2, false),
            RangelessBitmapDocIdIterator.create(bitmap3, false)
        }, false);

    // Advance to non-existing document 14 (should return 13 - highest document <= 14)
    assertEquals(descIterator.advance(14), 13);
    assertEquals(descIterator.next(), 7);
  }

  @Test
  public void testDescIteratorAdvanceToHigherDoc() {
    // Test advance() to a document ID higher than all common documents
    int[] docIds1 = new int[]{0, 1, 2, 3, 5, 7, 10, 12, 13, 15, 16, 18, 20};
    int[] docIds2 = new int[]{1, 2, 4, 5, 6, 7, 9, 11, 12, 13, 15, 16, 17, 19, 20};
    int[] docIds3 = new int[]{0, 2, 3, 4, 7, 8, 10, 11, 13, 15, 16, 19, 20};

    MutableRoaringBitmap bitmap1 = new MutableRoaringBitmap();
    bitmap1.add(docIds1);
    MutableRoaringBitmap bitmap2 = new MutableRoaringBitmap();
    bitmap2.add(docIds2);
    MutableRoaringBitmap bitmap3 = new MutableRoaringBitmap();
    bitmap3.add(docIds3);

    AndDocIdIterator descIterator = AndDocIdIterator.create(
        new BlockDocIdIterator[]{
            RangelessBitmapDocIdIterator.create(bitmap1, false),
            RangelessBitmapDocIdIterator.create(bitmap2, false),
            RangelessBitmapDocIdIterator.create(bitmap3, false)
        }, false);

    // Advance to document 25 (higher than all documents, should return 20)
    assertEquals(descIterator.advance(25), 20);
    assertEquals(descIterator.next(), 16);
  }

  @Test
  public void testDescIteratorAdvanceToLowerDoc() {
    // Test advance() to a document ID lower than current position
    // Expected AND result in descending order: [20, 7, 2]
    int[] docIds1 = new int[]{0, 1, 2, 3, 5, 7, 20};
    int[] docIds2 = new int[]{1, 2, 4, 5, 6, 7, 20};
    int[] docIds3 = new int[]{0, 2, 3, 4, 7, 8, 20};

    MutableRoaringBitmap bitmap1 = new MutableRoaringBitmap();
    bitmap1.add(docIds1);
    MutableRoaringBitmap bitmap2 = new MutableRoaringBitmap();
    bitmap2.add(docIds2);
    MutableRoaringBitmap bitmap3 = new MutableRoaringBitmap();
    bitmap3.add(docIds3);

    AndDocIdIterator descIterator = AndDocIdIterator.create(
        new BlockDocIdIterator[]{
            RangelessBitmapDocIdIterator.create(bitmap1, false),
            RangelessBitmapDocIdIterator.create(bitmap2, false),
            RangelessBitmapDocIdIterator.create(bitmap3, false)
        }, false);

    // Start at highest, then advance to very low document
    assertEquals(descIterator.next(), 20);
    assertEquals(descIterator.advance(1), Constants.EOF); // No common docs <= 1
  }

  @Test
  public void testDescIteratorExhaustAllDocs() {
    // Test iterating through all documents until exhaustion
    int[] docIds1 = new int[]{5, 10, 15};
    int[] docIds2 = new int[]{5, 10, 20};
    int[] docIds3 = new int[]{5, 10, 25};

    MutableRoaringBitmap bitmap1 = new MutableRoaringBitmap();
    bitmap1.add(docIds1);
    MutableRoaringBitmap bitmap2 = new MutableRoaringBitmap();
    bitmap2.add(docIds2);
    MutableRoaringBitmap bitmap3 = new MutableRoaringBitmap();
    bitmap3.add(docIds3);

    AndDocIdIterator descIterator = AndDocIdIterator.create(
        new BlockDocIdIterator[]{
            RangelessBitmapDocIdIterator.create(bitmap1, false),
            RangelessBitmapDocIdIterator.create(bitmap2, false),
            RangelessBitmapDocIdIterator.create(bitmap3, false)
        }, false);

    // Expected AND result: [10, 5] (common docs in descending order)
    assertEquals(descIterator.next(), 10);
    assertEquals(descIterator.next(), 5);
    assertEquals(descIterator.next(), Constants.EOF);
    assertEquals(descIterator.next(), Constants.EOF); // Should remain EOF
  }

  @Test
  public void testDescIteratorSingleIterator() {
    // Test with only one child iterator
    int[] docIds = new int[]{2, 8, 12, 20};

    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(docIds);

    AndDocIdIterator descIterator = AndDocIdIterator.create(
        new BlockDocIdIterator[]{
            RangelessBitmapDocIdIterator.create(bitmap, false)
        }, false);

    assertEquals(descIterator.next(), 20);
    assertEquals(descIterator.next(), 12);
    assertEquals(descIterator.advance(10), 8);
    assertEquals(descIterator.next(), 2);
    assertEquals(descIterator.next(), Constants.EOF);
  }

  @Test
  public void testDescIteratorEmptyIterators() {
    // Test with empty iterators
    MutableRoaringBitmap emptyBitmap1 = new MutableRoaringBitmap();
    MutableRoaringBitmap emptyBitmap2 = new MutableRoaringBitmap();

    AndDocIdIterator descIterator = AndDocIdIterator.create(
        new BlockDocIdIterator[]{
            RangelessBitmapDocIdIterator.create(emptyBitmap1, false),
            RangelessBitmapDocIdIterator.create(emptyBitmap2, false)
        }, false);

    assertEquals(descIterator.next(), Constants.EOF);
    assertEquals(descIterator.advance(10), Constants.EOF);
  }

  @Test
  public void testDescIteratorNoCommonDocs() {
    // Test with iterators that have no common document IDs
    int[] docIds1 = new int[]{1, 3, 5};
    int[] docIds2 = new int[]{2, 4, 6};

    MutableRoaringBitmap bitmap1 = new MutableRoaringBitmap();
    bitmap1.add(docIds1);
    MutableRoaringBitmap bitmap2 = new MutableRoaringBitmap();
    bitmap2.add(docIds2);

    AndDocIdIterator descIterator = AndDocIdIterator.create(
        new BlockDocIdIterator[]{
            RangelessBitmapDocIdIterator.create(bitmap1, false),
            RangelessBitmapDocIdIterator.create(bitmap2, false)
        }, false);

    // No common documents, should return EOF immediately
    assertEquals(descIterator.next(), Constants.EOF);
    assertEquals(descIterator.advance(10), Constants.EOF);
  }

  @Test
  public void testDescIteratorPartialOverlap() {
    // Test with iterators that have partial overlap
    int[] docIds1 = new int[]{5, 10, 15, 20, 25};
    int[] docIds2 = new int[]{10, 15, 30, 35};
    int[] docIds3 = new int[]{8, 10, 12, 15, 18};

    MutableRoaringBitmap bitmap1 = new MutableRoaringBitmap();
    bitmap1.add(docIds1);
    MutableRoaringBitmap bitmap2 = new MutableRoaringBitmap();
    bitmap2.add(docIds2);
    MutableRoaringBitmap bitmap3 = new MutableRoaringBitmap();
    bitmap3.add(docIds3);

    AndDocIdIterator descIterator = AndDocIdIterator.create(
        new BlockDocIdIterator[]{
            RangelessBitmapDocIdIterator.create(bitmap1, false),
            RangelessBitmapDocIdIterator.create(bitmap2, false),
            RangelessBitmapDocIdIterator.create(bitmap3, false)
        }, false);

    // Common documents: [15, 10] in descending order
    assertEquals(descIterator.next(), 15);
    assertEquals(descIterator.next(), 10);
    assertEquals(descIterator.next(), Constants.EOF);
  }

  @Test
  public void testDescIteratorMixedAdvanceAndNext() {
    // Test mixing advance() and next() calls
    int[] docIds1 = new int[]{0, 1, 2, 3, 5, 7, 10, 12, 13, 15, 16, 18, 20};
    int[] docIds2 = new int[]{1, 2, 4, 5, 6, 7, 9, 11, 12, 13, 15, 16, 17, 19, 20};
    int[] docIds3 = new int[]{0, 2, 3, 4, 7, 8, 10, 11, 13, 15, 16, 19, 20};

    MutableRoaringBitmap bitmap1 = new MutableRoaringBitmap();
    bitmap1.add(docIds1);
    MutableRoaringBitmap bitmap2 = new MutableRoaringBitmap();
    bitmap2.add(docIds2);
    MutableRoaringBitmap bitmap3 = new MutableRoaringBitmap();
    bitmap3.add(docIds3);

    AndDocIdIterator descIterator = AndDocIdIterator.create(
        new BlockDocIdIterator[]{
            RangelessBitmapDocIdIterator.create(bitmap1, false),
            RangelessBitmapDocIdIterator.create(bitmap2, false),
            RangelessBitmapDocIdIterator.create(bitmap3, false)
        }, false);

    assertEquals(descIterator.advance(16), 16);
    assertEquals(descIterator.next(), 15);
    assertEquals(descIterator.advance(10), 7);
    assertEquals(descIterator.next(), 2);
    assertEquals(descIterator.advance(1), Constants.EOF);
  }
}
