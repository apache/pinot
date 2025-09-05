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


public class OrDocIdIteratorTest {

  @Test
  public void testOrDocIdIterator() {
    // OR result: [0, 1, 2, 4, 5, 6, 8, 10, 13, 15, 16, 17, 18, 19, 20]
    int[] docIds1 = new int[]{1, 4, 6, 10, 15, 17, 18, 20};
    int[] docIds2 = new int[]{0, 1, 5, 8, 15, 18};
    int[] docIds3 = new int[]{1, 2, 6, 13, 16, 19};

    MutableRoaringBitmap bitmap1 = new MutableRoaringBitmap();
    bitmap1.add(docIds1);
    MutableRoaringBitmap bitmap2 = new MutableRoaringBitmap();
    bitmap2.add(docIds2);
    MutableRoaringBitmap bitmap3 = new MutableRoaringBitmap();
    bitmap3.add(docIds3);
    OrDocIdIterator andDocIdIterator = OrDocIdIterator.create(
        new BlockDocIdIterator[]{
            RangelessBitmapDocIdIterator.create(bitmap1, true),
            RangelessBitmapDocIdIterator.create(bitmap2, true),
            RangelessBitmapDocIdIterator.create(bitmap3, true)
    }, true);

    assertEquals(andDocIdIterator.advance(1), 1);
    assertEquals(andDocIdIterator.next(), 2);
    assertEquals(andDocIdIterator.next(), 4);
    assertEquals(andDocIdIterator.advance(7), 8);
    assertEquals(andDocIdIterator.advance(13), 13);
    assertEquals(andDocIdIterator.next(), 15);
    assertEquals(andDocIdIterator.advance(18), 18);
    assertEquals(andDocIdIterator.next(), 19);
    assertEquals(andDocIdIterator.advance(21), Constants.EOF);
  }

  @Test
  public void testDescIteratorBasicNext() {
    // Test basic next() functionality for descending iterator
    // Expected OR result in descending order: [20, 19, 18, 17, 16, 15, 13, 10, 8, 6, 5, 4, 2, 1, 0]
    int[] docIds1 = new int[]{1, 4, 6, 10, 15, 17, 18, 20};
    int[] docIds2 = new int[]{0, 1, 5, 8, 15, 18};
    int[] docIds3 = new int[]{1, 2, 6, 13, 16, 19};

    MutableRoaringBitmap bitmap1 = new MutableRoaringBitmap();
    bitmap1.add(docIds1);
    MutableRoaringBitmap bitmap2 = new MutableRoaringBitmap();
    bitmap2.add(docIds2);
    MutableRoaringBitmap bitmap3 = new MutableRoaringBitmap();
    bitmap3.add(docIds3);

    OrDocIdIterator descIterator = OrDocIdIterator.create(
        new BlockDocIdIterator[]{
            RangelessBitmapDocIdIterator.create(bitmap1, false),
            RangelessBitmapDocIdIterator.create(bitmap2, false),
            RangelessBitmapDocIdIterator.create(bitmap3, false)
        }, false);

    // Test sequential next() calls
    assertEquals(descIterator.next(), 20);
    assertEquals(descIterator.next(), 19);
    assertEquals(descIterator.next(), 18);
    assertEquals(descIterator.next(), 17);
    assertEquals(descIterator.next(), 16);
    assertEquals(descIterator.next(), 15);
    assertEquals(descIterator.next(), 13);
    assertEquals(descIterator.next(), 10);
  }

  @Test
  public void testDescIteratorAdvanceToExistingDoc() {
    // Test advance() to an existing document ID
    int[] docIds1 = new int[]{1, 4, 6, 10, 15, 17, 18, 20};
    int[] docIds2 = new int[]{0, 1, 5, 8, 15, 18};
    int[] docIds3 = new int[]{1, 2, 6, 13, 16, 19};

    MutableRoaringBitmap bitmap1 = new MutableRoaringBitmap();
    bitmap1.add(docIds1);
    MutableRoaringBitmap bitmap2 = new MutableRoaringBitmap();
    bitmap2.add(docIds2);
    MutableRoaringBitmap bitmap3 = new MutableRoaringBitmap();
    bitmap3.add(docIds3);

    OrDocIdIterator descIterator = OrDocIdIterator.create(
        new BlockDocIdIterator[]{
            RangelessBitmapDocIdIterator.create(bitmap1, false),
            RangelessBitmapDocIdIterator.create(bitmap2, false),
            RangelessBitmapDocIdIterator.create(bitmap3, false)
        }, false);

    // Advance to existing document 15
    assertEquals(descIterator.advance(15), 15);
    assertEquals(descIterator.next(), 13);
    assertEquals(descIterator.next(), 10);
  }

  @Test
  public void testDescIteratorAdvanceToNonExistingDoc() {
    // Test advance() to a non-existing document ID
    int[] docIds1 = new int[]{1, 4, 6, 10, 15, 17, 18, 20};
    int[] docIds2 = new int[]{0, 1, 5, 8, 15, 18};
    int[] docIds3 = new int[]{1, 2, 6, 13, 16, 19};

    MutableRoaringBitmap bitmap1 = new MutableRoaringBitmap();
    bitmap1.add(docIds1);
    MutableRoaringBitmap bitmap2 = new MutableRoaringBitmap();
    bitmap2.add(docIds2);
    MutableRoaringBitmap bitmap3 = new MutableRoaringBitmap();
    bitmap3.add(docIds3);

    OrDocIdIterator descIterator = OrDocIdIterator.create(
        new BlockDocIdIterator[]{
            RangelessBitmapDocIdIterator.create(bitmap1, false),
            RangelessBitmapDocIdIterator.create(bitmap2, false),
            RangelessBitmapDocIdIterator.create(bitmap3, false)
        }, false);

    // Advance to non-existing document 14 (should return 13)
    assertEquals(descIterator.advance(14), 13);
    assertEquals(descIterator.next(), 10);
  }

  @Test
  public void testDescIteratorAdvanceToHigherDoc() {
    // Test advance() to a document ID higher than current position
    int[] docIds1 = new int[]{1, 4, 6, 10, 15, 17, 18, 20};
    int[] docIds2 = new int[]{0, 1, 5, 8, 15, 18};
    int[] docIds3 = new int[]{1, 2, 6, 13, 16, 19};

    MutableRoaringBitmap bitmap1 = new MutableRoaringBitmap();
    bitmap1.add(docIds1);
    MutableRoaringBitmap bitmap2 = new MutableRoaringBitmap();
    bitmap2.add(docIds2);
    MutableRoaringBitmap bitmap3 = new MutableRoaringBitmap();
    bitmap3.add(docIds3);

    OrDocIdIterator descIterator = OrDocIdIterator.create(
        new BlockDocIdIterator[]{
            RangelessBitmapDocIdIterator.create(bitmap1, false),
            RangelessBitmapDocIdIterator.create(bitmap2, false),
            RangelessBitmapDocIdIterator.create(bitmap3, false)
        }, false);

    // Start from beginning and advance to high document 25 (beyond all docs)
    assertEquals(descIterator.advance(25), Constants.EOF);
  }

  @Test
  public void testDescIteratorExhaustAllDocs() {
    // Test iterating through all documents until exhaustion
    int[] docIds1 = new int[]{5, 10};
    int[] docIds2 = new int[]{3, 7};

    MutableRoaringBitmap bitmap1 = new MutableRoaringBitmap();
    bitmap1.add(docIds1);
    MutableRoaringBitmap bitmap2 = new MutableRoaringBitmap();
    bitmap2.add(docIds2);

    OrDocIdIterator descIterator = OrDocIdIterator.create(
        new BlockDocIdIterator[]{
            RangelessBitmapDocIdIterator.create(bitmap1, false),
            RangelessBitmapDocIdIterator.create(bitmap2, false)
        }, false);

    // Expected order: [10, 7, 5, 3]
    assertEquals(descIterator.next(), 10);
    assertEquals(descIterator.next(), 7);
    assertEquals(descIterator.next(), 5);
    assertEquals(descIterator.next(), 3);
    assertEquals(descIterator.next(), Constants.EOF);
    assertEquals(descIterator.next(), Constants.EOF); // Should remain EOF
  }

  @Test
  public void testDescIteratorSingleIterator() {
    // Test with only one child iterator
    int[] docIds = new int[]{2, 8, 12, 20};

    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(docIds);

    OrDocIdIterator descIterator = OrDocIdIterator.create(
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

    OrDocIdIterator descIterator = OrDocIdIterator.create(
        new BlockDocIdIterator[]{
            RangelessBitmapDocIdIterator.create(emptyBitmap1, false),
            RangelessBitmapDocIdIterator.create(emptyBitmap2, false)
        }, false);

    assertEquals(descIterator.next(), Constants.EOF);
    assertEquals(descIterator.advance(10), Constants.EOF);
  }

  @Test
  public void testDescIteratorDuplicateDocIds() {
    // Test with overlapping document IDs between iterators
    int[] docIds1 = new int[]{5, 10, 15};
    int[] docIds2 = new int[]{10, 15, 20}; // Overlaps with docIds1

    MutableRoaringBitmap bitmap1 = new MutableRoaringBitmap();
    bitmap1.add(docIds1);
    MutableRoaringBitmap bitmap2 = new MutableRoaringBitmap();
    bitmap2.add(docIds2);

    OrDocIdIterator descIterator = OrDocIdIterator.create(
        new BlockDocIdIterator[]{
            RangelessBitmapDocIdIterator.create(bitmap1, false),
            RangelessBitmapDocIdIterator.create(bitmap2, false)
        }, false);

    // Expected OR result in descending order: [20, 15, 10, 5]
    assertEquals(descIterator.next(), 20);
    assertEquals(descIterator.next(), 15); // Should appear only once despite being in both sets
    assertEquals(descIterator.next(), 10); // Should appear only once despite being in both sets
    assertEquals(descIterator.next(), 5);
    assertEquals(descIterator.next(), Constants.EOF);
  }

  @Test
  public void testDescIteratorMixedAdvanceAndNext() {
    // Test mixing advance() and next() calls
    int[] docIds1 = new int[]{1, 4, 6, 10, 15, 17, 18, 20};
    int[] docIds2 = new int[]{0, 1, 5, 8, 15, 18};
    int[] docIds3 = new int[]{1, 2, 6, 13, 16, 19};

    MutableRoaringBitmap bitmap1 = new MutableRoaringBitmap();
    bitmap1.add(docIds1);
    MutableRoaringBitmap bitmap2 = new MutableRoaringBitmap();
    bitmap2.add(docIds2);
    MutableRoaringBitmap bitmap3 = new MutableRoaringBitmap();
    bitmap3.add(docIds3);

    OrDocIdIterator descIterator = OrDocIdIterator.create(
        new BlockDocIdIterator[]{
            RangelessBitmapDocIdIterator.create(bitmap1, false),
            RangelessBitmapDocIdIterator.create(bitmap2, false),
            RangelessBitmapDocIdIterator.create(bitmap3, false)
        }, false);

    assertEquals(descIterator.advance(16), 16);
    assertEquals(descIterator.next(), 15);
    assertEquals(descIterator.advance(7), 6);
    assertEquals(descIterator.next(), 5);
    assertEquals(descIterator.advance(3), 2);
    assertEquals(descIterator.next(), 1);
    assertEquals(descIterator.next(), 0);
    assertEquals(descIterator.advance(0), Constants.EOF);
  }
}
