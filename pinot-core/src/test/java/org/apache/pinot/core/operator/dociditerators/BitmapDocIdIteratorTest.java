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
import org.assertj.core.api.Assertions;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class BitmapDocIdIteratorTest {

  @Test
  public void testBitmapDocIdIteratorAsc() {
    int[] docIds = new int[]{1, 2, 4, 5, 6, 8, 12, 15, 16, 18, 20, 21};
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(docIds);
    int numDocs = 25;
    BitmapDocIdIterator docIdIterator = BitmapDocIdIterator.create(bitmap, numDocs, true);
    assertEquals(docIdIterator.advance(2), 2);
    assertEquals(docIdIterator.advance(3), 4);
    assertEquals(docIdIterator.next(), 5);
    assertEquals(docIdIterator.advance(6), 6);
    assertEquals(docIdIterator.next(), 8);
    assertEquals(docIdIterator.advance(13), 15);
    assertEquals(docIdIterator.advance(19), 20);
    assertEquals(docIdIterator.next(), 21);
    assertEquals(docIdIterator.next(), Constants.EOF);
  }

  @Test
  public void descNext() {
    int[] docIds = new int[]{1, 2, 4, 5, 6, 8, 12, 15, 16, 18, 20, 21};
    int[] expectedDocIds = new int[]{21, 20, 18, 16, 15, 12, 8, 6, 5, 4, 2, 1};
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(docIds);
    int numDocs = 25;
    BitmapDocIdIterator docIdIterator = BitmapDocIdIterator.create(bitmap, numDocs, false);

    int[] actualDocIds = new int[expectedDocIds.length];
    for (int i = 0; i < expectedDocIds.length; i++) {
      actualDocIds[i] = docIdIterator.next();
    }
    Assertions.assertThat(actualDocIds)
        .describedAs("Unexpected docIds from descending iteration")
        .isEqualTo(expectedDocIds);
    assertEquals(docIdIterator.next(), Constants.EOF);
  }

  @Test
  public void descAdvanceToDoc() {
    int[] docIds = new int[]{1, 2, 4, 5, 6, 8, 12, 15, 16, 18, 20, 21};
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(docIds);
    int numDocs = 25;
    BitmapDocIdIterator docIdIterator = BitmapDocIdIterator.create(bitmap, numDocs, false);

    assertEquals(docIdIterator.next(), 21);
    assertEquals(docIdIterator.advance(20), 20, "advance to matching docId should return "
        + "the docId");
  }

  @Test
  public void descAdvanceToGap() {
    int[] docIds = new int[]{1, 5, 12};
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(docIds);
    int numDocs = 25;
    BitmapDocIdIterator docIdIterator = BitmapDocIdIterator.create(bitmap, numDocs, false);

    assertEquals(docIdIterator.next(), 12);
    assertEquals(docIdIterator.advance(7), 5, "advance into gap should return "
        + "next lower docId");
    assertEquals(docIdIterator.advance(4), 1, "advance into gap should return "
        + "next lower docId");
  }

  @Test
  public void descAdvanceEof() {
    int[] docIds = new int[]{5, 12};
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(docIds);
    int numDocs = 25;
    BitmapDocIdIterator docIdIterator = BitmapDocIdIterator.create(bitmap, numDocs, false);

    assertEquals(docIdIterator.advance(4), Constants.EOF, "advance past lowest docId should return EOF");
  }

  @Test
  public void testEmptyBitmap() {
    // Test with completely empty bitmap
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    int numDocs = 25;
    BitmapDocIdIterator docIdIterator = BitmapDocIdIterator.create(bitmap, numDocs, true);

    assertEquals(docIdIterator.next(), Constants.EOF);
    assertEquals(docIdIterator.advance(10), Constants.EOF);
  }

  @Test
  public void testEmptyBitmapDesc() {
    // Test with completely empty bitmap in descending mode
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    int numDocs = 25;
    BitmapDocIdIterator docIdIterator = BitmapDocIdIterator.create(bitmap, numDocs, false);

    assertEquals(docIdIterator.next(), Constants.EOF);
    assertEquals(docIdIterator.advance(10), Constants.EOF);
  }

  @Test
  public void testSingleDocumentBitmap() {
    // Test with bitmap containing only one document
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(10);
    int numDocs = 25;
    BitmapDocIdIterator docIdIterator = BitmapDocIdIterator.create(bitmap, numDocs, true);

    assertEquals(docIdIterator.next(), 10);
    assertEquals(docIdIterator.next(), Constants.EOF);
    assertEquals(docIdIterator.advance(15), Constants.EOF);
  }

  @Test
  public void testSingleDocumentBitmapDesc() {
    // Test with bitmap containing only one document in descending mode
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(10);
    int numDocs = 25;
    BitmapDocIdIterator docIdIterator = BitmapDocIdIterator.create(bitmap, numDocs, false);

    assertEquals(docIdIterator.next(), 10);
    assertEquals(docIdIterator.next(), Constants.EOF);
    assertEquals(docIdIterator.advance(5), Constants.EOF);
  }

  @Test
  public void testDocIdsAtNumDocsBoundary() {
    // Test with document IDs at or beyond the numDocs boundary
    int[] docIds = new int[]{23, 24, 25, 26, 27}; // Some beyond numDocs=25
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(docIds);
    int numDocs = 25;
    BitmapDocIdIterator docIdIterator = BitmapDocIdIterator.create(bitmap, numDocs, true);

    // Should only return docIds < numDocs
    assertEquals(docIdIterator.next(), 23);
    assertEquals(docIdIterator.next(), 24);
    assertEquals(docIdIterator.next(), Constants.EOF);
  }

  @Test
  public void testDocIdsAtNumDocsBoundaryDesc() {
    // Test with document IDs at or beyond the numDocs boundary in descending mode
    int[] docIds = new int[]{23, 24, 25, 26, 27}; // Some beyond numDocs=25
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(docIds);
    int numDocs = 25;
    BitmapDocIdIterator docIdIterator = BitmapDocIdIterator.create(bitmap, numDocs, false);

    // Should only return docIds < numDocs, in descending order
    assertEquals(docIdIterator.next(), 24);
    assertEquals(docIdIterator.next(), 23);
    assertEquals(docIdIterator.next(), Constants.EOF);
  }

  @Test
  public void testAllDocIdsAboveNumDocs() {
    // Test with all document IDs above the numDocs limit
    int[] docIds = new int[]{30, 35, 40};
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(docIds);
    int numDocs = 25;
    BitmapDocIdIterator docIdIterator = BitmapDocIdIterator.create(bitmap, numDocs, true);

    assertEquals(docIdIterator.next(), Constants.EOF);
    assertEquals(docIdIterator.advance(10), Constants.EOF);
  }

  @Test
  public void testAllDocIdsAboveNumDocsDesc() {
    // Test with all document IDs above the numDocs limit in descending mode
    int[] docIds = new int[]{30, 35, 40};
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(docIds);
    int numDocs = 25;
    BitmapDocIdIterator docIdIterator = BitmapDocIdIterator.create(bitmap, numDocs, false);

    assertEquals(docIdIterator.next(), Constants.EOF);
    assertEquals(docIdIterator.advance(10), Constants.EOF);
  }

  @Test
  public void testAdvanceToDocumentZero() {
    // Test advancing to document ID 0
    int[] docIds = new int[]{0, 5, 10, 15};
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(docIds);
    int numDocs = 25;
    BitmapDocIdIterator docIdIterator = BitmapDocIdIterator.create(bitmap, numDocs, true);

    assertEquals(docIdIterator.advance(0), 0);
    assertEquals(docIdIterator.next(), 5);
  }

  @Test
  public void testAdvanceToDocumentZeroDesc() {
    // Test advancing to document ID 0 in descending mode
    int[] docIds = new int[]{0, 5, 10, 15};
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(docIds);
    int numDocs = 25;
    BitmapDocIdIterator docIdIterator = BitmapDocIdIterator.create(bitmap, numDocs, false);

    assertEquals(docIdIterator.advance(0), 0);
    assertEquals(docIdIterator.next(), Constants.EOF);
  }

  @Test
  public void testAdvanceToNegativeDocIdDesc() {
    // Test advancing to negative document ID in descending mode
    int[] docIds = new int[]{5, 10, 15};
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(docIds);
    int numDocs = 25;
    BitmapDocIdIterator docIdIterator = BitmapDocIdIterator.create(bitmap, numDocs, false);

    assertEquals(docIdIterator.advance(-1), Constants.EOF); // No docs <= -1
  }

  @Test
  public void testAdvanceBeyondLastDoc() {
    // Test advancing beyond the last document in ascending mode
    int[] docIds = new int[]{5, 10, 15};
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(docIds);
    int numDocs = 25;
    BitmapDocIdIterator docIdIterator = BitmapDocIdIterator.create(bitmap, numDocs, true);

    assertEquals(docIdIterator.advance(20), Constants.EOF);
  }

  @Test
  public void testAdvanceBeyondLastDocDesc() {
    // Test advancing beyond the last document in descending mode
    int[] docIds = new int[]{5, 10, 15};
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(docIds);
    int numDocs = 25;
    BitmapDocIdIterator docIdIterator = BitmapDocIdIterator.create(bitmap, numDocs, false);

    assertEquals(docIdIterator.advance(20), 15); // Should return highest doc <= 20
  }

  @Test
  public void testConsecutiveAdvanceCalls() {
    // Test multiple consecutive advance calls
    int[] docIds = new int[]{1, 5, 10, 15, 20};
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(docIds);
    int numDocs = 25;
    BitmapDocIdIterator docIdIterator = BitmapDocIdIterator.create(bitmap, numDocs, true);

    assertEquals(docIdIterator.advance(3), 5);
    assertEquals(docIdIterator.advance(7), 10);
    assertEquals(docIdIterator.advance(12), 15);
    assertEquals(docIdIterator.advance(18), 20);
    assertEquals(docIdIterator.advance(25), Constants.EOF);
  }

  @Test
  public void testConsecutiveAdvanceCallsDesc() {
    // Test multiple consecutive advance calls in descending mode
    int[] docIds = new int[]{1, 5, 10, 15, 20};
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(docIds);
    int numDocs = 25;
    BitmapDocIdIterator docIdIterator = BitmapDocIdIterator.create(bitmap, numDocs, false);

    assertEquals(docIdIterator.advance(18), 15);
    assertEquals(docIdIterator.advance(12), 10);
    assertEquals(docIdIterator.advance(7), 5);
    assertEquals(docIdIterator.advance(3), 1);
    assertEquals(docIdIterator.advance(0), Constants.EOF);
  }

  @Test
  public void testAdvanceToCurrentPosition() {
    // Test advancing to the current position
    int[] docIds = new int[]{5, 10, 15};
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(docIds);
    int numDocs = 25;
    BitmapDocIdIterator docIdIterator = BitmapDocIdIterator.create(bitmap, numDocs, true);

    assertEquals(docIdIterator.next(), 5);
    assertEquals(docIdIterator.advance(5), 10); // Should advance to next doc > 5
  }

  @Test
  public void testAdvanceToCurrentPositionDesc() {
    // Test advancing to the current position in descending mode
    int[] docIds = new int[]{5, 10, 15};
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(docIds);
    int numDocs = 25;
    BitmapDocIdIterator docIdIterator = BitmapDocIdIterator.create(bitmap, numDocs, false);

    assertEquals(docIdIterator.next(), 15);
    assertEquals(docIdIterator.advance(15), 10); // Should advance to next doc < 15
  }

  @Test
  public void testLargeBitmap() {
    // Test with a large number of documents
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    for (int i = 0; i < 10000; i += 2) { // Add even numbers
      bitmap.add(i);
    }
    int numDocs = 10000;
    BitmapDocIdIterator docIdIterator = BitmapDocIdIterator.create(bitmap, numDocs, true);

    assertEquals(docIdIterator.next(), 0);
    assertEquals(docIdIterator.next(), 2);
    assertEquals(docIdIterator.advance(100), 100);
    assertEquals(docIdIterator.advance(101), 102); // Should find next even number
  }

  @Test
  public void testLargeBitmapDesc() {
    // Test with a large number of documents in descending mode
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    for (int i = 0; i < 10000; i += 2) { // Add even numbers
      bitmap.add(i);
    }
    int numDocs = 10000;
    BitmapDocIdIterator docIdIterator = BitmapDocIdIterator.create(bitmap, numDocs, false);

    assertEquals(docIdIterator.next(), 9998);
    assertEquals(docIdIterator.next(), 9996);
    assertEquals(docIdIterator.advance(9000), 9000);
    assertEquals(docIdIterator.advance(8999), 8998); // Should find next even number <= 8999
  }

  @Test
  public void testExhaustionBehavior() {
    // Test behavior after iterator is exhausted
    int[] docIds = new int[]{5, 10};
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(docIds);
    int numDocs = 25;
    BitmapDocIdIterator docIdIterator = BitmapDocIdIterator.create(bitmap, numDocs, true);

    assertEquals(docIdIterator.next(), 5);
    assertEquals(docIdIterator.next(), 10);
    assertEquals(docIdIterator.next(), Constants.EOF);

    // After exhaustion, should continue returning EOF
    assertEquals(docIdIterator.next(), Constants.EOF);
    assertEquals(docIdIterator.advance(15), Constants.EOF);
  }

  @Test
  public void testExhaustionBehaviorDesc() {
    // Test behavior after iterator is exhausted in descending mode
    int[] docIds = new int[]{5, 10};
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(docIds);
    int numDocs = 25;
    BitmapDocIdIterator docIdIterator = BitmapDocIdIterator.create(bitmap, numDocs, false);

    assertEquals(docIdIterator.next(), 10);
    assertEquals(docIdIterator.next(), 5);
    assertEquals(docIdIterator.next(), Constants.EOF);

    // After exhaustion, should continue returning EOF
    assertEquals(docIdIterator.next(), Constants.EOF);
    assertEquals(docIdIterator.advance(3), Constants.EOF);
  }

  @Test
  public void testNumDocsZero() {
    // Test with numDocs = 0 (edge case)
    int[] docIds = new int[]{1, 2, 3};
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(docIds);
    int numDocs = 0;
    BitmapDocIdIterator docIdIterator = BitmapDocIdIterator.create(bitmap, numDocs, true);

    // No documents should be valid since all are >= numDocs
    assertEquals(docIdIterator.next(), Constants.EOF);
    assertEquals(docIdIterator.advance(1), Constants.EOF);
  }
}
