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

import org.apache.pinot.query.spi.data.BlockDocIdIterator;
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
    OrDocIdIterator andDocIdIterator = new OrDocIdIterator(new BlockDocIdIterator[]{
        new RangelessBitmapDocIdIterator(bitmap1), new RangelessBitmapDocIdIterator(bitmap2),
        new RangelessBitmapDocIdIterator(bitmap3)
    });

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
}
