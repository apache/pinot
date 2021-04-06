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
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class BitmapDocIdIteratorTest {

  @Test
  public void testBitmapDocIdIterator() {
    int[] docIds = new int[]{1, 2, 4, 5, 6, 8, 12, 15, 16, 18, 20, 21};
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(docIds);
    int numDocs = 25;
    BitmapDocIdIterator docIdIterator = new BitmapDocIdIterator(bitmap, numDocs);
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
}
