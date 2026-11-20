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
package org.apache.pinot.core.operator.filter;

import java.util.List;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Tests [BitmapBasedFilterOperator] over bitmaps read from a consuming segment, which can hold documents ingested
/// after numDocs was read at planning time. Those documents are left out of the matches, the count and the bitmaps, on
/// both sides of exclusive, and the given bitmaps are not modified.
public class BitmapBasedFilterOperatorTest {
  private static final int NUM_DOCS = 10;

  @Test
  public void testDocsAtOrAboveNumDocsAreIgnored() {
    ImmutableRoaringBitmap docIds = bitmap(1, 5, NUM_DOCS, NUM_DOCS + 2);

    BitmapBasedFilterOperator operator = new BitmapBasedFilterOperator(docIds, false, NUM_DOCS);
    assertEquals(TestUtils.getDocIds(operator.nextBlock().getBlockDocIdSet()), List.of(1, 5));
    assertEquals(operator.getNumMatchingDocs(), 2);
    assertEquals(operator.getBitmaps().reduce().toArray(), new int[]{1, 5});

    operator = new BitmapBasedFilterOperator(docIds, true, NUM_DOCS);
    assertEquals(TestUtils.getDocIds(operator.nextBlock().getBlockDocIdSet()), List.of(0, 2, 3, 4, 6, 7, 8, 9));
    assertEquals(operator.getNumMatchingDocs(), 8);
    assertEquals(operator.getBitmaps().reduce().toArray(), new int[]{0, 2, 3, 4, 6, 7, 8, 9});

    assertEquals(docIds.toArray(), new int[]{1, 5, NUM_DOCS, NUM_DOCS + 2});
  }

  // A predicate that is true on every non-null document, as built for an always true leaf under null handling
  @Test
  public void testNullDocsAtOrAboveNumDocsAreIgnored() {
    ImmutableRoaringBitmap nullBitmap = bitmap(2, NUM_DOCS + 1);
    BitmapBasedFilterOperator operator = new BitmapBasedFilterOperator(nullBitmap, true, NUM_DOCS, nullBitmap);
    assertEquals(TestUtils.getDocIds(operator.nextBlock().getBlockDocIdSet()), List.of(0, 1, 3, 4, 5, 6, 7, 8, 9));
    assertEquals(operator.getNumMatchingDocs(), 9);
    assertEquals(operator.getBitmaps().reduce().toArray(), new int[]{0, 1, 3, 4, 5, 6, 7, 8, 9});

    // NOT of UNKNOWN is UNKNOWN, so the negation is true on no document
    NotFilterOperator notOperator = new NotFilterOperator(operator, NUM_DOCS, true);
    assertEquals(notOperator.getNumMatchingDocs(), 0);
    assertEquals(notOperator.getBitmaps().getCardinality(), 0);

    assertEquals(nullBitmap.toArray(), new int[]{2, NUM_DOCS + 1});
  }

  private static ImmutableRoaringBitmap bitmap(int... docIds) {
    return MutableRoaringBitmap.bitmapOf(docIds);
  }
}
