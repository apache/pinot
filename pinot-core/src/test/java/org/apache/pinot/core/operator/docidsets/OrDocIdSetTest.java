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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.operator.dociditerators.BitmapDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.OrDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.SVScanDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.SortedDocIdIterator;
import org.apache.pinot.spi.utils.Pairs;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class OrDocIdSetTest {
    @Test
    public void iteratorReturnsBitmapDocIdIteratorWhenOnlyIndexBasedIteratorsExist() {
        // All the idsets are index-base BlockDocIdIterator (SortedDocIdIterator or BitmapBasedDocIdIterator)
        SortedDocIdSet sortedDocIdSet = mock(SortedDocIdSet.class);
        SortedDocIdIterator sortedIterator = mock(SortedDocIdIterator.class);
        BitmapDocIdSet bitmapDocIdSet = mock(BitmapDocIdSet.class);
        BitmapDocIdIterator bitmapIterator = mock(BitmapDocIdIterator.class);

        when(sortedDocIdSet.iterator()).thenReturn(sortedIterator);
        when(bitmapDocIdSet.iterator()).thenReturn(bitmapIterator);
        when(sortedIterator.getDocIdRanges()).thenReturn(Collections.singletonList(new Pairs.IntPair(1, 10)));
        when(bitmapIterator.getDocIds()).thenReturn(new MutableRoaringBitmap());

        List<BlockDocIdSet> docIdSets = Arrays.asList(sortedDocIdSet, bitmapDocIdSet);
        OrDocIdSet orDocIdSet = new OrDocIdSet(docIdSets, 100);
        BlockDocIdIterator iterator = orDocIdSet.iterator();
        // Make sure the returned iterator is a BitmapDocIdIterator instead of an OrDocIdIterator.
        assertTrue(iterator instanceof BitmapDocIdIterator);
        assertFalse(iterator instanceof OrDocIdIterator);
    }

    @Test
    public void iteratorReturnsBitmapDocIdIteratorWhenBitmapBasedDocIdIterators() {
        BitmapDocIdSet bitmapDocIdSet1 = mock(BitmapDocIdSet.class);
        BitmapDocIdSet bitmapDocIdSet2 = mock(BitmapDocIdSet.class);
        BitmapDocIdIterator bitmapIterator1 = mock(BitmapDocIdIterator.class);
        BitmapDocIdIterator bitmapIterator2 = mock(BitmapDocIdIterator.class);

        when(bitmapDocIdSet1.iterator()).thenReturn(bitmapIterator1);
        when(bitmapDocIdSet2.iterator()).thenReturn(bitmapIterator2);
        when(bitmapIterator1.getDocIds()).thenReturn(new MutableRoaringBitmap());
        when(bitmapIterator2.getDocIds()).thenReturn(new MutableRoaringBitmap());

        List<BlockDocIdSet> docIdSets = Arrays.asList(bitmapDocIdSet1, bitmapDocIdSet2);
        OrDocIdSet orDocIdSet = new OrDocIdSet(docIdSets, 100);
        BlockDocIdIterator iterator = orDocIdSet.iterator();
        assertTrue(iterator instanceof BitmapDocIdIterator);
        assertFalse(iterator instanceof OrDocIdIterator);
    }

    @Test
    public void iteratorReturnsBitmapDocIdIteratorWhenBitmapAndSortedDocIdIterators() {
        SortedDocIdSet sortedDocIdSet = mock(SortedDocIdSet.class);
        BitmapDocIdSet bitmapDocIdSet = mock(BitmapDocIdSet.class);
        SortedDocIdIterator sortedIterator = mock(SortedDocIdIterator.class);
        BitmapDocIdIterator bitmapIterator = mock(BitmapDocIdIterator.class);

        when(sortedDocIdSet.iterator()).thenReturn(sortedIterator);
        when(bitmapDocIdSet.iterator()).thenReturn(bitmapIterator);
        when(sortedIterator.getDocIdRanges()).thenReturn(Collections.singletonList(new Pairs.IntPair(1, 5)));
        when(bitmapIterator.getDocIds()).thenReturn(new MutableRoaringBitmap());

        List<BlockDocIdSet> docIdSets = Arrays.asList(sortedDocIdSet, bitmapDocIdSet);
        OrDocIdSet orDocIdSet = new OrDocIdSet(docIdSets, 100);
        BlockDocIdIterator iterator = orDocIdSet.iterator();
        assertTrue(iterator instanceof BitmapDocIdIterator);
    }

    @Test
    public void iteratorReturnsOrDocIdIteratorWhenAtLeastOneScanBasedExist() {
        BitmapDocIdSet bitmapDocIdSet = mock(BitmapDocIdSet.class);
        SVScanDocIdSet svScanDocIdSet = mock(SVScanDocIdSet.class);
        BitmapDocIdIterator bitmapIterator = mock(BitmapDocIdIterator.class);
        SVScanDocIdIterator svScanDocIdIterator = mock(SVScanDocIdIterator.class);

        when(bitmapDocIdSet.iterator()).thenReturn(bitmapIterator);
        when(svScanDocIdSet.iterator()).thenReturn(svScanDocIdIterator);

        when(bitmapIterator.getDocIds()).thenReturn(new MutableRoaringBitmap());

        List<BlockDocIdSet> docIdSets = Arrays.asList(bitmapDocIdSet, svScanDocIdSet);
        OrDocIdSet orDocIdSet = new OrDocIdSet(docIdSets, 100);
        BlockDocIdIterator iterator = orDocIdSet.iterator();
        assertTrue(iterator instanceof OrDocIdIterator);
    }

    @Test
    public void iteratorReturnsOrDocIdIteratorWhenAtLeastOneScanBasedExist2() {
        BitmapDocIdSet bitmapDocIdSet = mock(BitmapDocIdSet.class);
        SortedDocIdSet sortedDocIdIterator = mock(SortedDocIdSet.class);
        SVScanDocIdSet svScanDocIdSet = mock(SVScanDocIdSet.class);
        SortedDocIdIterator sortedIterator = mock(SortedDocIdIterator.class);
        BitmapDocIdIterator bitmapIterator = mock(BitmapDocIdIterator.class);
        SVScanDocIdIterator svScanDocIdIterator = mock(SVScanDocIdIterator.class);

        when(bitmapDocIdSet.iterator()).thenReturn(bitmapIterator);
        when(sortedDocIdIterator.iterator()).thenReturn(sortedIterator);
        when(svScanDocIdSet.iterator()).thenReturn(svScanDocIdIterator);
        when(sortedIterator.getDocIdRanges()).thenReturn(Collections.singletonList(new Pairs.IntPair(1, 5)));
        when(bitmapIterator.getDocIds()).thenReturn(new MutableRoaringBitmap());

        List<BlockDocIdSet> docIdSets = Arrays.asList(bitmapDocIdSet, sortedDocIdIterator, svScanDocIdSet);
        OrDocIdSet orDocIdSet = new OrDocIdSet(docIdSets, 100);
        BlockDocIdIterator iterator = orDocIdSet.iterator();
        assertTrue(iterator instanceof OrDocIdIterator);
    }
}
