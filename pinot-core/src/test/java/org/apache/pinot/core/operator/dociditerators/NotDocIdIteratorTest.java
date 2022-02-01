package org.apache.pinot.core.operator.dociditerators;

import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.segment.spi.Constants;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class NotDocIdIteratorTest {
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
    NotDocIdIterator notDocIdIterator = new NotDocIdIterator(new RangelessBitmapDocIdIterator(bitmap1),
        25);

    assertEquals(notDocIdIterator.advance(1), 2);
    assertEquals(notDocIdIterator.next(), 3);
    assertEquals(notDocIdIterator.next(), 5);
    assertEquals(notDocIdIterator.advance(7), 8);
    assertEquals(notDocIdIterator.advance(13), 14);
    assertEquals(notDocIdIterator.next(), 16);
    assertEquals(notDocIdIterator.advance(18), 19);
    //assertEquals(notDocIdIterator.next(), 19);
    assertEquals(notDocIdIterator.advance(21), 22);
    assertEquals(notDocIdIterator.advance(26), 19);
  }
}
