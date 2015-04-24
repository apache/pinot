package com.linkedin.pinot.core.segment.index;

import java.io.IOException;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


public interface InvertedIndexReader {

  /**
   * Returns the immutable bitmap at the specified index.
   * @param idx the index
   * @return the immutable bitmap at the specified index.
   */
  public ImmutableRoaringBitmap getImmutable(int idx);

  /**
   *
   * @param docId
   * @return
   */
  public int[] getMinMaxRangeFor(int docId);

  public void close() throws IOException;
}
