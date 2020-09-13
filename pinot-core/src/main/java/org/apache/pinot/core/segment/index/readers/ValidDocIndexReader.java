package org.apache.pinot.core.segment.index.readers;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


public interface ValidDocIndexReader {

  /**
   * Return the underlying validDoc bitmap (used in query execution)
   */
  ImmutableRoaringBitmap getValidDocBitmap();
}
