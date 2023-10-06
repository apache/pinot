package org.apache.pinot.segment.spi.index.reader;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


public interface DictionaryIdBasedBitmapProvider {
  ImmutableRoaringBitmap getDocIds(int dictId);
}
