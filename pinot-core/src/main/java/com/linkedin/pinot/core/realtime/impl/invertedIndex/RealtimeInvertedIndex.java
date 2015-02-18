package com.linkedin.pinot.core.realtime.impl.invertedIndex;

import org.roaringbitmap.buffer.MutableRoaringBitmap;

public interface RealtimeInvertedIndex {
  public void add(Object dictId, int docId);

  public MutableRoaringBitmap getDocIdSetFor (Object dicId);

}
