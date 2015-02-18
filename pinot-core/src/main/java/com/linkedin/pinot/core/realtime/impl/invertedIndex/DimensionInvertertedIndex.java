package com.linkedin.pinot.core.realtime.impl.invertedIndex;

import java.util.HashMap;
import java.util.Map;

import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class DimensionInvertertedIndex implements RealtimeInvertedIndex {

  private Map<Integer, MutableRoaringBitmap> invertedIndex;

  public DimensionInvertertedIndex(String columnName) {
    invertedIndex = new HashMap<Integer, MutableRoaringBitmap>();
  }

  @Override
  public void add(Object dictId, int docId) {
    if (!invertedIndex.containsKey(dictId))
      invertedIndex.put((Integer) dictId, new MutableRoaringBitmap());

    invertedIndex.get((Integer) dictId).add(docId);
  }

  @Override
  public MutableRoaringBitmap getDocIdSetFor(Object dicId) {
    return invertedIndex.get(dicId);
  }

}
