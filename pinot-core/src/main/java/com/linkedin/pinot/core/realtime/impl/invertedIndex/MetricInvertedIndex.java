package com.linkedin.pinot.core.realtime.impl.invertedIndex;

import java.util.HashMap;
import java.util.Map;

import org.roaringbitmap.buffer.MutableRoaringBitmap;

public class MetricInvertedIndex implements RealtimeInvertedIndex {

  Map<Object, MutableRoaringBitmap> invertedIndex;

  public MetricInvertedIndex(String columnName) {
    invertedIndex = new HashMap<Object, MutableRoaringBitmap>();
  }

  @Override
  public void add(Object id, int docId) {
    if (!invertedIndex.containsKey(id)) {
      invertedIndex.put(id, new MutableRoaringBitmap());
    }
    invertedIndex.get(id).add(docId);
  }

  @Override
  public MutableRoaringBitmap getDocIdSetFor(Object dicId) {
    return invertedIndex.get(dicId);
  }

}
