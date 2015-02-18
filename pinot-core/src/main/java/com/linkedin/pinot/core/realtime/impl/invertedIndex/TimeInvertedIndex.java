package com.linkedin.pinot.core.realtime.impl.invertedIndex;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.google.common.collect.Sets;

public class TimeInvertedIndex implements RealtimeInvertedIndex {

  private Map<Long, MutableRoaringBitmap> invertedIndex;

  public TimeInvertedIndex(String columnName) {
    invertedIndex = new HashMap<Long, MutableRoaringBitmap>();
  }

  @Override
  public void add(Object id, int docId) {
    if (!invertedIndex.containsKey(id)) {
      invertedIndex.put((Long) id, new MutableRoaringBitmap());
    }
    invertedIndex.get((Long) id).add(docId);
  }

  @Override
  public MutableRoaringBitmap getDocIdSetFor(Object dicId) {
    return invertedIndex.get(dicId);
  }
}
