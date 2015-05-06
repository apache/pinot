/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.realtime.impl.invertedIndex;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class TimeInvertedIndex implements RealtimeInvertedIndex {

  private Map<Object, MutableRoaringBitmap> invertedIndex;

  public TimeInvertedIndex(String columnName) {
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

  @Override
  public ImmutableRoaringBitmap getImmutable(int idx) {
    throw new UnsupportedOperationException("");
  }

  @Override
  public int[] getMinMaxRangeFor(int docId) {
    throw new UnsupportedOperationException("");
  }

  @Override
  public void close() throws IOException {
  }
}
