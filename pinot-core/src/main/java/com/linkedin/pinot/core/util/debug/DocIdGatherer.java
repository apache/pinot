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
package com.linkedin.pinot.core.util.debug;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/**
 * Debug class to display docId ranges ([1;5], 7 instead of {1,2,3,4,5,7}), for example
 * <pre><code>System.out.println(DocIdGatherer.from(docIdList))</code></pre>. Assumes that the doc ids are in ascending
 * order.
 */
public class DocIdGatherer {
  private class DocIdRange {
    private int _start;
    private int _end;

    public DocIdRange(int docId) {
      _start = docId;
      _end = docId;
    }

    public DocIdRange(int start, int end) {
      _start = start;
      _end = end;
    }

    public boolean isContainedOrAdjacent(int value) {
      return _start - 1 <= value && value <= _end + 1;
    }

    public void updateWithDocId(int value) {
      if (!isContainedOrAdjacent(value)) {
        throw new IllegalArgumentException("Value " + value + " not contained or adjacent to range " + this.toString());
      }

      _start = Math.min(_start, value);
      _end = Math.max(_end, value);
    }

    @Override
    public String toString() {
      if (_start != _end) {
        return "[" + _start + ";" + _end + "]";
      } else {
        return Integer.toString(_start);
      }
    }
  }

  private List<DocIdRange> _docIdRangeList = new ArrayList<>();

  public void addDocId(int docId) {
    if (_docIdRangeList.isEmpty()) {
      _docIdRangeList.add(new DocIdRange(docId));
    } else {
      DocIdRange lastRange = _docIdRangeList.get(_docIdRangeList.size() - 1);
      if (lastRange.isContainedOrAdjacent(docId)) {
        lastRange.updateWithDocId(docId);
      } else {
        _docIdRangeList.add(new DocIdRange(docId));
      }
    }
  }

  @Override
  public String toString() {
    return Arrays.toString(_docIdRangeList.toArray());
  }

  public static DocIdGatherer from(ImmutableRoaringBitmap roaringBitmap) {
    DocIdGatherer gatherer = new DocIdGatherer();
    IntIterator intIterator = roaringBitmap.getIntIterator();
    while(intIterator.hasNext()) {
      gatherer.addDocId(intIterator.next());
    }
    return gatherer;
  }

  public static DocIdGatherer from(List<Integer> listOfInts) {
    DocIdGatherer gatherer = new DocIdGatherer();
    for (Integer anInt : listOfInts) {
      gatherer.addDocId(anInt);
    }
    return gatherer;
  }

  public static DocIdGatherer from(int[] ints) {
    DocIdGatherer gatherer = new DocIdGatherer();
    for (int anInt : ints) {
      gatherer.addDocId(anInt);
    }
    return gatherer;

  }
}
