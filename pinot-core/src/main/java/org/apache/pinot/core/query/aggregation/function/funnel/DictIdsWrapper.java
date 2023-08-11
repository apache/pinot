package org.apache.pinot.core.query.aggregation.function.funnel;

import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.roaringbitmap.RoaringBitmap;


final class DictIdsWrapper {
  final Dictionary _dictionary;
  final RoaringBitmap[] _stepsBitmaps;

  DictIdsWrapper(int numSteps, Dictionary dictionary) {
    _dictionary = dictionary;
    _stepsBitmaps = new RoaringBitmap[numSteps];
    for (int n = 0; n < numSteps; n++) {
      _stepsBitmaps[n] = new RoaringBitmap();
    }
  }
}
