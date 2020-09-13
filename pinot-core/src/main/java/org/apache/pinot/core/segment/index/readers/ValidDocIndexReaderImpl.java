package org.apache.pinot.core.segment.index.readers;

import org.apache.pinot.core.realtime.impl.ThreadSafeMutableRoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


public class ValidDocIndexReaderImpl implements ValidDocIndexReader {
  private final ThreadSafeMutableRoaringBitmap _validDocBitmap;

  public ValidDocIndexReaderImpl(ThreadSafeMutableRoaringBitmap validDocBitmap) {
    _validDocBitmap = validDocBitmap;
  }

  @Override
  public ImmutableRoaringBitmap getValidDocBitmap() {
    return _validDocBitmap.getMutableRoaringBitmap();
  }
}
