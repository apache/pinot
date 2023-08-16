package org.apache.pinot.segment.spi.index.reader;

import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.spi.data.readers.Vector;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


public interface VectorIndexReader extends IndexReader {
  /**
   * Gets the matching Doc IDs of the given a Vector object
   * @param vector Vector object
   * @return the matched DocIDs
   */
  ImmutableRoaringBitmap getDocIds(Vector vector);
}
