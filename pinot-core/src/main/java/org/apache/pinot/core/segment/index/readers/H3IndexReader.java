package org.apache.pinot.core.segment.index.readers;

import org.apache.pinot.core.segment.creator.impl.geospatial.H3IndexResolution;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/**
 * Reader of the H3 index.
 */
public interface H3IndexReader {
  /**
   * Gets the matching Doc IDs of the given H3 index ID as bitmaps.
   * @param h3IndexId the H3 index ID to match
   * @return the matched DocIDs
   */
  ImmutableRoaringBitmap getDocIds(long h3IndexId);

  /**
   * @return the H3 index resolutions
   */
  H3IndexResolution getH3IndexResolution();
}
