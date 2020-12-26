package org.apache.pinot.core.segment.creator.impl.geospatial;

import java.util.ArrayList;
import java.util.List;


/**
 * Stores the resolutions for an index. There are in total of H3 resolutions https://h3geo.org/#/documentation/core-library/resolution-table
 * To efficiently serialize the resolutions, we use two bytes for encoding th enabled resolutions. The resolution level
 * maps to the corresponding bit.
 */
public class H3IndexResolution {
  private short _resolutions;

  public H3IndexResolution(List<Integer> resolutions) {
    for (int resolution : resolutions) {
      _resolutions |= 1 << resolution;
    }
  }

  /**
   * Creates the resolutions with the serialized short value
   * @param resolutions
   */
  public H3IndexResolution(short resolutions) {
    _resolutions = resolutions;
  }

  /**
   * @return the encoding of the resolutions into a short value (two bytes)
   */
  public short serialize() {
    return _resolutions;
  }

  public int size() {
    return Integer.bitCount(_resolutions);
  }

  public List<Integer> getResolutions() {
    List<Integer> res = new ArrayList<>();
    for (int i = 0; i < 15; i++) {
      if ((_resolutions & (1 << i)) != 0) {
        res.add(i);
      }
    }
    return res;
  }

  public int getLowestResolution() {
    return Integer.numberOfTrailingZeros(_resolutions);
  }
}
