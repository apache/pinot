package org.apache.pinot.core.segment.creator;

import java.io.Closeable;


public interface GeoSpatialIndexCreator extends Closeable {

  void add(int docId, double lat, double lon);
}
