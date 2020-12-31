package org.apache.pinot.core.realtime.impl.geospatial;

import com.uber.h3core.H3Core;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.core.realtime.impl.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.core.segment.creator.GeoSpatialIndexCreator;
import org.apache.pinot.core.segment.creator.impl.geospatial.H3IndexResolution;
import org.apache.pinot.core.segment.index.readers.H3IndexReader;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * A H3 index reader for the real-time H3 index values on the fly.
 * <p>This class is thread-safe for single writer multiple readers.
 */
public class RealtimeH3IndexReader implements GeoSpatialIndexCreator, H3IndexReader {
  private final H3Core _h3Core;
  private final Map<Long, ThreadSafeMutableRoaringBitmap> _h3IndexMap = new ConcurrentHashMap<>();
  private final H3IndexResolution _resolution;
  private int _lowestResolution;

  public RealtimeH3IndexReader(List<Integer> resolutions)
      throws IOException {
    this(new H3IndexResolution(resolutions));
  }

  public RealtimeH3IndexReader(H3IndexResolution resolution)
      throws IOException {
    _resolution = resolution;
    _lowestResolution = resolution.getLowestResolution();
    _h3Core = H3Core.newInstance();
  }

  @Override
  public void add(int docId, double lat, double lon) {
    // TODO support multiple resolutions
    Long h3Id = _h3Core.geoToH3(lat, lon, _lowestResolution);
    _h3IndexMap.computeIfAbsent(h3Id, k -> new ThreadSafeMutableRoaringBitmap());
    synchronized (this) {
      _h3IndexMap.get(h3Id).add(docId);
    }
  }

  @Override
  public ImmutableRoaringBitmap getDocIds(long h3IndexId) {
    return _h3IndexMap.containsKey(h3IndexId) ? _h3IndexMap.get(h3IndexId).getMutableRoaringBitmap()
        : new MutableRoaringBitmap();
  }

  @Override
  public H3IndexResolution getH3IndexResolution() {
    return _resolution;
  }

  @Override
  public void close()
      throws IOException {
  }
}
