/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.local.realtime.impl.geospatial;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.utils.GeometrySerializer;
import org.apache.pinot.segment.local.utils.H3Utils;
import org.apache.pinot.segment.spi.index.mutable.MutableIndex;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.index.reader.H3IndexReader;
import org.apache.pinot.segment.spi.index.reader.H3IndexResolution;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * A H3 index reader for the real-time H3 index values on the fly.
 * <p>This class is thread-safe for single writer multiple readers.
 */
public class MutableH3Index implements H3IndexReader, MutableIndex {
  private final H3IndexResolution _resolution;
  private final int _lowestResolution;
  private final Map<Long, ThreadSafeMutableRoaringBitmap> _bitmaps = new ConcurrentHashMap<>();

  private int _nextDocId;

  public MutableH3Index(H3IndexResolution resolution)
      throws IOException {
    _resolution = resolution;
    _lowestResolution = resolution.getLowestResolution();
  }

  @Override
  public void add(@Nonnull Object value, int dictId, int docId) {
    try {
      Geometry geometry = GeometrySerializer.deserialize((byte[]) value);
      add(geometry);
    } finally {
      _nextDocId++;
    }
  }

  @Override
  public void add(@Nonnull Object[] values, @Nullable int[] dictIds, int docId) {
    throw new UnsupportedOperationException("Mutable H3 indexes are not supported for multi-valued columns");
  }

  /**
   * Adds the next geospatial value.
   */
  public void add(Geometry geometry) {
    Preconditions.checkState(geometry instanceof Point, "H3 index can only be applied to Point, got: %s",
        geometry.getGeometryType());
    Coordinate coordinate = geometry.getCoordinate();
    // TODO: support multiple resolutions
    long h3Id = H3Utils.H3_CORE.geoToH3(coordinate.y, coordinate.x, _lowestResolution);
    _bitmaps.computeIfAbsent(h3Id, k -> new ThreadSafeMutableRoaringBitmap()).add(_nextDocId);
  }

  @Override
  public MutableRoaringBitmap getDocIds(long h3Id) {
    ThreadSafeMutableRoaringBitmap bitmap = _bitmaps.get(h3Id);
    return bitmap != null ? bitmap.getMutableRoaringBitmap() : new MutableRoaringBitmap();
  }

  @Override
  public H3IndexResolution getH3IndexResolution() {
    return _resolution;
  }

  @Override
  public void close() {
  }
}
