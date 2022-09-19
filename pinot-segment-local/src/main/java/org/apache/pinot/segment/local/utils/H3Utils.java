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
package org.apache.pinot.segment.local.utils;

import com.uber.h3core.H3CoreV3;
import com.uber.h3core.util.LatLng;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;


public class H3Utils {

  private H3Utils() {
  }

  public static final H3CoreV3 H3_CORE;

  static {
    try {
      H3_CORE = H3CoreV3.newInstance();
    } catch (IOException e) {
      throw new RuntimeException("Failed to instantiate H3 V3 instance", e);
    }
  }

  private static LongSet coverLineInH3(LineString lineString, int resolution) {
    LongSet coveringH3Cells = new LongOpenHashSet();
    LongList endpointH3Cells = new LongArrayList();
    for (Coordinate endpoint : lineString.getCoordinates()) {
      endpointH3Cells.add(H3_CORE.geoToH3(endpoint.y, endpoint.x, resolution));
    }
    for (int i = 0; i < endpointH3Cells.size() - 1; i++) {
      try {
        coveringH3Cells.addAll(H3_CORE.h3Line(endpointH3Cells.getLong(i), endpointH3Cells.getLong(i + 1)));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return coveringH3Cells;
  }

  private static Pair<LongSet, LongSet> coverPolygonInH3(Polygon polygon, int resolution) {
    List<Long> polyfillCells = H3_CORE.polyfill(Arrays.stream(polygon.getExteriorRing().getCoordinates())
            .map(coordinate -> new LatLng(coordinate.y, coordinate.x)).collect(Collectors.toList()),
        Collections.emptyList(), resolution);
    // TODO: this can be further optimized to use native H3 implementation. They have plan to support natively.
    // https://github.com/apache/pinot/issues/8547
    LongSet potentialH3Cells = new LongOpenHashSet();
    if (polyfillCells.isEmpty()) {
      // If the polyfill cells are empty, meaning the polygon might be smaller than a single cell in the H3 system.
      // So just get whatever one. here choose the first one. the follow up kRing(firstCell, 1) will cover the whole
      // polygon if there is potential not covered by the first point's belonging cell.
      // ref: https://github.com/uber/h3/issues/456#issuecomment-827760163
      Coordinate represent = polygon.getCoordinate();
      potentialH3Cells.add(H3_CORE.geoToH3(represent.getY(), represent.getX(), resolution));
    } else {
      potentialH3Cells.addAll(polyfillCells);
    }
    LongSet fullyContainedCell = new LongOpenHashSet(
        potentialH3Cells.stream().filter(h3Cell -> polygon.contains(createPolygonFromH3Cell(h3Cell)))
            .collect(Collectors.toSet()));
    potentialH3Cells
        .addAll(potentialH3Cells.stream().flatMap(cell -> H3_CORE.kRing(cell, 1).stream()).collect(Collectors.toSet()));
    return Pair.of(fullyContainedCell, potentialH3Cells);
  }

  private static Polygon createPolygonFromH3Cell(long h3Cell) {
    List<LatLng> boundary = H3_CORE.h3ToGeoBoundary(h3Cell);
    boundary.add(boundary.get(0));
    return GeometryUtils.GEOMETRY_FACTORY.createPolygon(
        boundary.stream().map(geoCoord -> new Coordinate(geoCoord.lng, geoCoord.lat)).toArray(Coordinate[]::new));
  }

  // Return a pair of cell ids: The first fully contain, the second is potential contain.
  // potential contains contain the fully contain.
  public static Pair<LongSet, LongSet> coverGeometryInH3(Geometry geometry, int resolution) {
    if (geometry instanceof Point) {
      LongSet potentialCover = new LongOpenHashSet();
      potentialCover.add(H3_CORE.geoToH3(geometry.getCoordinate().y, geometry.getCoordinate().x, resolution));
      return Pair.of(new LongOpenHashSet(), potentialCover);
    } else if (geometry instanceof LineString) {
      LongSet potentialCover = new LongOpenHashSet();
      potentialCover.addAll(coverLineInH3(((LineString) geometry), resolution));
      return Pair.of(new LongOpenHashSet(), potentialCover);
    } else if (geometry instanceof Polygon) {
      return coverPolygonInH3(((Polygon) geometry), resolution);
    } else if (geometry instanceof GeometryCollection) {
      LongOpenHashSet fullCover = new LongOpenHashSet();
      LongOpenHashSet potentialCover = new LongOpenHashSet();
      for (int i = 0; i < geometry.getNumGeometries(); i++) {
        fullCover.addAll(coverGeometryInH3(geometry.getGeometryN(i), resolution).getLeft());
        potentialCover.addAll(coverGeometryInH3(geometry.getGeometryN(i), resolution).getRight());
      }
      return Pair.of(fullCover, potentialCover);
    } else {
      throw new UnsupportedOperationException("Unexpected type: " + geometry.getGeometryType());
    }
  }
}
