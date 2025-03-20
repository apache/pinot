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

import com.uber.h3core.H3Core;
import com.uber.h3core.util.LatLng;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.longs.LongSets;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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

  public static final H3Core H3_CORE;

  static {
    try {
      H3_CORE = H3Core.newInstance();
    } catch (IOException e) {
      throw new RuntimeException("Failed to instantiate H3 V3 instance", e);
    }
  }

  /**
   * Returns the H3 cells that is crossed by the line.
   */
  private static LongSet coverLineInH3(LineString lineString, int resolution) {
    Coordinate[] endPoints = lineString.getCoordinates();
    int numEndPoints = endPoints.length;
    if (numEndPoints == 0) {
      return LongSets.EMPTY_SET;
    }
    long previousCell = H3_CORE.latLngToCell(endPoints[0].y, endPoints[0].x, resolution);
    if (numEndPoints == 1) {
      return LongSets.singleton(previousCell);
    }
    LongSet coveringCells = new LongOpenHashSet();
    for (int i = 1; i < numEndPoints; i++) {
      long currentCell = H3_CORE.latLngToCell(endPoints[i].y, endPoints[i].x, resolution);
      coveringCells.addAll(H3_CORE.gridPathCells(previousCell, currentCell));
      previousCell = currentCell;
    }
    return coveringCells;
  }

  /**
   * Returns the H3 cells that is fully covered and potentially covered (excluding fully covered) by the polygon.
   */
  private static Pair<LongSet, LongSet> coverPolygonInH3(Polygon polygon, int resolution) {
    // TODO: this can be further optimized to use native H3 implementation. They have plan to support natively.
    // https://github.com/apache/pinot/issues/8547
    Coordinate[] coordinates = polygon.getExteriorRing().getCoordinates();
    List<LatLng> points = new ArrayList<>(coordinates.length);
    for (Coordinate coordinate : coordinates) {
      points.add(new LatLng(coordinate.y, coordinate.x));
    }
    List<Long> polyfillCells = H3_CORE.polygonToCells(points, List.of(), resolution);
    if (polyfillCells.isEmpty()) {
      // If the polyfill cells are empty, meaning the polygon might be smaller than a single cell in the H3 system.
      // So just get whatever one. here choose the first one. the follow up kRing(firstCell, 1) will cover the whole
      // polygon if there is potential not covered by the first point's belonging cell.
      // ref: https://github.com/uber/h3/issues/456#issuecomment-827760163
      Coordinate represent = polygon.getCoordinate();
      return Pair.of(LongSets.EMPTY_SET,
          new LongOpenHashSet(H3_CORE.gridDisk(H3_CORE.latLngToCell(represent.y, represent.x, resolution), 1)));
    }

    LongSet fullyCoveredCells = new LongOpenHashSet();
    LongSet potentiallyCoveredCells = new LongOpenHashSet(polyfillCells);
    for (long cell : polyfillCells) {
      if (polygon.contains(createPolygonFromH3Cell(cell))) {
        fullyCoveredCells.add(cell);
      }
      potentiallyCoveredCells.addAll(H3_CORE.gridDisk(cell, 1));
    }
    potentiallyCoveredCells.removeAll(fullyCoveredCells);
    return Pair.of(fullyCoveredCells, potentiallyCoveredCells);
  }

  private static Polygon createPolygonFromH3Cell(long h3Cell) {
    List<LatLng> boundary = H3_CORE.cellToBoundary(h3Cell);
    int numVertices = boundary.size();
    Coordinate[] coordinates = new Coordinate[numVertices + 1];
    for (int i = 0; i < numVertices; i++) {
      LatLng vertex = boundary.get(i);
      coordinates[i] = new Coordinate(vertex.lng, vertex.lat);
    }
    coordinates[numVertices] = coordinates[0];
    return GeometryUtils.GEOMETRY_FACTORY.createPolygon(coordinates);
  }

  /**
   * Returns the H3 cells that is fully covered and potentially covered (excluding fully covered) by the geometry.
   */
  public static Pair<LongSet, LongSet> coverGeometryInH3(Geometry geometry, int resolution) {
    if (geometry instanceof Point) {
      return Pair.of(LongSets.EMPTY_SET,
          LongSets.singleton(H3_CORE.latLngToCell(geometry.getCoordinate().y, geometry.getCoordinate().x, resolution)));
    } else if (geometry instanceof LineString) {
      return Pair.of(LongSets.EMPTY_SET, coverLineInH3(((LineString) geometry), resolution));
    } else if (geometry instanceof Polygon) {
      return coverPolygonInH3(((Polygon) geometry), resolution);
    } else if (geometry instanceof GeometryCollection) {
      LongOpenHashSet fullyCoveredCells = new LongOpenHashSet();
      LongOpenHashSet potentiallyCoveredCells = new LongOpenHashSet();
      int numGeometries = geometry.getNumGeometries();
      for (int i = 0; i < numGeometries; i++) {
        Pair<LongSet, LongSet> pair = coverGeometryInH3(geometry.getGeometryN(i), resolution);
        fullyCoveredCells.addAll(pair.getLeft());
        potentiallyCoveredCells.addAll(pair.getRight());
      }
      potentiallyCoveredCells.removeAll(fullyCoveredCells);
      return Pair.of(fullyCoveredCells, potentiallyCoveredCells);
    } else {
      throw new UnsupportedOperationException("Unexpected type: " + geometry.getGeometryType());
    }
  }
}
