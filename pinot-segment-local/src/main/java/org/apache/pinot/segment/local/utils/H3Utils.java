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

import com.google.common.collect.ImmutableList;
import com.uber.h3core.H3Core;
import com.uber.h3core.exceptions.LineUndefinedException;
import com.uber.h3core.util.GeoCoord;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
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
      throw new RuntimeException("Failed to instantiate H3 instance", e);
    }
  }

  private static Set<Long> coverLineInH3(LineString lineString, int resolution) {
    Set<Long> coveringH3Cells = new HashSet<>();
    List<Long> endpointH3Cells = new ArrayList<>();
    for (Coordinate endpoint : lineString.getCoordinates()) {
      endpointH3Cells.add(H3_CORE.geoToH3(endpoint.y, endpoint.x, resolution));
    }
    for (int i = 0; i < endpointH3Cells.size() - 1; i++) {
      try {
        coveringH3Cells.addAll(H3_CORE.h3Line(endpointH3Cells.get(i), endpointH3Cells.get(i + 1)));
      } catch (LineUndefinedException e) {
        throw new RuntimeException(e);
      }
    }
    return coveringH3Cells;
  }

  private static Set<Long> coverPolygonInH3(Polygon polygon, int resolution) {
    Set<Long> coveringH3Cells = new HashSet<>(coverLineInH3(polygon.getExteriorRing(), resolution));

    coveringH3Cells.addAll(H3_CORE.polyfill(Arrays.asList(polygon.getCoordinates()).stream()
        .map(coordinate -> new GeoCoord(coordinate.y, coordinate.x)).collect(
            Collectors.toList()), ImmutableList.of(), resolution));
    return coveringH3Cells;
  }

  // Return the set of H3 cells at the specified resolution which completely cover the input shape.
  // inspired by https://github.com/uber/h3/issues/275
  public static Set<Long> coverGeometryInH3(Geometry geometry, int resolution) {
    Set<Long> coveringH3Cells = new HashSet<>();
    if (geometry instanceof Point) {
      coveringH3Cells
          .add(H3_CORE.geoToH3(geometry.getCoordinate().y, geometry.getCoordinate().x, resolution));
    } else if (geometry instanceof LineString) {
      coveringH3Cells.addAll(coverLineInH3(((LineString) geometry), resolution));
    } else if (geometry instanceof Polygon) {
      coveringH3Cells.addAll(coverPolygonInH3(((Polygon) geometry), resolution));
    } else if (geometry instanceof GeometryCollection) {
      for (int i = 0; i < geometry.getNumGeometries(); i++) {
        coveringH3Cells.addAll(coverGeometryInH3(geometry.getGeometryN(i), resolution));
      }
    } else {
      throw new UnsupportedOperationException("Unexpected type: " + geometry.getGeometryType());
    }
    return coveringH3Cells;
  }
}
