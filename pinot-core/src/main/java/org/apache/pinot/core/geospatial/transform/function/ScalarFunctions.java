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
package org.apache.pinot.core.geospatial.transform.function;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.uber.h3core.LengthUnit;
import com.uber.h3core.util.GeoCoord;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.pinot.segment.local.utils.GeometrySerializer;
import org.apache.pinot.segment.local.utils.GeometryUtils;
import org.apache.pinot.segment.local.utils.H3Utils;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.WKTWriter;


/**
 * Geospatial scalar functions that can be used in transformation.
 */
public class ScalarFunctions {

  private static final ImmutableMap<Double, Integer> RESOLUTIONS = ImmutableMap.<Double, Integer>builder()
          .put(1107.712591000, 0)
          .put(418.676005500, 1)
          .put(158.244655800, 2)
          .put(59.810857940, 3)
          .put(22.606379400, 4)
          .put(8.544408276, 5)
          .put(3.229482772, 6)
          .put(1.220629759, 7)
          .put(0.461354684, 8)
          .put(0.174375668, 9)
          .put(0.065907807, 10)
          .put(0.024910561, 11)
          .put(0.009415526, 12)
          .put(0.003559893, 13)
          .put(0.001348575, 14)
          .put(0.000509713, 15)
          .build();

  /**
   * Creates a point.
   *
   * @param x x
   * @param y y
   * @return the created point
   */
  @ScalarFunction
  public static byte[] stPoint(double x, double y) {
    return GeometrySerializer.serialize(GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(x, y)));
  }

  /**
   * Creates a point.
   *
   * @param x x
   * @param y y
   * @param isGeography if it's geography
   * @return the created point
   */
  @ScalarFunction
  public static byte[] stPoint(double x, double y, boolean isGeography) {
    Point point = GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(x, y));
    if (isGeography) {
      GeometryUtils.setGeography(point);
    }
    return GeometrySerializer.serialize(point);
  }

  /**
   * Saves the geometry object as WKT format.
   *
   * @param bytes the serialized geometry object
   * @return the geometry in WKT
   */
  @ScalarFunction
  public static String stAsText(byte[] bytes) {
    WKTWriter writer = new WKTWriter();
    return writer.write(GeometrySerializer.deserialize(bytes));
  }

  /**
   * Converts a Geometry object to a spherical geography object.
   *
   * @param bytes the serialized geometry object
   * @return the geographical object
   */
  @ScalarFunction
  public static byte[] toSphericalGeography(byte[] bytes) {
    Geometry geometry = GeometrySerializer.deserialize(bytes);
    GeometryUtils.setGeography(geometry);
    return GeometrySerializer.serialize(geometry);
  }

  /**
   * Converts a spherical geographical object to a Geometry object.
   *
   * @param bytes the serialized geographical object
   * @return the geometry object
   */
  @ScalarFunction
  public static byte[] toGeometry(byte[] bytes) {
    Geometry geometry = GeometrySerializer.deserialize(bytes);
    GeometryUtils.setGeometry(geometry);
    return GeometrySerializer.serialize(geometry);
  }

  /**
   * Gets the H3 hexagon address from the location
   * @param latitude latitude of the location
   * @param longitude longitude of the location
   * @param resolution H3 index resolution
   * @return the H3 index address
   */
  @ScalarFunction
  public static long geoToH3(double longitude, double latitude, int resolution) {
    return H3Utils.H3_CORE.geoToH3(latitude, longitude, resolution);
  }

  public static List<Long> polygonToH3(List<Coordinate> region, int res) {
    return H3Utils.H3_CORE.polyfill(
            region.stream()
                    .map(coord -> new GeoCoord(coord.x, coord.y))
                    .collect(Collectors.toUnmodifiableList()),
            ImmutableList.of(),
            res);
  }

  public static int calcResFromMaxDist(double maxDist, int minHexagonEdges) {
    return RESOLUTIONS.get(Collections.min(RESOLUTIONS.keySet().stream()
            .filter(edgeLen -> edgeLen < maxDist / minHexagonEdges)
            .collect(Collectors.toUnmodifiableSet())));
  }

  public static double maxDist(List<Coordinate> points) {
    int n = points.size();
    double max = 0;

    for (int i = 0; i < n; i++) {
      for (int j = i + 1; j < n; j++) {
        max = Math.max(max, dist(points.get(i), points.get(j)));
      }
    }
    return Math.sqrt(max);
  }

  public static double dist(Coordinate a, Coordinate b) {
        return H3Utils.H3_CORE.pointDist(new GeoCoord(a.y, a.x), new GeoCoord(b.y, b.x), LengthUnit.km);
  }
}
