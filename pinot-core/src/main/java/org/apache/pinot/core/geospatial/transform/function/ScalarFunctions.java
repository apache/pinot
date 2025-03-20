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

import org.apache.pinot.segment.local.utils.GeometrySerializer;
import org.apache.pinot.segment.local.utils.GeometryUtils;
import org.apache.pinot.segment.local.utils.H3Utils;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.utils.BooleanUtils;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.ParseException;


/**
 * Geospatial scalar functions that can be used in transformation.
 */
public class ScalarFunctions {
  private ScalarFunctions() {
  }

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
  public static byte[] stPoint(double x, double y, Object isGeography) {
    Point point = GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(x, y));
    if (BooleanUtils.toBoolean(isGeography)) {
      GeometryUtils.setGeography(point);
    }
    return GeometrySerializer.serialize(point);
  }

  /**
   * Reads a geometry object from the WKT format.
   */
  @ScalarFunction
  public static byte[] stGeomFromText(String wkt)
      throws ParseException {
    return GeometrySerializer.serialize(GeometryUtils.GEOMETRY_WKT_READER.read(wkt));
  }

  /**
   * Reads a geography object from the WKT format.
   */
  @ScalarFunction
  public static byte[] stGeogFromText(String wkt)
      throws ParseException {
    return GeometrySerializer.serialize(GeometryUtils.GEOGRAPHY_WKT_READER.read(wkt));
  }

  /**
   * Reads a geometry object from the GeoJSON format.
   */
  @ScalarFunction
  public static byte[] stGeomFromGeoJson(String geoJson)
  throws ParseException {
    return GeometrySerializer.serialize(GeometryUtils.GEOMETRY_GEO_JSON_READER.read(geoJson));
  }

  /**
   * Reads a geography object from the GeoJSon format.
   */
  @ScalarFunction
  public static byte[] stGeogFromGeoJson(String geoJson)
  throws ParseException {
    return GeometrySerializer.serialize(GeometryUtils.GEOGRAPHY_GEO_JSON_READER.read(geoJson));
  }

  /**
   * Reads a geometry object from the WKB format.
   */
  @ScalarFunction
  public static byte[] stGeomFromWKB(byte[] wkb)
      throws ParseException {
    return GeometrySerializer.serialize(GeometryUtils.GEOMETRY_WKB_READER.read(wkb));
  }

  /**
   * Reads a geography object from the WKB format.
   */
  @ScalarFunction
  public static byte[] stGeogFromWKB(byte[] wkb)
      throws ParseException {
    return GeometrySerializer.serialize(GeometryUtils.GEOGRAPHY_WKB_READER.read(wkb));
  }

  /**
   * Saves the geometry object as WKT format.
   *
   * @param bytes the serialized geometry object
   * @return the geometry in WKT
   */
  @ScalarFunction
  public static String stAsText(byte[] bytes) {
    return GeometryUtils.WKT_WRITER.write(GeometrySerializer.deserialize(bytes));
  }

  /**
   * Saves the geometry object in GeoJSON format.
   *
   * @param bytes the serialized geometry object
   * @return the geometry in GeoJSON
   */
  @ScalarFunction
  public static String stAsGeoJson(byte[] bytes) {
    return GeometryUtils.GEO_JSON_WRITER.write(GeometrySerializer.deserialize(bytes));
  }

  /**
   * Saves the geometry object as WKB format.
   *
   * @param bytes the serialized geometry object
   * @return the geometry in WKB
   */
  @ScalarFunction
  public static byte[] stAsBinary(byte[] bytes) {
    return GeometryUtils.WKB_WRITER.write(GeometrySerializer.deserialize(bytes));
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
    return H3Utils.H3_CORE.latLngToCell(latitude, longitude, resolution);
  }

  /**
   * Gets the H3 hexagon address from the location
   * @param geoBytes ST_point serialized bytes
   * @param resolution H3 index resolution
   * @return the H3 index address
   */
  @ScalarFunction
  public static long geoToH3(byte[] geoBytes, int resolution) {
    Geometry geometry = GeometrySerializer.deserialize(geoBytes);
    double latitude = geometry.getCoordinate().y;
    double longitude = geometry.getCoordinate().x;
    return H3Utils.H3_CORE.latLngToCell(latitude, longitude, resolution);
  }

  @ScalarFunction
  public static double stDistance(byte[] firstPoint, byte[] secondPoint) {
    Geometry firstGeometry = GeometrySerializer.deserialize(firstPoint);
    Geometry secondGeometry = GeometrySerializer.deserialize(secondPoint);
    if (GeometryUtils.isGeography(firstGeometry) != GeometryUtils.isGeography(secondGeometry)) {
      throw new RuntimeException("The first and second arguments shall either all be geometry or all geography");
    }
    if (GeometryUtils.isGeography(firstGeometry)) {
      return StDistanceFunction.sphericalDistance(firstGeometry, secondGeometry);
    } else {
      return firstGeometry.isEmpty() || secondGeometry.isEmpty() ? Double.NaN : firstGeometry.distance(secondGeometry);
    }
  }

  @ScalarFunction
  public static int stContains(byte[] first, byte[] second) {
    Geometry firstGeometry = GeometrySerializer.deserialize(first);
    Geometry secondGeometry = GeometrySerializer.deserialize(second);
    if (GeometryUtils.isGeography(firstGeometry) != GeometryUtils.isGeography(secondGeometry)) {
      throw new RuntimeException("The first and second arguments should either both be geometry or both be geography");
    }
    // TODO: to fully support Geography contains operation.
    return firstGeometry.contains(secondGeometry) ? 1 : 0;
  }

  @ScalarFunction
  public static int stEquals(byte[] first, byte[] second) {
    Geometry firstGeometry = GeometrySerializer.deserialize(first);
    Geometry secondGeometry = GeometrySerializer.deserialize(second);

    return firstGeometry.equals(secondGeometry) ? 1 : 0;
  }

  @ScalarFunction
  public static String stGeometryType(byte[] bytes) {
    Geometry geometry = GeometrySerializer.deserialize(bytes);
    return geometry.getGeometryType();
  }

  @ScalarFunction
  public static int stWithin(byte[] first, byte[] second) {
    Geometry firstGeometry = GeometrySerializer.deserialize(first);
    Geometry secondGeometry = GeometrySerializer.deserialize(second);
    if (GeometryUtils.isGeography(firstGeometry) != GeometryUtils.isGeography(secondGeometry)) {
      throw new RuntimeException("The first and second arguments should either both be geometry or both be geography");
    }
    // TODO: to fully support Geography within operation.
    return firstGeometry.within(secondGeometry) ? 1 : 0;
  }
}
