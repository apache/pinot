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
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;


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
  public static byte[] stPoint(double x, double y, boolean isGeography) {
    Point point = GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(x, y));
    if (isGeography) {
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
    return GeometrySerializer.serialize(new WKTReader(GeometryUtils.GEOMETRY_FACTORY).read(wkt));
  }

  /**
   * Reads a geography object from the WKT format.
   */
  @ScalarFunction
  public static byte[] stGeogFromText(String wkt)
      throws ParseException {
    return GeometrySerializer.serialize(new WKTReader(GeometryUtils.GEOGRAPHY_FACTORY).read(wkt));
  }

  /**
   * Reads a geometry object from the WKB format.
   */
  @ScalarFunction
  public static byte[] stGeomFromWKB(byte[] wkb)
      throws ParseException {
    return GeometrySerializer.serialize(new WKBReader(GeometryUtils.GEOMETRY_FACTORY).read(wkb));
  }

  /**
   * Reads a geography object from the WKB format.
   */
  @ScalarFunction
  public static byte[] stGeogFromWKB(byte[] wkb)
      throws ParseException {
    return GeometrySerializer.serialize(new WKBReader(GeometryUtils.GEOGRAPHY_FACTORY).read(wkb));
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
   * Saves the geometry object as WKB format.
   *
   * @param bytes the serialized geometry object
   * @return the geometry in WKB
   */
  @ScalarFunction
  public static byte[] stAsBinary(byte[] bytes) {
    WKBWriter writer = new WKBWriter();
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
}
