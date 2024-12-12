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

import com.google.common.base.Joiner;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;
import org.locationtech.jts.io.geojson.GeoJsonReader;
import org.locationtech.jts.io.geojson.GeoJsonWriter;


/**
 * Utility methods for the geometry.
 */
public class GeometryUtils {
  private GeometryUtils() {
  }

  /**
   * Coordinate system of lat/lng per https://epsg.io/4326
   */
  public static final int GEOGRAPHY_SRID = 4326;
  public static final byte GEOGRAPHY_SET_MASK = (byte) 0x80;
  public static final byte GEOGRAPHY_GET_MASK = (byte) 0x7f;

  public static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
  public static final GeometryFactory GEOGRAPHY_FACTORY = new GeometryFactory(new PrecisionModel(), GEOGRAPHY_SRID);

  public static final WKTReader GEOMETRY_WKT_READER = new WKTReader(GEOMETRY_FACTORY);
  public static final WKTReader GEOGRAPHY_WKT_READER = new WKTReader(GEOGRAPHY_FACTORY);

  public static final GeoJsonReader GEOMETRY_GEO_JSON_READER = new GeoJsonReader(GEOMETRY_FACTORY);
  public static final GeoJsonReader GEOGRAPHY_GEO_JSON_READER = new GeoJsonReader(GEOGRAPHY_FACTORY);

  public static final WKBReader GEOMETRY_WKB_READER = new WKBReader(GEOMETRY_FACTORY);
  public static final WKBReader GEOGRAPHY_WKB_READER = new WKBReader(GEOGRAPHY_FACTORY);

  public static final WKTWriter WKT_WRITER = new WKTWriter();
  public static final WKBWriter WKB_WRITER = new WKBWriter();
  public static final GeoJsonWriter GEO_JSON_WRITER = new GeoJsonWriter();

  public static final double EARTH_RADIUS_KM = 6371.01;
  public static final double EARTH_RADIUS_M = EARTH_RADIUS_KM * 1000.0;
  public static final Joiner OR_JOINER = Joiner.on(" or ");
  public static final Geometry EMPTY_POINT = GEOMETRY_FACTORY.createPoint();

  /**
   * Checks if the given geo-spatial object is a geography object.
   * @param geometry the given object to check
   * @return <code>true</code> if the given geo-spatial object is a geography object, <code>false</code> otherwise
   */
  public static boolean isGeography(Geometry geometry) {
    return geometry.getSRID() == GEOGRAPHY_SRID;
  }

  /**
   * Sets the geometry to geography.
   * @param geometry the geometry to set
   */
  public static void setGeography(Geometry geometry) {
    geometry.setSRID(GEOGRAPHY_SRID);
  }

  /**
   * Sets to geometry.
   * @param geometry the geometry to set
   */
  public static void setGeometry(Geometry geometry) {
    geometry.setSRID(0);
  }
}
