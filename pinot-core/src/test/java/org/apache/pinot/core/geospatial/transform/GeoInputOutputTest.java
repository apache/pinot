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
package org.apache.pinot.core.geospatial.transform;

import java.util.Arrays;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.annotations.Test;


public class GeoInputOutputTest extends GeoFunctionTest {
  @Test
  public void testInputOutput()
      throws Exception {
    // empty geometries
    assertAsTextAndBinary("MULTIPOINT EMPTY");
    assertAsTextAndBinary("LINESTRING EMPTY");
    assertAsTextAndBinary("MULTILINESTRING EMPTY");
    assertAsTextAndBinary("POLYGON EMPTY");
    assertAsTextAndBinary("MULTIPOLYGON EMPTY");
    assertAsTextAndBinary("GEOMETRYCOLLECTION EMPTY");

    // valid nonempty geometries
    assertAsTextAndBinary("POINT (1 2)");
    assertAsTextAndBinary("MULTIPOINT ((1 2), (3 4))");
    assertAsTextAndBinary("LINESTRING (0 0, 1 2, 3 4)");
    assertAsTextAndBinary("MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))");
    assertAsTextAndBinary("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))");
    assertAsTextAndBinary("POLYGON ((0 0, 0 3, 3 3, 3 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))");
    assertAsTextAndBinary("MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((2 4, 2 6, 6 6, 6 4, 2 4)))");
    assertAsTextAndBinary(
        "GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 1 2, 3 4), POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0)))");

    assertAsTextAndBinary("MULTIPOINT ((0 0), (0 1), (1 1), (0 1))");
    assertAsTextAndBinary("LINESTRING (0 0, 0 1, 0 1, 1 1, 1 0, 0 0)");
  }

  private void assertAsTextAndBinary(String wkt)
      throws Exception {
    // assert geometry
    assertStringFunction(String.format("ST_AsText(ST_GeomFromWKB(ST_AsBinary(ST_GeomFromText(%s))))", STRING_SV_COLUMN),
        new String[]{wkt}, Arrays.asList(new Column(STRING_SV_COLUMN, FieldSpec.DataType.STRING, new String[]{wkt})));
    // assert geography
    assertStringFunction(String.format("ST_AsText(ST_GeogFromWKB(ST_AsBinary(ST_GeogFromText(%s))))", STRING_SV_COLUMN),
        new String[]{wkt}, Arrays.asList(new Column(STRING_SV_COLUMN, FieldSpec.DataType.STRING, new String[]{wkt})));
  }

  @Test
  public void testGeoJsonInputOutput()
  throws Exception {
    // empty geometries
    assertAsGeoJsonAndBinary(
        "{\"type\":\"Point\",\"coordinates\":[],"
            + "\"crs\":{\"type\":\"name\",\"properties\":{\"name\":\"EPSG:#EPSG#\"}}}");

    assertAsGeoJsonAndBinary(
        "{\"type\":\"LineString\",\"coordinates\":[],"
            + "\"crs\":{\"type\":\"name\",\"properties\":{\"name\":\"EPSG:#EPSG#\"}}}");

    assertAsGeoJsonAndBinary(
        "{\"type\":\"MultiLineString\",\"coordinates\":[],"
            + "\"crs\":{\"type\":\"name\",\"properties\":{\"name\":\"EPSG:#EPSG#\"}}}");

    assertAsGeoJsonAndBinary("{\"type\":\"Polygon\",\"coordinates\":[],"
        + "\"crs\":{\"type\":\"name\",\"properties\":{\"name\":\"EPSG:#EPSG#\"}}}");

    assertAsGeoJsonAndBinary("{\"type\":\"MultiPolygon\",\"coordinates\":[],"
        + "\"crs\":{\"type\":\"name\",\"properties\":{\"name\":\"EPSG:#EPSG#\"}}}");

    assertAsGeoJsonAndBinary("{\"type\":\"GeometryCollection\",\"geometries\":[],"
        + "\"crs\":{\"type\":\"name\",\"properties\":{\"name\":\"EPSG:#EPSG#\"}}}");

    // valid nonempty geometries
    assertAsGeoJsonAndBinary(
        "{\"type\":\"Point\","
            + "\"coordinates\":[1,2],"
            + "\"crs\":{\"type\":\"name\",\"properties\":{\"name\":\"EPSG:#EPSG#\"}}}");

    assertAsGeoJsonAndBinary("{\"type\":\"MultiPoint\","
        + "\"coordinates\":[[1,2],[3,4]],"
        + "\"crs\":{\"type\":\"name\",\"properties\":{\"name\":\"EPSG:#EPSG#\"}}}");

    assertAsGeoJsonAndBinary("{\"type\":\"LineString\","
        + "\"coordinates\":[[0.0,0.0],[1,2],[3,4]],"
        + "\"crs\":{\"type\":\"name\",\"properties\":{\"name\":\"EPSG:#EPSG#\"}}}");

    assertAsGeoJsonAndBinary("{\"type\":\"MultiLineString\","
        + "\"coordinates\":["
        + "[[100,0.0],[101,1]],"
        + "[[102,2],[103,3]]"
        + "],"
        + "\"crs\":{\"type\":\"name\",\"properties\":{\"name\":\"EPSG:#EPSG#\"}}}");

    assertAsGeoJsonAndBinary("{\"type\":\"Polygon\","
        + "\"coordinates\":["
        + "[[100,0.0],[100,1],[101,1],[101,0.0],[100,0.0]],"
        + "[[100.8,0.8],[100.2,0.8],[100.2,0.2],[100.8,0.2],[100.8,0.8]]"
        + "],"
        + "\"crs\":{\"type\":\"name\",\"properties\":{\"name\":\"EPSG:#EPSG#\"}}}");

    assertAsGeoJsonAndBinary("{\"type\":\"MultiPolygon\","
        + "\"coordinates\":["
        + "[[[0.0,0.0],[0.0,100],[100,100],[100,0.0],[0.0,0.0]],[[1,1],[10,1],[10,10],[1,10],[1,1]]],"
        + "[[[200,200],[200,250],[250,250],[250,200],[200,200]]]"
        + "],"
        + "\"crs\":{\"type\":\"name\",\"properties\":{\"name\":\"EPSG:#EPSG#\"}}}");

    assertAsGeoJsonAndBinary("{\"type\":\"GeometryCollection\","
        + "\"geometries\":["
        + "{\"type\":\"Point\",\"coordinates\":[100,0.0]},"
        + "{\"type\":\"LineString\",\"coordinates\":[[101,0.0],[102,1]]"
        + "}],"
        + "\"crs\":{\"type\":\"name\",\"properties\":{\"name\":\"EPSG:#EPSG#\"}}}");
  }

  private void assertAsGeoJsonAndBinary(String geoJson)
  throws Exception {
    // assert geometry
    assertStringFunction(
        String.format("ST_AsGeoJson(ST_GeomFromWKB(ST_AsBinary(ST_GeomFromGeoJson(%s))))", STRING_SV_COLUMN),
        new String[]{geoJson.replace("#EPSG#", "0")},
        Arrays.asList(new Column(STRING_SV_COLUMN, FieldSpec.DataType.STRING, new String[]{geoJson})));

    // assert geography
    assertStringFunction(
        String.format("ST_AsGeoJson(ST_GeogFromWKB(ST_AsBinary(ST_GeogFromGeoJson(%s))))", STRING_SV_COLUMN),
        new String[]{geoJson.replace("#EPSG#", "4326")},
        Arrays.asList(new Column(STRING_SV_COLUMN, FieldSpec.DataType.STRING, new String[]{geoJson})));
  }
}
