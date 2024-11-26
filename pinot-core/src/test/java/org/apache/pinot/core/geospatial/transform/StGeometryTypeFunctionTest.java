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

import java.util.Collections;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.Test;


public class StGeometryTypeFunctionTest extends GeoFunctionTest {

  @Test
  public void testGeometryType()
      throws Exception {
    assertType("Point (1 4)", "Point");
    assertType("LINESTRING (1 1, 2 2)", "LineString");
    assertType("POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))'))", "Polygon");
    assertType("MULTIPOINT (1 1, 2 2)'))", "MultiPoint");
    assertType("MULTILINESTRING ((1 1, 2 2), (3 3, 4 4))", "MultiLineString");
    assertType("MULTIPOLYGON (((1 1, 1 4, 4 4, 4 1, 1 1)), ((1 1, 1 4, 4 4, 4 1, 1 1)))", "MultiPolygon");
    assertType("GEOMETRYCOLLECTION(POINT(4 6),LINESTRING(4 6, 7 10))", "GeometryCollection");
  }

  private void assertType(String wkt, String type)
      throws Exception {
    // assert geometry
    assertStringFunction(String.format("ST_GeometryType(ST_GeomFromText(%s))", STRING_SV_COLUMN), new String[]{type},
        Collections.singletonList(new Column(STRING_SV_COLUMN, DataType.STRING, new String[]{wkt})));

    // assert geography
    assertStringFunction(String.format("ST_GeometryType(ST_GeogFromText(%s))", STRING_SV_COLUMN), new String[]{type},
        Collections.singletonList(new Column(STRING_SV_COLUMN, DataType.STRING, new String[]{wkt})));
  }

  @Test
  public void testGeoJsonConversion()
  throws Exception {
    assertGeoType("{\n \"type\": \"Point\",\n \"coordinates\": [100.0, 0.0]\n }", "Point");

    assertGeoType("{\n"
        + "         \"type\": \"LineString\",\n"
        + "         \"coordinates\": [\n"
        + "             [100.0, 0.0],\n"
        + "             [101.0, 1.0]\n"
        + "         ]\n"
        + "     }", "LineString");

    //no holes
    assertGeoType("{\n"
        + "         \"type\": \"Polygon\",\n"
        + "         \"coordinates\": [\n"
        + "             [\n"
        + "                 [100.0, 0.0],\n"
        + "                 [101.0, 0.0],\n"
        + "                 [101.0, 1.0],\n"
        + "                 [100.0, 1.0],\n"
        + "                 [100.0, 0.0]\n"
        + "             ]\n"
        + "         ]\n"
        + "     }", "Polygon");

    //with holes
    assertGeoType("{\n"
        + "    \"type\": \"Polygon\",\n"
        + "    \"coordinates\": [\n"
        + "        [\n"
        + "            [100.0, 0.0],\n"
        + "            [101.0, 0.0],\n"
        + "            [101.0, 1.0],\n"
        + "            [100.0, 1.0],\n"
        + "            [100.0, 0.0]\n"
        + "        ],\n"
        + "        [\n"
        + "            [100.8, 0.8],\n"
        + "            [100.8, 0.2],\n"
        + "            [100.2, 0.2],\n"
        + "            [100.2, 0.8],\n"
        + "            [100.8, 0.8]\n"
        + "        ]\n"
        + "    ]\n"
        + "}", "Polygon");

    assertGeoType("{\n"
        + "    \"type\": \"MultiPoint\",\n"
        + "    \"coordinates\": [\n"
        + "        [100.0, 0.0],\n"
        + "        [101.0, 1.0]\n"
        + "    ]\n"
        + "}", "MultiPoint");

    assertGeoType("{\n"
        + "    \"type\": \"MultiLineString\",\n"
        + "    \"coordinates\": [\n"
        + "        [\n"
        + "            [100.0, 0.0],\n"
        + "            [101.0, 1.0]\n"
        + "        ],\n"
        + "        [\n"
        + "            [102.0, 2.0],\n"
        + "            [103.0, 3.0]\n"
        + "        ]\n"
        + "    ]\n"
        + "}", "MultiLineString");

    assertGeoType(
        "{\"type\":\"MultiPolygon\","
            + "\"coordinates\":["
            + "[[[0.0,0.0],[100,0.0],[100,100],[0.0,100],[0.0,0.0]],[[1,1],[1,10],[10,10],[10,1],[1,1]]],"
            + "[[[200,200],[200,250],[250,250],[250,200],[200,200]]]"
            + "]}", "MultiPolygon");

    assertGeoType("{\n"
        + "    \"type\": \"GeometryCollection\",\n"
        + "    \"geometries\": [{\n"
        + "        \"type\": \"Point\",\n"
        + "        \"coordinates\": [100.0, 0.0]\n"
        + "    }, {\n"
        + "        \"type\": \"LineString\",\n"
        + "        \"coordinates\": [\n"
        + "            [101.0, 0.0],\n"
        + "            [102.0, 1.0]\n"
        + "        ]\n"
        + "    }]\n"
        + "}", "GeometryCollection");
  }

  private void assertGeoType(String geoJson, String type)
  throws Exception {
    // assert geometry
    assertStringFunction(String.format("ST_GeometryType(ST_GeomFromGeoJSON(%s))", STRING_SV_COLUMN), new String[]{type},
        Collections.singletonList(new Column(STRING_SV_COLUMN, DataType.STRING, new String[]{geoJson})));

    // assert geography
    assertStringFunction(String.format("ST_GeometryType(ST_GeogFromGeoJSON(%s))", STRING_SV_COLUMN), new String[]{type},
        Collections.singletonList(new Column(STRING_SV_COLUMN, DataType.STRING, new String[]{geoJson})));
  }
}
