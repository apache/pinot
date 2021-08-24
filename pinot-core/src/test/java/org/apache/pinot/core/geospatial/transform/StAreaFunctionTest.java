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

import static java.lang.String.format;


public class StAreaFunctionTest extends GeoFunctionTest {
  @Test
  public void testStAreaGeom()
      throws Exception {
    assertArea("POLYGON ((2 2, 2 6, 6 6, 6 2, 2 2))", 16.0, true);
    assertArea("POLYGON EMPTY", 0.0, true);
    assertArea("LINESTRING (1 4, 2 5)", 0.0, true);
    assertArea("LINESTRING EMPTY", 0.0, true);
    assertArea("POINT (1 4)", 0.0, true);
    assertArea("POINT EMPTY", 0.0, true);
    assertArea("GEOMETRYCOLLECTION EMPTY", 0.0, true);

    // Test basic geometry collection. Area is the area of the polygon.
    assertArea("GEOMETRYCOLLECTION (POINT (8 8), LINESTRING (5 5, 6 6), POLYGON ((1 1, 3 1, 3 4, 1 4, 1 1)))", 6.0,
        true);

    // Test overlapping geometries. Area is the sum of the individual elements
    assertArea("GEOMETRYCOLLECTION (POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0)), POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1)))", 8.0,
        true);

    // Test nested geometry collection
    assertArea(
        "GEOMETRYCOLLECTION (POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0)), POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1)), "
            + "GEOMETRYCOLLECTION (POINT (8 8), LINESTRING (5 5, 6 6), POLYGON ((1 1, 3 1, 3 4, 1 4, 1 1))))",
        14.0, true);
  }

  @Test
  public void testStAreaGeog()
      throws Exception {
    assertArea("POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))", 1.2364036567076416E10, false);

    assertArea(
        "POLYGON((-122.150124 37.486095, -122.149201 37.486606,  -122.145725 37.486580, -122.145923 37.483961, -122"
            + ".149324 37.482480,  -122.150837 37.483238,  -122.150901 37.485392, -122.150124 37.486095))",
        163290.93943479148, false);

    double angleOfOneKm = 0.008993201943349;
    assertArea(format("POLYGON((0 0, %.15f 0, %.15f %.15f, 0 %.15f, 0 0))", angleOfOneKm, angleOfOneKm, angleOfOneKm,
        angleOfOneKm), 999999.9979474121, false);

    // 1/4th of an hemisphere, ie 1/8th of the planet, should be close to 4PiR2/8 = 637.58E11
    assertArea("POLYGON((90 0, 0 0, 0 90, 90 0))", 6.375825913974856E13, false);

    //A Polygon with a large hole
    assertArea("POLYGON((90 0, 0 0, 0 90, 90 0), (89 1, 1 1, 1 89, 89 1))", 3.480423348045961E12, false);
  }

  private void assertArea(String wkt, double area, boolean geom)
      throws Exception {
    assertDoubleFunction(
        String.format("ST_Area(%s(%s))", geom ? "ST_GeomFromText" : "ST_GeogFromText", STRING_SV_COLUMN),
        new double[]{area}, Arrays.asList(new Column(STRING_SV_COLUMN, FieldSpec.DataType.STRING, new String[]{wkt})));
  }
}
