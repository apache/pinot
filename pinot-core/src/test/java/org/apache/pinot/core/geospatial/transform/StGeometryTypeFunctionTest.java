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
    assertStringFunction(String.format("ST_GeometryType(ST_GeomFromText(%s))))", STRING_SV_COLUMN), new String[]{type},
        Arrays.asList(new GeoFunctionTest.Column(STRING_SV_COLUMN, FieldSpec.DataType.STRING, new String[]{wkt})));
    // assert geography
    assertStringFunction(String.format("ST_GeometryType(ST_GeogFromText(%s))))", STRING_SV_COLUMN), new String[]{type},
        Arrays.asList(new GeoFunctionTest.Column(STRING_SV_COLUMN, FieldSpec.DataType.STRING, new String[]{wkt})));
  }
}
