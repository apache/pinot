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
  public void testInputOutput() throws Exception {
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

  private void assertAsTextAndBinary(String wkt) throws Exception {
    // assert geometry
    assertStringFunction(String.format("ST_AsText(ST_GeomFromWKB(ST_AsBinary(ST_GeomFromText(%s))))", STRING_SV_COLUMN),
        new String[]{wkt}, Arrays.asList(new Column(STRING_SV_COLUMN, FieldSpec.DataType.STRING, new String[]{wkt})));
    // assert geography
    assertStringFunction(String.format("ST_AsText(ST_GeogFromWKB(ST_AsBinary(ST_GeogFromText(%s))))", STRING_SV_COLUMN),
        new String[]{wkt}, Arrays.asList(new Column(STRING_SV_COLUMN, FieldSpec.DataType.STRING, new String[]{wkt})));
  }
}
