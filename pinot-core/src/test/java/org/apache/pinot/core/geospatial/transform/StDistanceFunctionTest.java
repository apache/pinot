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


public class StDistanceFunctionTest extends GeoFunctionTest {
  @Test
  public void testGeomDistance()
      throws Exception {
    assertDistance("POINT (50 100)", "POINT (150 150)", 111.80339887498948, true);
    assertDistance("MULTIPOINT (50 100, 50 200)", "Point (50 100)", 0.0, true);
    assertDistance("LINESTRING (50 100, 50 200)", "LINESTRING (10 10, 20 20)", 85.44003745317531, true);
    assertDistance("MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))", "LINESTRING (10 20, 20 50)", 17.08800749063506, true);
    assertDistance("POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))", "POLYGON ((4 4, 4 5, 5 5, 5 4, 4 4))", 1.4142135623730951,
        true);
    assertDistance("MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((0 0, 0 2, 2 2, 2 0, 0 0)))",
        "POLYGON ((10 100, 30 10, 30 100, 10 100))", 27.892651361962706, true);

    //  (FIXME): the follow testings require null handling
//    assertDistance("POINT EMPTY", "POINT EMPTY",  null, true);
//    assertDistance("MULTIPOINT EMPTY", "Point (50 100)",  null, true);
//    assertDistance("LINESTRING (50 100, 50 200)", "LINESTRING EMPTY",  null, true);
//    assertDistance("MULTILINESTRING EMPTY", "LINESTRING (10 20, 20 50)",  null, true);
//    assertDistance("POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))", "POLYGON EMPTY",  null, true);
//    assertDistance("MULTIPOLYGON EMPTY", "POLYGON ((10 100, 30 10, 30 100, 10 100))",  null, true);
  }

  @Test
  public void testGeogDistance()
      throws Exception {
    assertDistance("POINT(-86.67 36.12)", "POINT(-118.40 33.94)", 2886448.9734367016, false);
    assertDistance("POINT(-118.40 33.94)", "POINT(-86.67 36.12)", 2886448.9734367016, false);
    assertDistance("POINT(-71.0589 42.3601)", "POINT(-71.2290 42.4430)", 16734.69743457383, false);
    assertDistance("POINT(-86.67 36.12)", "POINT(-86.67 36.12)", 0.0, false);

    //  (FIXME): the follow testings require null handling
//    assertDistance("POINT EMPTY", "POINT (40 30)", null);
//    assertDistance("POINT (20 10)", "POINT EMPTY", null);
//    assertDistance("POINT EMPTY", "POINT EMPTY", null);
  }

  private void assertDistance(String wkt1, String wkt2, double distance, boolean geom)
      throws Exception {
    assertDoubleFunction(String
        .format("ST_Distance(%s(%s), %s(%s))", geom ? "ST_GeomFromText" : "ST_GeogFromText", STRING_SV_COLUMN,
            geom ? "ST_GeomFromText" : "ST_GeogFromText", STRING_SV_COLUMN2), new double[]{distance}, Arrays
        .asList(new Column(STRING_SV_COLUMN, FieldSpec.DataType.STRING, new String[]{wkt1}),
            new Column(STRING_SV_COLUMN2, FieldSpec.DataType.STRING, new String[]{wkt2})));
  }
}
