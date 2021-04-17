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

import org.testng.annotations.Test;


public class StContainsFunctionTest extends GeoFunctionTest {
  @Test
  public void testContains() throws Exception {
    assertRelation("ST_Contains", "POINT (20 20)", "POINT (25 25)", false);
    assertRelation("ST_Contains", "MULTIPOINT (20 20, 25 25)", "POINT (25 25)", true);
    assertRelation("ST_Contains", "LINESTRING (20 20, 30 30)", "POINT (25 25)", true);
    assertRelation("ST_Contains", "LINESTRING (20 20, 30 30)", "MULTIPOINT (25 25, 31 31)", false);
    assertRelation("ST_Contains", "LINESTRING (20 20, 30 30)", "LINESTRING (25 25, 27 27)", true);
    assertRelation("ST_Contains", "MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))",
        "MULTILINESTRING ((3 4, 4 4), (2 1, 6 1))", false);
    assertRelation("ST_Contains", "POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))", "POLYGON ((1 1, 1 2, 2 2, 2 1, 1 1))", true);
    assertRelation("ST_Contains", "POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))", "POLYGON ((-1 -1, -1 2, 2 2, 2 -1, -1 -1))",
        false);
    assertRelation("ST_Contains", "MULTIPOLYGON (((0 0, 0 2, 2 2, 2 0, 0 0)), ((2 2, 2 4, 4 4, 4 2, 2 2)))",
        "POLYGON ((2 2, 2 3, 3 3, 3 2, 2 2))", true);
    assertRelation("ST_Contains", "LINESTRING (20 20, 30 30)", "POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))", false);
    assertRelation("ST_Contains", "LINESTRING EMPTY", "POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))", false);
    assertRelation("ST_Contains", "LINESTRING (20 20, 30 30)", "POLYGON EMPTY", false);
  }
}
