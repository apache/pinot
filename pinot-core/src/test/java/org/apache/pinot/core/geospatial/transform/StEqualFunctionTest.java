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


public class StEqualFunctionTest extends GeoFunctionTest {
  @Test
  public void testEquals() throws Exception {
    assertRelation("ST_Equals", "POINT (20 20)", "POINT (25 25)", false);
    assertRelation("ST_Equals", "POINT (20 20)", "POINT (20 20)", true);
    assertRelation("ST_Equals", "MULTIPOINT (20 20, 25 25)", "MULTIPOINT (20 20, 25 25)", true);
    assertRelation("ST_Equals", "LINESTRING (20 20, 30 30)", "LINESTRING (20 20, 30 30)", true);
    assertRelation("ST_Equals", "MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))", "MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))",
        true);
    assertRelation("ST_Equals", "POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))", "POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))", true);
    assertRelation("ST_Equals", "MULTIPOLYGON (((0 0, 0 2, 2 2, 2 0, 0 0)), ((2 2, 2 4, 4 4, 4 2, 2 2)))",
        "MULTIPOLYGON (((2 2, 2 4, 4 4, 4 2, 2 2)), ((0 0, 0 2, 2 2, 2 0, 0 0)))", true);
  }
}
