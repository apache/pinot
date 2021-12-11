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


public class GeoToH3FunctionTest extends GeoFunctionTest {

  @Test
  public void testGetHexagonAddress()
      throws Exception {
    assertValue("20,102,5", 599041711439609855L);
    assertValue("-122.419,37.775,6", 604189371209351167L);
    assertValue("116.407394,39.904202,6", 604356067480043519L);

    assertValue("ST_point(20,102),5", 599041711439609855L);
    assertValue("ST_point(-122.419,37.775),6", 604189371209351167L);
    assertValue("ST_point(116.407394,39.904202),6", 604356067480043519L);
  }

  private void assertValue(String args, long value)
      throws Exception {
    assertLongFunction(String.format("geoToH3(%s)", args), new long[]{value},
        Arrays.asList(new Column(LONG_SV_COLUMN, FieldSpec.DataType.LONG, new Long[]{value})));
  }
}
