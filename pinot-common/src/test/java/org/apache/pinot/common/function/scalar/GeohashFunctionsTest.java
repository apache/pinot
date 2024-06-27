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
package org.apache.pinot.common.function.scalar;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class GeohashFunctionsTest {

  @DataProvider(name = "encodeHashTestCases")
  public Object[][] encodeHashTestCases() {
    return new Object[][]{
        {37.8324, -122.271, 7, "9q9p1xh"}, {51.5074, -0.1278, 8, "gcpvj0du"}, {0.0, 0.0, 5, "s0000"}
    };
  }

  @Test(dataProvider = "encodeHashTestCases")
  public void testEncodeHash(double latitude, double longitude, int precision, String expectedGeohash) {
    assertEquals(GeohashFunctions.encodeGeoHash(latitude, longitude, precision), expectedGeohash);
  }

  @DataProvider(name = "decodeHashTestCases")
  public Object[][] decodeHashTestCases() {
    return new Object[][]{
        {"9q9p1ds", new double[]{37.807, -122.271}}, {"gcpvj0eu", new double[]{51.50739431381226, -0.126}}, {"s0000",
        new double[]{0.021, 0.021}}
    };
  }

  @Test(dataProvider = "decodeHashTestCases")
  public void testDecodeHash(String geohash, double[] expectedCoords) {
    double[] decodedCoords = GeohashFunctions.decodeGeoHash(geohash);
    assertEquals(decodedCoords.length, 2);
    assertEquals(decodedCoords[0], expectedCoords[0], 0.001);
    assertEquals(decodedCoords[1], expectedCoords[1], 0.001);
  }

  @Test(dataProvider = "decodeHashTestCases")
  public void testDecodeHashLatitude(String geohash, double[] expectedCoords) {
    assertEquals(GeohashFunctions.decodeGeoHashLatitude(geohash), expectedCoords[0], 0.001);
  }

  @Test(dataProvider = "decodeHashTestCases")
  public void testDecodeHashLongitude(String geohash, double[] expectedCoords) {
    assertEquals(GeohashFunctions.decodeGeoHashLongitude(geohash), expectedCoords[1], 0.001);
  }
}
