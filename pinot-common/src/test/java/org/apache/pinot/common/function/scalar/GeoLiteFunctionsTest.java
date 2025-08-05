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

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class GeoLiteFunctionsTest {

  private static final String TEST_IP = "142.251.10.104";

  @Test
  public void testAsnFromIP() {
    String asnJson = GeoLiteFunctions.getAsnJsonFromIP(TEST_IP);  // 或 TEST_IP
    assertNotNull(asnJson);
    assertTrue(asnJson.contains("\"autonomous_system_number\":15169"));
    assertTrue(asnJson.contains("\"autonomous_system_organization\":\"GOOGLE\""));
    assertTrue(asnJson.contains("\"ip_address\":\"142.251.10.104\""));
    assertTrue(asnJson.contains("\"network\":\"142.250.0.0/15\""));
  }

  @Test
  public void testAsnNUmberFromIP() {
    String asnNumber = GeoLiteFunctions.getAsnNumberFromIP(TEST_IP);
    assertEquals(asnNumber, "15169");
  }

  @Test
  public void testAsnNameFromIP() {
    String asnName = GeoLiteFunctions.getAsnNameFromIP(TEST_IP);
    assertEquals(asnName, "GOOGLE");
  }

  @Test
  public void getContinentFromIP() {
    String continent = GeoLiteFunctions.getContinentFromIP(TEST_IP);
    assertEquals(continent, "North America");
  }

  @Test
  public void testCountryFromIP() {
    String country = GeoLiteFunctions.getCountryFromIP(TEST_IP);
    assertEquals(country, "United States");
  }

  @Test
  public void testCountryCodeFromIP() {
    String countryCode = GeoLiteFunctions.getCountryCodeFromIP(TEST_IP);
    assertEquals(countryCode, "US");
  }

  @Test
  public void testLatitudeFromIP() {
    Double latitude = GeoLiteFunctions.getLatitudeFromIP(TEST_IP);
    assertEquals(latitude, 37.751);
  }

  @Test
  public void testLongitudeFromIP() {
    Double longitude = GeoLiteFunctions.getLongitudeFromIP(TEST_IP);
    assertEquals(longitude, -97.822);
  }

  @Test
  public void testTimeZoneFromIP() {
    String postalCode = GeoLiteFunctions.getTimeZoneFromIP(TEST_IP);
    assertEquals(postalCode, "America/Chicago");
  }
}
