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
package org.apache.pinot.common.function;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Pattern;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests that all timezones in zone-index.properties are supported by java.util.TimeZone,
 * java.time.ZoneId, and Joda-Time's DateTimeZone.
 */
public class TimeZoneKeyTest {

  // Matches bare numeric UTC offset strings like +01:00, -05:30, +00:01.
  // java.util.TimeZone does not support this format; it requires GMT+HH:MM.
  private static final Pattern MINUTE_GRANULARITY_OFFSET =
      Pattern.compile("[+-]\\d{2}:[0-5]\\d");

  @Test
  public void testAllTimeZonesSupportedByJavaUtilTimeZone() {
    List<String> unsupported = new ArrayList<>();
    for (TimeZoneKey timeZoneKey : TimeZoneKey.getTimeZoneKeys()) {
      String zoneId = timeZoneKey.getId();
      if (MINUTE_GRANULARITY_OFFSET.matcher(zoneId).matches()) {
        continue;
      }
      TimeZone timeZone = TimeZone.getTimeZone(zoneId);
      // TimeZone.getTimeZone returns GMT for unrecognized IDs, so verify the ID matches
      // unless the original was already GMT/UTC
      if (!zoneId.equals("GMT") && !zoneId.equals("UTC") && timeZone.getID().equals("GMT")) {
        unsupported.add(zoneId);
      }
    }
    Assert.assertTrue(unsupported.isEmpty(),
        "java.util.TimeZone does not recognize the following timezones: " + unsupported);
  }

  @Test
  public void testAllTimeZonesSupportedByJavaTimeZoneId() {
    List<String> unsupported = new ArrayList<>();
    for (TimeZoneKey timeZoneKey : TimeZoneKey.getTimeZoneKeys()) {
      String zoneId = timeZoneKey.getId();
      try {
        ZoneId.of(zoneId);
      } catch (Exception e) {
        unsupported.add(zoneId + " (" + e.getMessage() + ")");
      }
    }
    Assert.assertTrue(unsupported.isEmpty(),
        "java.time.ZoneId does not recognize the following timezones: " + unsupported);
  }

  @Test
  public void testAllTimeZonesSupportedByJodaTime() {
    List<String> unsupported = new ArrayList<>();
    for (TimeZoneKey timeZoneKey : TimeZoneKey.getTimeZoneKeys()) {
      String zoneId = timeZoneKey.getId();
      try {
        DateTimeZone.forID(zoneId);
      } catch (Exception e) {
        unsupported.add(zoneId + " (" + e.getMessage() + ")");
      }
    }
    Assert.assertTrue(unsupported.isEmpty(),
        "Joda-Time DateTimeZone does not recognize the following timezones: " + unsupported);
  }

  @Test
  public void testNoDuplicateTimezoneIds() {
    Set<String> seen = new HashSet<>();
    Set<String> duplicates = new LinkedHashSet<>();
    for (TimeZoneKey timeZoneKey : TimeZoneKey.getTimeZoneKeys()) {
      String id = timeZoneKey.getId();
      if (!seen.add(id.toLowerCase(Locale.ENGLISH))) {
        duplicates.add(id);
      }
    }
    Assert.assertTrue(duplicates.isEmpty(),
        "zone-index.properties has duplicate timezone IDs: " + duplicates);
  }

  @Test
  public void testZoneIndexPropertiesIsNotEmpty() {
    Assert.assertFalse(TimeZoneKey.getTimeZoneKeys().isEmpty(),
        "zone-index.properties should contain at least one timezone");
  }

  @Test
  public void testUtcKeyIsPresent() {
    Assert.assertNotNull(TimeZoneKey.getTimeZoneKey("UTC"),
        "UTC should be a recognized timezone key");
    Assert.assertEquals(TimeZoneKey.UTC_KEY.getKey(), (short) 0,
        "UTC should have key 0");
  }
}
