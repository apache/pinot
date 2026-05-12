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
package org.apache.pinot.query.service.dispatch;

import java.util.Map;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class AdaptiveRoutingUpstreamTimingsTest {

  @Test
  public void testEncodeMultipleEntriesRoundTrips() {
    Map<String, Long> timings = new java.util.LinkedHashMap<>();
    timings.put("host-z|8442", 300L);
    timings.put("host-a|8442", 100L);
    timings.put("host-m|8442", 200L);
    String encoded = AdaptiveRoutingUpstreamTimings.encode(timings);
    assertEquals(AdaptiveRoutingUpstreamTimings.decode(encoded), Map.of(
        "host-z|8442", 300L, "host-a|8442", 100L, "host-m|8442", 200L));
  }

  @Test
  public void testRoundTrip() {
    Map<String, Long> original = Map.of("host-a|8442", 100L, "host-b|8442", 200L, "host-c|8442", 300L);
    String encoded = AdaptiveRoutingUpstreamTimings.encode(original);
    assertEquals(AdaptiveRoutingUpstreamTimings.decode(encoded), original);
  }

  @Test
  public void testMergeEncodingsTakesMax() {
    String enc1 = AdaptiveRoutingUpstreamTimings.encode(Map.of("host-a|8442", 100L, "host-b|8442", 50L));
    String enc2 = AdaptiveRoutingUpstreamTimings.encode(Map.of("host-a|8442", 80L, "host-b|8442", 200L));
    Map<String, Long> merged = AdaptiveRoutingUpstreamTimings.decode(
        AdaptiveRoutingUpstreamTimings.mergeEncodings(enc1, enc2));
    assertEquals(merged, Map.of("host-a|8442", 100L, "host-b|8442", 200L));
  }

  @Test
  public void testMergeEncodingsDisjointKeys() {
    String enc1 = AdaptiveRoutingUpstreamTimings.encode(Map.of("host-a|8442", 100L));
    String enc2 = AdaptiveRoutingUpstreamTimings.encode(Map.of("host-b|8442", 200L));
    Map<String, Long> merged = AdaptiveRoutingUpstreamTimings.decode(
        AdaptiveRoutingUpstreamTimings.mergeEncodings(enc1, enc2));
    assertEquals(merged, Map.of("host-a|8442", 100L, "host-b|8442", 200L));
  }
}
