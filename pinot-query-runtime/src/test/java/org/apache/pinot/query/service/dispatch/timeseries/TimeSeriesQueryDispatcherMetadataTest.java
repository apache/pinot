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
package org.apache.pinot.query.service.dispatch.timeseries;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants.Query.Request.MetadataKeys;
import org.apache.pinot.tsdb.planner.physical.TimeSeriesDispatchablePlan;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


/// The segment lists a server receives with a time-series query.
public class TimeSeriesQueryDispatcherMetadataTest {
  @Test
  public void testSegmentListsInServerMetadata() {
    // Server_1 reads leafA from required and optional (not yet ONLINE) segments.
    TimeSeriesDispatchablePlan plan = new TimeSeriesDispatchablePlan("m3ql", List.of(), null, List.of(), false,
        TimeBuckets.ofSeconds(1000L, Duration.ofSeconds(10), 6),
        Map.of("Server_1", Map.of("leafA", List.of("seg1", "seg2"))),
        Map.of("Server_1", Map.of("leafA", List.of("seg3"))), Map.of(), List.of());
    RequestContext requestContext = mock(RequestContext.class);
    when(requestContext.getRequestId()).thenReturn(7L);
    when(requestContext.getBrokerId()).thenReturn("broker");

    Map<String, String> metadata =
        TimeSeriesQueryDispatcher.initializeTimeSeriesMetadataMap(plan, 1000L, requestContext, "Server_1");

    assertEquals(metadata.get(MetadataKeys.TimeSeries.encodeSegmentListKey("leafA")), "seg1,seg2");
    assertEquals(metadata.get(MetadataKeys.TimeSeries.encodeOptionalSegmentListKey("leafA")), "seg3");
  }
}
