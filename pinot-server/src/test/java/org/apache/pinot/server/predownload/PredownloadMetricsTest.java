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
package org.apache.pinot.server.predownload;

import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.mockito.MockedStatic;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;


public class PredownloadMetricsTest {

  @Test
  public void testPeerSegmentDownloadedFailureEmitsSegmentTaggedMeter() {
    ServerMetrics mockServerMetrics = mock(ServerMetrics.class);
    try (MockedStatic<ServerMetrics> serverMetricsMock = mockStatic(ServerMetrics.class)) {
      serverMetricsMock.when(ServerMetrics::get).thenReturn(mockServerMetrics);
      PredownloadMetrics predownloadMetrics = new PredownloadMetrics();

      predownloadMetrics.peerSegmentDownloaded(false, "testSegment", 0, 0);

      verify(mockServerMetrics).addMeteredValue(eq(ServerMeter.PREDOWNLOAD_PEER_SEGMENT_DOWNLOAD_FAILURE_COUNT), eq(1L),
          eq("testSegment"));
      verifyNoMoreInteractions(mockServerMetrics);
    }
  }

  @Test
  public void testPeerSegmentDownloadedSuccessEmitsGlobalMeterAndGauge() {
    ServerMetrics mockServerMetrics = mock(ServerMetrics.class);
    try (MockedStatic<ServerMetrics> serverMetricsMock = mockStatic(ServerMetrics.class)) {
      serverMetricsMock.when(ServerMetrics::get).thenReturn(mockServerMetrics);
      PredownloadMetrics predownloadMetrics = new PredownloadMetrics();

      predownloadMetrics.peerSegmentDownloaded(true, "testSegment", 1024 * 1024, 1000);

      verify(mockServerMetrics).addMeteredGlobalValue(ServerMeter.PREDOWNLOAD_PEER_SEGMENT_DOWNLOAD_COUNT, 1);
      verify(mockServerMetrics).setValueOfGlobalGauge(eq(ServerGauge.PEER_DOWNLOAD_SPEED), eq(0L));
      verifyNoMoreInteractions(mockServerMetrics);
    }
  }
}
