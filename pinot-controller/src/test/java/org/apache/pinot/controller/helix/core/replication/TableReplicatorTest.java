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
package org.apache.pinot.controller.helix.core.replication;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.controller.api.resources.CopyTablePayload;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.WatermarkInductionResult;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TableReplicatorTest {

  @Mock
  private PinotHelixResourceManager _pinotHelixResourceManager;
  @Mock
  private ExecutorService _executorService;
  @Mock
  private SegmentCopier _segmentCopier;
  @Mock
  private HttpClient _httpClient;

  private TableReplicator _tableReplicator;
  private AutoCloseable _mocks;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
    _tableReplicator = new TableReplicator(_pinotHelixResourceManager, _executorService, _segmentCopier, _httpClient);
  }

  @AfterMethod
  public void tearDown() throws Exception {
    _mocks.close();
  }

  @Test
  public void testReplicateTable() throws Exception {
    String jobId = "job1";
    String tableName = "table1_REALTIME";
    String sourceClusterUri = "http://localhost:9000";
    CopyTablePayload copyTablePayload = new CopyTablePayload(sourceClusterUri, Collections.emptyMap(),
        "http://dest", Collections.emptyMap(), "broker", "server", Collections.emptyMap(), null, null);

    List<WatermarkInductionResult.Watermark> watermarks = Arrays.asList(
        new WatermarkInductionResult.Watermark(0, 10, 100L),
        new WatermarkInductionResult.Watermark(1, 11, 110L));
    WatermarkInductionResult watermarkInductionResult = new WatermarkInductionResult(watermarks,
        Arrays.asList("seg1", "seg2"));

    // Mock HttpClient response for ZK metadata
    Map<String, Map<String, String>> zkMetadataMap = new HashMap<>();
    Map<String, String> seg1Metadata = new HashMap<>();
    seg1Metadata.put("k1", "v1");
    zkMetadataMap.put("seg1", seg1Metadata);

    Map<String, String> seg2Metadata = new HashMap<>();
    seg2Metadata.put("k2", "v2");
    zkMetadataMap.put("seg2", seg2Metadata);

    String zkMetadataJson = new ObjectMapper().writeValueAsString(zkMetadataMap);
    SimpleHttpResponse response = mock(SimpleHttpResponse.class);
    when(response.getResponse()).thenReturn(zkMetadataJson);
    when(response.getStatusCode()).thenReturn(200);

    when(_httpClient.sendGetRequest(any(URI.class), anyMap())).thenReturn(response);
    when(_pinotHelixResourceManager.addNewTableReplicationJob(anyString(), anyString(), anyLong(),
        any(WatermarkInductionResult.class))).thenReturn(true);

    _tableReplicator.replicateTable(jobId, tableName, copyTablePayload, watermarkInductionResult);

    ArgumentCaptor<WatermarkInductionResult> resultCaptor = ArgumentCaptor.forClass(WatermarkInductionResult.class);
    verify(_pinotHelixResourceManager).addNewTableReplicationJob(eq(tableName), eq(jobId), anyLong(),
        resultCaptor.capture());

    List<String> capturedSegments = resultCaptor.getValue().getHistoricalSegments();
    Assert.assertEquals(capturedSegments.size(), 2);
    Assert.assertTrue(capturedSegments.contains("seg1"));
    Assert.assertTrue(capturedSegments.contains("seg2"));

    verify(_executorService, times(watermarks.size())).submit(any(Runnable.class));
  }
}
