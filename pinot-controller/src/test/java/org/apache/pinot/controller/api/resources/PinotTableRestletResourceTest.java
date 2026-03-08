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
package org.apache.pinot.controller.api.resources;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.WatermarkInductionResult;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamMetadata;
import org.apache.pinot.spi.utils.JsonUtils;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class PinotTableRestletResourceTest {

  @Test
  public void testTweakRealtimeTableConfig() throws Exception {
    try (InputStream inputStream = Thread.currentThread().getContextClassLoader()
        .getResourceAsStream("table/table_config_with_instance_assignment.json")) {
      ObjectNode tableConfig = (ObjectNode) JsonUtils.inputStreamToJsonNode(inputStream);

      String brokerTenant = "testBroker";
      String serverTenant = "testServer";
      CopyTablePayload copyTablePayload = new CopyTablePayload("http://localhost:9000", null, brokerTenant,
          serverTenant, Map.of("server1_REALTIME", "testServer_REALTIME"));
      PinotTableRestletResource.tweakRealtimeTableConfig(tableConfig, copyTablePayload);

      assertEquals(tableConfig.get("tenants").get("broker").asText(), brokerTenant);
      assertEquals(tableConfig.get("tenants").get("server").asText(), serverTenant);
      assertEquals(tableConfig.path("instanceAssignmentConfigMap").path("CONSUMING").path("tagPoolConfig").path("tag")
          .asText(), serverTenant + "_REALTIME");
    }
  }

  @Test
  public void testGetStreamMetadataList()
      throws Exception {
    StreamConfig streamConfig0 = Mockito.mock(StreamConfig.class);
    StreamConfig streamConfig1 = Mockito.mock(StreamConfig.class);

    Map<Integer, Integer> streamPartitionCountMap = Map.of(0, 4, 1, 8);
    PinotLLCRealtimeSegmentManager realtimeSegmentManager = Mockito.mock(PinotLLCRealtimeSegmentManager.class);
    Mockito.when(realtimeSegmentManager.getPartitionCountMap(Mockito.anyList())).thenReturn(streamPartitionCountMap);
    PinotHelixResourceManager pinotHelixResourceManager = Mockito.mock(PinotHelixResourceManager.class);
    Mockito.when(pinotHelixResourceManager.getRealtimeSegmentManager()).thenReturn(realtimeSegmentManager);
    PinotTableRestletResource resource = new PinotTableRestletResource();
    resource._pinotHelixResourceManager = pinotHelixResourceManager;

    List<StreamMetadata> streamMetadataList = resource.getStreamMetadataList(List.of(streamConfig0, streamConfig1),
        new WatermarkInductionResult(List.of(
            new WatermarkInductionResult.Watermark(1, 3, 101L),
            new WatermarkInductionResult.Watermark(0, 2, 100L),
            new WatermarkInductionResult.Watermark(10000, 5, 200L))));

    assertEquals(streamMetadataList.size(), 2);

    // List is ordered by streamConfigIndex (0, 1)
    StreamMetadata streamMetadata0 = streamMetadataList.get(0);
    assertEquals(streamMetadata0.getStreamConfig(), streamConfig0);
    assertEquals(streamMetadata0.getNumPartitions(), 4);
    assertEquals(streamMetadata0.getPartitionGroupMetadataList().size(), 2);
    assertEquals(streamMetadata0.getPartitionGroupMetadataList().get(0).getPartitionGroupId(), 1);
    assertEquals(((LongMsgOffset) streamMetadata0.getPartitionGroupMetadataList().get(0).getStartOffset()).getOffset(),
        101L);
    assertEquals(streamMetadata0.getPartitionGroupMetadataList().get(0).getSequenceNumber(), 3);
    assertEquals(streamMetadata0.getPartitionGroupMetadataList().get(1).getPartitionGroupId(), 0);
    assertEquals(((LongMsgOffset) streamMetadata0.getPartitionGroupMetadataList().get(1).getStartOffset()).getOffset(),
        100L);
    assertEquals(streamMetadata0.getPartitionGroupMetadataList().get(1).getSequenceNumber(), 2);

    StreamMetadata streamMetadata1 = streamMetadataList.get(1);
    assertEquals(streamMetadata1.getStreamConfig(), streamConfig1);
    assertEquals(streamMetadata1.getNumPartitions(), 8);
    assertEquals(streamMetadata1.getPartitionGroupMetadataList().size(), 1);
    assertEquals(streamMetadata1.getPartitionGroupMetadataList().get(0).getPartitionGroupId(), 10000);
    assertEquals(((LongMsgOffset) streamMetadata1.getPartitionGroupMetadataList().get(0).getStartOffset()).getOffset(),
        200L);
    assertEquals(streamMetadata1.getPartitionGroupMetadataList().get(0).getSequenceNumber(), 5);
  }
}
