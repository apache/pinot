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
package org.apache.pinot.server.realtime;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


/// Tests recovery from metadata-upload failures without making controller HTTP requests or changing singleton state.
public class ServerSegmentCompletionProtocolHandlerTest {
  @DataProvider(name = "uploadFailure")
  public Object[][] uploadFailure() {
    return new Object[][]{{true}, {false}};
  }

  @Test(dataProvider = "uploadFailure")
  public void testMetadataUploadFailureInvalidatesLeaderAndReturnsNotSent(boolean transportFailure)
      throws Exception {
    ControllerLeaderLocator locator = mock(ControllerLeaderLocator.class);
    ServerMetrics metrics = mock(ServerMetrics.class);
    Map<String, File> metadata = Map.of("metadata.properties", new File("metadata.properties"));
    when(locator.getControllerLeader("events")).thenReturn(Pair.of("controller", 9000));
    try (MockedStatic<ControllerLeaderLocator> singleton = mockStatic(ControllerLeaderLocator.class);
        MockedConstruction<FileUploadDownloadClient> clients = mockConstruction(FileUploadDownloadClient.class,
            (client, context) -> {
              if (transportFailure) {
                when(client.uploadSegmentMetadataFiles(any(URI.class), eq(metadata), any(), isNull(), anyInt()))
                    .thenThrow(new IOException("controller unavailable"));
              } else {
                when(client.uploadSegmentMetadataFiles(any(URI.class), eq(metadata), any(), isNull(), anyInt()))
                    .thenReturn(new SimpleHttpResponse(200, "not a protocol response"));
              }
            })) {
      singleton.when(ControllerLeaderLocator::getInstance).thenReturn(locator);
      ServerSegmentCompletionProtocolHandler handler =
          new ServerSegmentCompletionProtocolHandler(metrics, "events_REALTIME");
      SegmentCompletionProtocol.Response response = handler.segmentCommitEndWithMetadata(params(), metadata);
      assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.NOT_SENT);
      verify(locator).invalidateCachedControllerLeader();
      verify(clients.constructed().get(0))
          .uploadSegmentMetadataFiles(any(URI.class), eq(metadata), any(), isNull(), anyInt());
      verify(metrics).addMeteredGlobalValue(ServerMeter.LLC_CONTROLLER_RESPONSE_NOT_SENT, 1);
    }
  }

  @Test
  public void testSuccessfulMetadataCommitKeepsTheCachedLeader()
      throws Exception {
    ControllerLeaderLocator locator = mock(ControllerLeaderLocator.class);
    ServerMetrics metrics = mock(ServerMetrics.class);
    Map<String, File> metadata = Map.of("metadata.properties", new File("metadata.properties"));
    when(locator.getControllerLeader("events")).thenReturn(Pair.of("controller", 9000));
    try (MockedStatic<ControllerLeaderLocator> singleton = mockStatic(ControllerLeaderLocator.class);
        MockedConstruction<FileUploadDownloadClient> clients = mockConstruction(FileUploadDownloadClient.class,
            (client, context) -> when(client.uploadSegmentMetadataFiles(any(URI.class), eq(metadata), any(), isNull(),
                anyInt())).thenReturn(new SimpleHttpResponse(200,
                    SegmentCompletionProtocol.RESP_COMMIT_SUCCESS.toJsonString())))) {
      singleton.when(ControllerLeaderLocator::getInstance).thenReturn(locator);
      ServerSegmentCompletionProtocolHandler handler =
          new ServerSegmentCompletionProtocolHandler(metrics, "events_REALTIME");
      assertEquals(handler.segmentCommitEndWithMetadata(params(), metadata).getStatus(),
          SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS);
      verify(locator, never()).invalidateCachedControllerLeader();
      verify(metrics).addMeteredGlobalValue(ServerMeter.LLC_CONTROLLER_RESPONSE_COMMIT_SUCCESS, 1);
    }
  }

  private static SegmentCompletionProtocol.Request.Params params() {
    return new SegmentCompletionProtocol.Request.Params().withInstanceId("Server_localhost_8000")
        .withSegmentName("events__0__0__20261030T0000Z").withStreamPartitionMsgOffset("42");
  }
}
