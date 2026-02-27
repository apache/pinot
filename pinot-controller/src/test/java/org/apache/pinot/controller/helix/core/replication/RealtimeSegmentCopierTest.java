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

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.resources.CopyTablePayload;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RealtimeSegmentCopierTest {

  @Mock
  private ControllerConf _controllerConf;
  @Mock
  private HttpClient _httpClient;
  @Mock
  private PinotFS _pinotFS;

  private RealtimeSegmentCopier _copier;
  private AutoCloseable _mocks;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
    when(_controllerConf.getDataDir()).thenReturn("hdfs://data");
    _copier = spy(new RealtimeSegmentCopier(_controllerConf, _httpClient));
  }

  @AfterMethod
  public void tearDown() throws Exception {
    _mocks.close();
  }

  @Test
  public void testCopy() throws Exception {
    String tableNameWithType = "table1_REALTIME";
    String segmentName = "seg1";
    CopyTablePayload payload = new CopyTablePayload("http://src", Collections.emptyMap(),
        "http://dest", Collections.emptyMap(), "broker", "server", Collections.emptyMap(), null, null);

    Map<String, String> metadata = new HashMap<>();
    metadata.put("segment.download.url", "hdfs://src/data/seg1");

    doReturn(_pinotFS).when(_copier).getPinotFS(any(URI.class));
    when(_pinotFS.exists(any(URI.class))).thenReturn(false);
    when(_pinotFS.copy(any(URI.class), any(URI.class))).thenReturn(true);

    SimpleHttpResponse response = mock(SimpleHttpResponse.class);
    when(response.getStatusCode()).thenReturn(200);
    when(response.getResponse()).thenReturn("{}");
    doReturn(response).when(_httpClient).sendRequest(any(ClassicHttpRequest.class), anyLong());

    _copier.copy(tableNameWithType, segmentName, payload, metadata);

    verify(_pinotFS).copy(any(URI.class), any(URI.class));
    verify(_httpClient).sendRequest(any(ClassicHttpRequest.class), anyLong());
  }

  @Test
  public void testCopyExisting() throws Exception {
    String tableNameWithType = "table1_REALTIME";
    String segmentName = "seg1";
    CopyTablePayload payload = new CopyTablePayload("http://src", Collections.emptyMap(),
        "http://dest", Collections.emptyMap(), "broker", "server", Collections.emptyMap(), null, null);

    Map<String, String> metadata = new HashMap<>();
    metadata.put("segment.download.url", "hdfs://src/data/seg1");

    doReturn(_pinotFS).when(_copier).getPinotFS(any(URI.class));
    when(_pinotFS.exists(any(URI.class))).thenReturn(true);

    SimpleHttpResponse response = mock(SimpleHttpResponse.class);
    when(response.getStatusCode()).thenReturn(200);
    when(response.getResponse()).thenReturn("{}");
    doReturn(response).when(_httpClient).sendRequest(any(ClassicHttpRequest.class), anyLong());

    _copier.copy(tableNameWithType, segmentName, payload, metadata);

    verify(_pinotFS, never()).copy(any(URI.class), any(URI.class));
    verify(_httpClient).sendRequest(any(ClassicHttpRequest.class), anyLong());
  }
}
