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

package org.apache.pinot.core.data.manager.realtime;

import com.google.common.collect.ImmutableMap;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.server.realtime.ServerSegmentCompletionProtocolHandler;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SegmentCommitterFactoryTest {

  private Map<String, String> getMinimumStreamConfigMap() {
    return ImmutableMap.of(
        "streamType", "kafka",
        "stream.kafka.consumer.type", "simple",
        "stream.kafka.topic.name", "ignore",
        "stream.kafka.decoder.class.name", "org.apache.pinot.plugin.inputformat.json.JsonMessageDecoder");
  }

  private TableConfigBuilder createRealtimeTableConfig(String tableName) {
    return createRealtimeTableConfig(tableName, getMinimumStreamConfigMap());
  }

  private TableConfigBuilder createRealtimeTableConfig(String tableName, Map<String, String> realtimeStreamConfig) {
    return new TableConfigBuilder(TableType.REALTIME)
        .setTableName(tableName)
        .setLLC(true)
        .setStreamConfigs(realtimeStreamConfig);
  }

  @Test (description = "if controller doesn't support split commit, it should return default segment committer")
  public void testControllerNoSplit()
      throws URISyntaxException {
    TableConfig config = createRealtimeTableConfig("test").build();

    SegmentCommitterFactory factory = new SegmentCommitterFactory(Mockito.mock(Logger.class),
        Mockito.mock(ServerSegmentCompletionProtocolHandler.class), config,
        Mockito.mock(IndexLoadingConfig.class), Mockito.mock(ServerMetrics.class));
    SegmentCommitter committer = factory.createSegmentCommitter(false, null, null);
    Assert.assertNotNull(committer);
    Assert.assertTrue(committer instanceof DefaultSegmentCommitter);
  }

  @Test(description = "when controller supports split commit, server should always use split segment commit")
  public void testSplitSegmentCommitterIsDefault()
      throws URISyntaxException {
    TableConfig config = createRealtimeTableConfig("test").build();
    ServerSegmentCompletionProtocolHandler protocolHandler =
        new ServerSegmentCompletionProtocolHandler(Mockito.mock(ServerMetrics.class), "test_REALTIME");
    String controllerVipUrl = "http://localhost:1234";
    SegmentCompletionProtocol.Request.Params requestParams = new SegmentCompletionProtocol.Request.Params();
    SegmentCommitterFactory factory = new SegmentCommitterFactory(Mockito.mock(Logger.class), protocolHandler, config,
        Mockito.mock(IndexLoadingConfig.class), Mockito.mock(ServerMetrics.class));
    SegmentCommitter committer = factory.createSegmentCommitter(true, requestParams, controllerVipUrl);
    Assert.assertNotNull(committer);
    Assert.assertTrue(committer instanceof SplitSegmentCommitter);
  }

  @Test(description = "use upload to deepstore when either serverUploadToDeepStore is set or peer segment download "
      + "scheme is non-null")
  public void testUploadToDeepStoreConfig()
      throws URISyntaxException {
    ServerSegmentCompletionProtocolHandler protocolHandler =
        new ServerSegmentCompletionProtocolHandler(Mockito.mock(ServerMetrics.class), "test_REALTIME");
    String controllerVipUrl = "http://localhost:1234";
    SegmentCompletionProtocol.Request.Params requestParams = new SegmentCompletionProtocol.Request.Params();

    // No peer segment download scheme, serverUploadToDeepStore = true
    Map<String, String> streamConfigMap = new HashMap<>(getMinimumStreamConfigMap());
    streamConfigMap.put(StreamConfigProperties.SERVER_UPLOAD_TO_DEEPSTORE, "true");
    TableConfig config = createRealtimeTableConfig("testDeepStoreConfig", streamConfigMap).build();
    IndexLoadingConfig indexLoadingConfig = Mockito.mock(IndexLoadingConfig.class);
    Mockito.when(indexLoadingConfig.getSegmentStoreURI()).thenReturn("file:///path/to/segment/store.txt");

    SegmentCommitterFactory factory = new SegmentCommitterFactory(Mockito.mock(Logger.class), protocolHandler, config,
        indexLoadingConfig, Mockito.mock(ServerMetrics.class));
    SegmentCommitter committer = factory.createSegmentCommitter(true, requestParams, controllerVipUrl);
    Assert.assertNotNull(committer);
    Assert.assertTrue(committer instanceof SplitSegmentCommitter);
    Assert.assertTrue(((SplitSegmentCommitter) committer).getSegmentUploader() instanceof PinotFSSegmentUploader);

    // Peer segment download scheme is set, serverUploadToDeepStore = false (for backwards compatibility)
    Map<String, String> streamConfigMap1 = new HashMap<>(getMinimumStreamConfigMap());
    streamConfigMap1.put(StreamConfigProperties.SERVER_UPLOAD_TO_DEEPSTORE, "false");
    TableConfig config1 = createRealtimeTableConfig("testDeepStoreConfig", streamConfigMap1)
        .setPeerSegmentDownloadScheme("http")
        .build();

    factory = new SegmentCommitterFactory(Mockito.mock(Logger.class), protocolHandler, config1,
        indexLoadingConfig, Mockito.mock(ServerMetrics.class));
    committer = factory.createSegmentCommitter(true, requestParams, controllerVipUrl);
    Assert.assertNotNull(committer);
    Assert.assertTrue(committer instanceof SplitSegmentCommitter);
    Assert.assertTrue(((SplitSegmentCommitter) committer).getSegmentUploader() instanceof PinotFSSegmentUploader);
  }
}
