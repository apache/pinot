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

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.server.realtime.ServerSegmentCompletionProtocolHandler;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SegmentCommitterFactoryTest {
  private static final String BLOCKING_PINOT_FS_SCHEME = "factory-blocking";

  private Map<String, String> getMinimumStreamConfigMap() {
    return Map.of("streamType", "kafka", "stream.kafka.topic.name", "ignore", "stream.kafka.decoder.class.name",
        "org.apache.pinot.plugin.inputformat.json.JsonMessageDecoder");
  }

  private TableConfigBuilder createRealtimeTableConfig(String tableName) {
    return createRealtimeTableConfig(tableName, getMinimumStreamConfigMap());
  }

  private TableConfigBuilder createRealtimeTableConfig(String tableName, Map<String, String> realtimeStreamConfig) {
    return new TableConfigBuilder(TableType.REALTIME).setTableName(tableName).setStreamConfigs(realtimeStreamConfig);
  }

  @Test(description = "when controller supports split commit, server should always use split segment commit")
  public void testSplitSegmentCommitterIsDefault()
      throws URISyntaxException {
    TableConfig config = createRealtimeTableConfig("test").build();
    ServerSegmentCompletionProtocolHandler protocolHandler =
        new ServerSegmentCompletionProtocolHandler(Mockito.mock(ServerMetrics.class), "test_REALTIME");
    String controllerVipUrl = "http://localhost:1234";
    IndexLoadingConfig indexLoadingConfig = mockIndexLoadConfig();
    SegmentCompletionProtocol.Request.Params requestParams = new SegmentCompletionProtocol.Request.Params();
    SegmentCommitterFactory factory = new SegmentCommitterFactory(Mockito.mock(Logger.class), protocolHandler, config,
        indexLoadingConfig, Mockito.mock(ServerMetrics.class));
    SegmentCommitter committer = factory.createSegmentCommitter(requestParams, controllerVipUrl);
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
    // Create and set up the mocked IndexLoadingConfig and InstanceDataManager
    IndexLoadingConfig indexLoadingConfig = mockIndexLoadConfig();
    SegmentCommitterFactory factory = new SegmentCommitterFactory(Mockito.mock(Logger.class), protocolHandler, config,
        indexLoadingConfig, Mockito.mock(ServerMetrics.class));
    SegmentCommitter committer = factory.createSegmentCommitter(requestParams, controllerVipUrl);
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
    committer = factory.createSegmentCommitter(requestParams, controllerVipUrl);
    Assert.assertNotNull(committer);
    Assert.assertTrue(committer instanceof SplitSegmentCommitter);
    Assert.assertTrue(((SplitSegmentCommitter) committer).getSegmentUploader() instanceof PinotFSSegmentUploader);
  }

  @Test
  public void testReusesUploaderForOneSegmentBuildAndRetiresItForNextBuild()
      throws URISyntaxException {
    Map<String, String> streamConfigMap = new HashMap<>(getMinimumStreamConfigMap());
    streamConfigMap.put(StreamConfigProperties.SERVER_UPLOAD_TO_DEEPSTORE, "true");
    TableConfig config = createRealtimeTableConfig("testStableUploader", streamConfigMap).build();
    SegmentCommitterFactory factory = new SegmentCommitterFactory(Mockito.mock(Logger.class),
        new ServerSegmentCompletionProtocolHandler(Mockito.mock(ServerMetrics.class), "testStableUploader_REALTIME"),
        config, mockIndexLoadConfig(), Mockito.mock(ServerMetrics.class));
    SegmentCompletionProtocol.Request.Params requestParams = new SegmentCompletionProtocol.Request.Params();
    UUID firstBuildId = UUID.fromString("00000000-0000-0000-0000-000000000001");
    UUID secondBuildId = UUID.fromString("00000000-0000-0000-0000-000000000002");
    try {
      SplitSegmentCommitter firstCommitter = (SplitSegmentCommitter) factory.createSegmentCommitter(requestParams,
          "http://localhost:1234", firstBuildId);
      SplitSegmentCommitter retryCommitter = (SplitSegmentCommitter) factory.createSegmentCommitter(requestParams,
          "http://localhost:1234", firstBuildId);
      SplitSegmentCommitter rebuiltCommitter = (SplitSegmentCommitter) factory.createSegmentCommitter(requestParams,
          "http://localhost:1234", secondBuildId);

      Assert.assertSame(retryCommitter.getSegmentUploader(), firstCommitter.getSegmentUploader());
      Assert.assertNotSame(rebuiltCommitter.getSegmentUploader(), firstCommitter.getSegmentUploader());
    } finally {
      factory.close();
    }
  }

  @Test
  public void testBuildAwarePathPreservesCustomFactoryOverride()
      throws URISyntaxException {
    TableConfig config = createRealtimeTableConfig("testCustomFactory").build();
    SegmentCommitter expectedCommitter = Mockito.mock(SegmentCommitter.class);
    AtomicBoolean overrideCalled = new AtomicBoolean();
    SegmentCommitterFactory factory = new SegmentCommitterFactory(Mockito.mock(Logger.class),
        new ServerSegmentCompletionProtocolHandler(Mockito.mock(ServerMetrics.class), "testCustomFactory_REALTIME"),
        config, mockIndexLoadConfig(), Mockito.mock(ServerMetrics.class)) {
      @Override
      public SegmentCommitter createSegmentCommitter(SegmentCompletionProtocol.Request.Params params,
          String controllerVipUrl) {
        overrideCalled.set(true);
        return expectedCommitter;
      }
    };

    SegmentCommitter actualCommitter = factory.createSegmentCommitter(new SegmentCompletionProtocol.Request.Params(),
        "http://localhost:1234", UUID.fromString("00000000-0000-0000-0000-000000000003"));

    Assert.assertTrue(overrideCalled.get());
    Assert.assertSame(actualCommitter, expectedCommitter);
  }

  @Test
  public void testRetirementAndCloseInterruptBlockedUploaderWork()
      throws Exception {
    PinotFSFactory.register(BLOCKING_PINOT_FS_SCHEME, BlockingPinotFS.class.getName(), new PinotConfiguration());
    Map<String, String> streamConfigMap = new HashMap<>(getMinimumStreamConfigMap());
    streamConfigMap.put(StreamConfigProperties.SERVER_UPLOAD_TO_DEEPSTORE, "true");
    TableConfig config = createRealtimeTableConfig("testUploaderLifecycle", streamConfigMap).build();
    IndexLoadingConfig indexLoadingConfig = mockIndexLoadConfig();
    Mockito.when(indexLoadingConfig.getSegmentStoreURI()).thenReturn(BLOCKING_PINOT_FS_SCHEME + "://root");
    SegmentCommitterFactory factory = new SegmentCommitterFactory(Mockito.mock(Logger.class),
        new ServerSegmentCompletionProtocolHandler(Mockito.mock(ServerMetrics.class), "testUploaderLifecycle_REALTIME"),
        config, indexLoadingConfig, Mockito.mock(ServerMetrics.class));
    SegmentCompletionProtocol.Request.Params requestParams = new SegmentCompletionProtocol.Request.Params();
    LLCSegmentName segmentName = new LLCSegmentName("testUploaderLifecycle_REALTIME", 1, 0, 1234L);
    File segmentFile = File.createTempFile("test-uploader-lifecycle", ".tar");
    UUID firstBuildId = UUID.fromString("00000000-0000-0000-0000-000000000004");
    UUID secondBuildId = UUID.fromString("00000000-0000-0000-0000-000000000005");
    try {
      SplitSegmentCommitter firstCommitter = (SplitSegmentCommitter) factory.createSegmentCommitter(requestParams,
          "http://localhost:1234", firstBuildId);
      PinotFSSegmentUploader firstUploader = (PinotFSSegmentUploader) firstCommitter.getSegmentUploader();
      BlockingPinotFS.reset();
      CompletableFuture<URI> firstUpload = CompletableFuture.supplyAsync(
          () -> firstUploader.uploadSegment(segmentFile, segmentName, 60_000));
      Assert.assertTrue(BlockingPinotFS.awaitUploadStarted());

      SplitSegmentCommitter secondCommitter = (SplitSegmentCommitter) factory.createSegmentCommitter(requestParams,
          "http://localhost:1234", secondBuildId);
      Assert.assertTrue(BlockingPinotFS.awaitInterrupted());
      Assert.assertNull(firstUpload.get(5, TimeUnit.SECONDS));

      PinotFSSegmentUploader secondUploader = (PinotFSSegmentUploader) secondCommitter.getSegmentUploader();
      BlockingPinotFS.reset();
      CompletableFuture<URI> secondUpload = CompletableFuture.supplyAsync(
          () -> secondUploader.uploadSegment(segmentFile, segmentName, 60_000));
      Assert.assertTrue(BlockingPinotFS.awaitUploadStarted());

      factory.close();
      Assert.assertTrue(BlockingPinotFS.awaitInterrupted());
      Assert.assertNull(secondUpload.get(5, TimeUnit.SECONDS));
    } finally {
      factory.close();
      BlockingPinotFS.releaseUpload();
      FileUtils.deleteQuietly(segmentFile);
    }
  }

  private IndexLoadingConfig mockIndexLoadConfig() {
    IndexLoadingConfig indexLoadingConfig = Mockito.mock(IndexLoadingConfig.class);
    InstanceDataManagerConfig instanceDataManagerConfig = Mockito.mock(InstanceDataManagerConfig.class);
    Mockito.when(indexLoadingConfig.getInstanceDataManagerConfig()).thenReturn(instanceDataManagerConfig);
    PinotConfiguration pinotConfiguration = Mockito.mock(PinotConfiguration.class);
    Mockito.when(instanceDataManagerConfig.getConfig()).thenReturn(pinotConfiguration);

    return indexLoadingConfig;
  }

  /// Test filesystem that exposes when a factory-owned uploader starts and receives an interrupt.
  public static class BlockingPinotFS extends PinotFSSegmentUploaderTest.AlwaysSucceedPinotFS {
    private static volatile CountDownLatch _uploadStarted = new CountDownLatch(1);
    private static volatile CountDownLatch _releaseUpload = new CountDownLatch(1);
    private static volatile CountDownLatch _interrupted = new CountDownLatch(1);

    private static void reset() {
      _uploadStarted = new CountDownLatch(1);
      _releaseUpload = new CountDownLatch(1);
      _interrupted = new CountDownLatch(1);
    }

    private static boolean awaitUploadStarted()
        throws InterruptedException {
      return _uploadStarted.await(5, TimeUnit.SECONDS);
    }

    private static boolean awaitInterrupted()
        throws InterruptedException {
      return _interrupted.await(5, TimeUnit.SECONDS);
    }

    private static void releaseUpload() {
      _releaseUpload.countDown();
    }

    @Override
    public void copyFromLocalFile(File srcFile, URI dstUri)
        throws Exception {
      _uploadStarted.countDown();
      try {
        _releaseUpload.await();
      } catch (InterruptedException e) {
        _interrupted.countDown();
        throw e;
      }
    }
  }
}
