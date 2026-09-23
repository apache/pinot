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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import java.io.File;
import java.nio.file.Files;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.segment.local.dedup.DedupContext;
import org.apache.pinot.segment.local.dedup.PartitionDedupMetadataManager;
import org.apache.pinot.segment.local.dedup.TableDedupMetadataManager;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.TableUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.UpsertContext;
import org.apache.pinot.segment.local.utils.SegmentLocks;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig.ConsistencyMode;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConsumerFactoryProvider;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class RealtimeTableDataManagerTest {
  @Test
  public void testStopBeforeConsumptionStarts()
      throws Exception {
    RealtimeSegmentDataManager segment = mock(RealtimeSegmentDataManager.class);
    doCallRealMethod().when(segment).stop();
    segment.stop();
  }

  @DataProvider
  public Object[][] upsertEnabled() {
    return new Object[][]{{false}, {true}};
  }

  @Test(dataProvider = "upsertEnabled")
  // Verify the existing gauge API used by consuming admission and segment cleanup.
  @SuppressWarnings("deprecation")
  public void testConsumingSegmentConstructedDuringShutdownIsDestroyed(boolean upsertEnabled)
      throws Exception {
    ServerMetrics.register(mock(ServerMetrics.class));
    ServerMetrics metrics = ServerMetrics.get();
    clearInvocations(metrics);
    File indexDir = Files.createTempDirectory("consuming-shutdown").toFile();
    RealtimeSegmentDataManager segment = mock(RealtimeSegmentDataManager.class);
    CountDownLatch constructed = new CountDownLatch(1);
    CountDownLatch finishConstruction = new CountDownLatch(1);
    LifecycleTableDataManager table = new LifecycleTableDataManager(indexDir, segment, upsertEnabled, () -> {
      constructed.countDown();
      await(finishConstruction);
    });
    FutureTask<Void> addition = new FutureTask<>(() -> {
      table.addConsumingSegment(LifecycleTableDataManager.SEGMENT_NAME);
      return null;
    });
    Thread additionThread = new Thread(addition, "construct-consuming-segment");
    try {
      additionThread.start();
      await(constructed, addition);
      table.shutDown();
      finishConstruction.countDown();
      ExecutionException failure = expectThrows(ExecutionException.class, () -> addition.get(10, TimeUnit.SECONDS));
      assertTrue(failure.getCause() instanceof IllegalStateException);
      assertEquals(table.getNumSegments(), 0);
      verify(segment).destroy();
      verify(segment, never()).startConsumption();
      verify(metrics, never()).addValueToTableGauge(eq(table.getTableName()), eq(ServerGauge.SEGMENT_COUNT), anyLong());
      verify(table._partitionUpsertMetadataManager, never()).trackSegmentForUpsertView(any());
      verify(table._partitionUpsertMetadataManager, never()).trackNewlyAddedSegment(anyString());
      if (upsertEnabled) {
        verify(table._upsertMetadataManager).stop();
        verify(table._upsertMetadataManager).close();
      }
    } finally {
      finishConstruction.countDown();
      additionThread.join(10000);
      FileUtils.deleteDirectory(indexDir);
    }
  }

  @Test(dataProvider = "upsertEnabled")
  public void testShutdownWaitsForConsumingSegmentStart(boolean upsertEnabled)
      throws Exception {
    ServerMetrics.register(mock(ServerMetrics.class));
    File indexDir = Files.createTempDirectory("consuming-start").toFile();
    RealtimeSegmentDataManager segment = mock(RealtimeSegmentDataManager.class);
    MutableSegment mutableSegment = mock(MutableSegment.class);
    when(segment.getSegment()).thenReturn(mutableSegment);
    when(mutableSegment.getSegmentMetadata()).thenReturn(mock(SegmentMetadata.class));
    when(segment.decreaseReferenceCount()).thenReturn(true);
    CountDownLatch startEntered = new CountDownLatch(1);
    CountDownLatch finishStart = new CountDownLatch(1);
    doAnswer(invocation -> {
      startEntered.countDown();
      await(finishStart);
      return null;
    }).when(segment).startConsumption();
    LifecycleTableDataManager table = new LifecycleTableDataManager(indexDir, segment, upsertEnabled, () -> { });
    FutureTask<Void> addition = new FutureTask<>(() -> {
      table.addConsumingSegment(LifecycleTableDataManager.SEGMENT_NAME);
      return null;
    });
    FutureTask<Void> shutdown = new FutureTask<>(() -> {
      table.shutDown();
      return null;
    });
    Thread additionThread = new Thread(addition, "start-consuming-segment");
    Thread shutdownThread = new Thread(shutdown, "shutdown-consuming-table");
    try {
      additionThread.start();
      await(startEntered, addition);
      shutdownThread.start();
      TestUtils.waitForCondition(ignored -> shutdownThread.getState() == Thread.State.BLOCKED || shutdown.isDone(),
          10, 10000, "Shutdown did not reach the consuming-segment admission monitor");
      assertFalse(shutdown.isDone(), "Shutdown must not offload a consumer before it has started");
      verify(segment, never()).offload();
      finishStart.countDown();
      addition.get(10, TimeUnit.SECONDS);
      shutdown.get(10, TimeUnit.SECONDS);
      verify(segment).offload();
      verify(segment).destroy();
      assertEquals(table.getNumSegments(), 0);
    } finally {
      finishStart.countDown();
      additionThread.join(10000);
      shutdownThread.join(10000);
      FileUtils.deleteDirectory(indexDir);
    }
  }

  @DataProvider
  public Object[][] upsertConsistencyModes() {
    return new Object[][]{{ConsistencyMode.NONE}, {ConsistencyMode.SYNC}, {ConsistencyMode.SNAPSHOT}};
  }

  @Test(dataProvider = "upsertEnabled")
  public void testOnlinePreloadDoesNotCreatePartitionAfterShutdown(boolean upsertEnabled)
      throws Exception {
    ServerMetrics.register(mock(ServerMetrics.class));
    File indexDir = Files.createTempDirectory("online-preload-shutdown").toFile();
    LifecycleTableDataManager table = spy(
        new LifecycleTableDataManager(indexDir, mock(RealtimeSegmentDataManager.class), upsertEnabled, () -> { }));
    TableDedupMetadataManager dedup = mock(TableDedupMetadataManager.class);
    PartitionDedupMetadataManager partitionDedup = mock(PartitionDedupMetadataManager.class);
    if (upsertEnabled) {
      when(table._upsertMetadataManager.getContext().isPreloadEnabled()).thenReturn(true);
    } else {
      FieldUtils.writeField(table, "_tableDedupMetadataManager", dedup, true);
      DedupContext context = mock(DedupContext.class);
      when(dedup.getContext()).thenReturn(context);
      when(context.isPreloadEnabled()).thenReturn(true);
      when(dedup.getOrCreatePartitionManager(0)).thenReturn(partitionDedup);
    }
    SegmentZKMetadata metadata = new SegmentZKMetadata(LifecycleTableDataManager.SEGMENT_NAME);
    metadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    doReturn(metadata).when(table).fetchZKMetadata(LifecycleTableDataManager.SEGMENT_NAME);
    CountDownLatch loadingConfig = new CountDownLatch(1);
    CountDownLatch finishLoadingConfig = new CountDownLatch(1);
    doAnswer(invocation -> {
      loadingConfig.countDown();
      await(finishLoadingConfig);
      return invocation.callRealMethod();
    }).when(table).fetchIndexLoadingConfig();
    FutureTask<Void> addition = new FutureTask<>(() -> {
      table.addOnlineSegment(LifecycleTableDataManager.SEGMENT_NAME);
      return null;
    });
    Thread additionThread = new Thread(addition, "preload-online-segment");
    try {
      additionThread.start();
      await(loadingConfig, addition);
      table.shutDown();
      finishLoadingConfig.countDown();
      ExecutionException failure = expectThrows(ExecutionException.class, () -> addition.get(10, TimeUnit.SECONDS));
      verify(table._upsertMetadataManager, never()).getOrCreatePartitionManager(0);
      verify(table._partitionUpsertMetadataManager, never()).preloadSegments(any());
      verify(dedup, never()).getOrCreatePartitionManager(0);
      verify(partitionDedup, never()).preloadSegments(any());
      assertTrue(failure.getCause() instanceof IllegalStateException);
      assertEquals(table.getNumSegments(), 0);
    } finally {
      finishLoadingConfig.countDown();
      additionThread.join(10000);
      FileUtils.deleteDirectory(indexDir);
    }
  }

  @Test(dataProvider = "upsertConsistencyModes")
  public void testShutdownWaitsForUpsertSegmentReplacement(ConsistencyMode consistencyMode)
      throws Exception {
    ServerMetrics.register(mock(ServerMetrics.class));
    File indexDir = Files.createTempDirectory("upsert-replacement-shutdown").toFile();
    LifecycleTableDataManager table =
        new LifecycleTableDataManager(indexDir, mock(RealtimeSegmentDataManager.class), true, () -> { });
    when(table._upsertMetadataManager.getContext().getConsistencyMode()).thenReturn(consistencyMode);
    ImmutableSegment oldSegment = immutableSegment(LifecycleTableDataManager.SEGMENT_NAME);
    ImmutableSegment newSegment = immutableSegment(LifecycleTableDataManager.SEGMENT_NAME);
    table.addSegment(oldSegment, null);
    CountDownLatch replacementEntered = new CountDownLatch(1);
    CountDownLatch finishReplacement = new CountDownLatch(1);
    doAnswer(invocation -> {
      replacementEntered.countDown();
      await(finishReplacement);
      return null;
    }).when(table._partitionUpsertMetadataManager).replaceSegment(newSegment, oldSegment);
    FutureTask<Void> replacement = new FutureTask<>(() -> {
      table.addSegment(newSegment, null);
      return null;
    });
    FutureTask<Void> shutdown = new FutureTask<>(() -> {
      table.shutDown();
      return null;
    });
    Thread replacementThread = new Thread(replacement, "replace-upsert-segment");
    Thread shutdownThread = new Thread(shutdown, "shutdown-upsert-table");
    try {
      replacementThread.start();
      await(replacementEntered, replacement);
      assertEquals(table.getSegmentDataManager(LifecycleTableDataManager.SEGMENT_NAME).hasMultiSegments(),
          consistencyMode != ConsistencyMode.NONE);
      shutdownThread.start();
      TestUtils.waitForCondition(ignored -> table.isShutDown()
              && (shutdownThread.getState() == Thread.State.WAITING || shutdown.isDone()), 10, 10000,
          "Shutdown did not wait for the admitted upsert replacement");
      assertFalse(shutdown.isDone(), "Shutdown must wait for the complete upsert replacement");
      verify(table._upsertMetadataManager, never()).stop();
      verify(oldSegment, never()).destroy();
      verify(newSegment, never()).destroy();
      finishReplacement.countDown();
      replacement.get(10, TimeUnit.SECONDS);
      shutdown.get(10, TimeUnit.SECONDS);
      verify(oldSegment).offload();
      verify(oldSegment).destroy();
      verify(newSegment).offload();
      verify(newSegment).destroy();
      verify(table._upsertMetadataManager).stop();
      verify(table._upsertMetadataManager).close();
      assertEquals(table.getNumSegments(), 0);

      ImmutableSegment lateSegment = immutableSegment("lifecycle__1__0__1");
      expectThrows(IllegalStateException.class, () -> table.addSegment(lateSegment, null));
      verify(lateSegment).destroy();
      verify(table._upsertMetadataManager, never()).getOrCreatePartitionManager(1);
      assertEquals(table.getNumSegments(), 0);
    } finally {
      finishReplacement.countDown();
      replacementThread.join(10000);
      shutdownThread.join(10000);
      FileUtils.deleteDirectory(indexDir);
    }
  }

  private static ImmutableSegment immutableSegment(String segmentName) {
    ImmutableSegment segment = mock(ImmutableSegment.class);
    SegmentMetadata metadata = mock(SegmentMetadata.class);
    when(segment.getSegmentName()).thenReturn(segmentName);
    when(segment.getSegmentMetadata()).thenReturn(metadata);
    when(metadata.getName()).thenReturn(segmentName);
    return segment;
  }

  private static void await(CountDownLatch latch, FutureTask<?> operation)
      throws Exception {
    TestUtils.waitForCondition(ignored -> latch.getCount() == 0 || operation.isDone(), 10, 10000,
        "Consuming segment operation did not reach the lifecycle barrier");
    if (operation.isDone()) {
      operation.get();
    }
    assertEquals(latch.getCount(), 0L);
  }

  private static void await(CountDownLatch latch) {
    try {
      assertTrue(latch.await(10, TimeUnit.SECONDS), "Timed out waiting for consuming segment lifecycle operation");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  /// Exercises public segment admission and shutdown while controlling construction, startup and upsert replacement.
  private static class LifecycleTableDataManager extends RealtimeTableDataManager {
    private static final String SEGMENT_NAME = "lifecycle__0__0__1";
    private final TableConfig _tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName("lifecycle").build();
    private final Schema _schema = new Schema.SchemaBuilder().setSchemaName("lifecycle")
        .addSingleValueDimension("value", DataType.STRING).build();
    private final RealtimeSegmentDataManager _segment;
    private final Runnable _beforeConstructionReturns;
    private final PartitionUpsertMetadataManager _partitionUpsertMetadataManager =
        mock(PartitionUpsertMetadataManager.class);
    private final TableUpsertMetadataManager _upsertMetadataManager = mock(TableUpsertMetadataManager.class);

    LifecycleTableDataManager(File indexDir, RealtimeSegmentDataManager segment, boolean upsertEnabled,
        Runnable beforeConstructionReturns)
        throws IllegalAccessException {
      super(null);
      _indexDir = indexDir;
      _tableNameWithType = _tableConfig.getTableName();
      _cachedTableConfigAndSchema = Pair.of(_tableConfig, _schema);
      _logger = LoggerFactory.getLogger(LifecycleTableDataManager.class);
      _segmentLocks = new SegmentLocks();
      _recentlyDeletedSegments = CacheBuilder.newBuilder().build();
      _segment = segment;
      _beforeConstructionReturns = beforeConstructionReturns;
      FieldUtils.writeField(this, "_ingestionDelayTracker", mock(IngestionDelayTracker.class), true);
      if (upsertEnabled) {
        _tableUpsertMetadataManager = _upsertMetadataManager;
        when(_tableUpsertMetadataManager.getContext()).thenReturn(mock(UpsertContext.class));
        when(_tableUpsertMetadataManager.getOrCreatePartitionManager(0)).thenReturn(_partitionUpsertMetadataManager);
      }
    }

    @Override
    public SegmentZKMetadata fetchZKMetadata(String segmentName) {
      SegmentZKMetadata metadata = new SegmentZKMetadata(segmentName);
      metadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
      return metadata;
    }

    @Override
    public IndexLoadingConfig fetchIndexLoadingConfig() {
      return new IndexLoadingConfig(_tableConfig, _schema);
    }

    @Override
    protected RealtimeSegmentDataManager createRealtimeSegmentDataManager(SegmentZKMetadata zkMetadata,
        TableConfig tableConfig, IndexLoadingConfig indexLoadingConfig, Schema schema, LLCSegmentName llcSegmentName,
        ConsumerCoordinator consumerCoordinator, PartitionUpsertMetadataManager partitionUpsertMetadataManager,
        PartitionDedupMetadataManager partitionDedupMetadataManager, BooleanSupplier isTableReadyToConsumeData) {
      _beforeConstructionReturns.run();
      return _segment;
    }
  }

  @Test
  public void testSetDefaultTimeValueIfInvalid() {
    SegmentZKMetadata segmentZKMetadata = mock(SegmentZKMetadata.class);
    long currentTimeMs = System.currentTimeMillis();
    when(segmentZKMetadata.getCreationTime()).thenReturn(currentTimeMs);

    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setTimeColumnName("timeColumn").build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable")
        .addDateTime("timeColumn", FieldSpec.DataType.TIMESTAMP, "TIMESTAMP", "1:MILLISECONDS").build();
    RealtimeTableDataManager.setDefaultTimeValueIfInvalid(tableConfig, schema, segmentZKMetadata);
    DateTimeFieldSpec timeFieldSpec = schema.getSpecForTimeColumn("timeColumn");
    assertNotNull(timeFieldSpec);
    assertEquals(timeFieldSpec.getDefaultNullValue(), currentTimeMs);

    schema = new Schema.SchemaBuilder().setSchemaName("testTable")
        .addDateTime("timeColumn", FieldSpec.DataType.INT, "SIMPLE_DATE_FORMAT|yyyyMMdd", "1:DAYS").build();
    RealtimeTableDataManager.setDefaultTimeValueIfInvalid(tableConfig, schema, segmentZKMetadata);
    timeFieldSpec = schema.getSpecForTimeColumn("timeColumn");
    assertNotNull(timeFieldSpec);
    assertEquals(timeFieldSpec.getDefaultNullValue(),
        Integer.parseInt(DateTimeFormat.forPattern("yyyyMMdd").withZone(DateTimeZone.UTC).print(currentTimeMs)));
  }

  @Test
  public void testStreamMetadataProviderCache() {
    class FakeRealtimeTableDataManager extends RealtimeTableDataManager {
      private boolean _cacheEntryRemovalNotified;
      public FakeRealtimeTableDataManager(Semaphore segmentBuildSemaphore) {
        super(segmentBuildSemaphore);
        _streamMetadataProviderCache = getStreamMetadataProviderCache();
      }
      public void updateCache() {
        _streamMetadataProviderCache = CacheBuilder.newBuilder()
            .expireAfterAccess(Duration.ofMillis(1))
            .removalListener((RemovalNotification<String, StreamMetadataProvider> notification) -> {
              StreamMetadataProvider provider = notification.getValue();
              if (provider != null) {
                try {
                  provider.close();
                  _cacheEntryRemovalNotified = true;
                } catch (Exception e) {
                  LOGGER.warn("Failed to close StreamMetadataProvider for key {}", notification.getKey(), e);
                }
              }
            })
            .build();
      }
      public Cache<String, StreamMetadataProvider> getCache() {
        return _streamMetadataProviderCache;
      }
    }
    FakeRealtimeTableDataManager fakeRealtimeTableDataManager = new FakeRealtimeTableDataManager(null);

    RealtimeSegmentDataManager mockRealtimeSegmentDataManager = Mockito.mock(RealtimeSegmentDataManager.class);
    when(mockRealtimeSegmentDataManager.getTableStreamName()).thenReturn("testTable-testTopic");
    StreamConfig streamConfig = FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs();
    when(mockRealtimeSegmentDataManager.getStreamConsumerFactory()).thenReturn(
        StreamConsumerFactoryProvider.create(streamConfig));

    StreamMetadataProvider streamMetadataProvider =
        fakeRealtimeTableDataManager.getStreamMetadataProvider(mockRealtimeSegmentDataManager);
    Assert.assertEquals(fakeRealtimeTableDataManager.getStreamMetadataProvider(mockRealtimeSegmentDataManager),
        streamMetadataProvider);

    fakeRealtimeTableDataManager.updateCache();
    Assert.assertEquals(fakeRealtimeTableDataManager.getCache().size(), 0);

    StreamMetadataProvider streamMetadataProvider1 =
        fakeRealtimeTableDataManager.getStreamMetadataProvider(mockRealtimeSegmentDataManager);
    Assert.assertNotEquals(streamMetadataProvider, streamMetadataProvider1);

    TestUtils.waitForCondition(
        aVoid -> !(fakeRealtimeTableDataManager.getStreamMetadataProvider(mockRealtimeSegmentDataManager)
            .equals(streamMetadataProvider1)), 5, 2000, "streamMetadataProvider returned from cache must be new.");
    Assert.assertTrue(fakeRealtimeTableDataManager._cacheEntryRemovalNotified);
  }

  @Test
  public void testEnforceConsumptionInOrderRuntimeOverride() {
    RealtimeTableDataManager realtimeTableDataManager = new RealtimeTableDataManager(null);

    Assert.assertFalse(realtimeTableDataManager.isEnforceConsumptionInOrderEnabled());

    realtimeTableDataManager.setEnforceConsumptionInOrder(true);
    Assert.assertTrue(realtimeTableDataManager.isEnforceConsumptionInOrderEnabled());

    realtimeTableDataManager.setEnforceConsumptionInOrder(false);
    Assert.assertFalse(realtimeTableDataManager.isEnforceConsumptionInOrderEnabled());
  }
}
