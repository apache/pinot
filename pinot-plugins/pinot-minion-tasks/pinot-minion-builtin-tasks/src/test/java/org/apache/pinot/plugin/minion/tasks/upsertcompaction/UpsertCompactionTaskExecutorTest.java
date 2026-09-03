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
package org.apache.pinot.plugin.minion.tasks.upsertcompaction;

import java.io.File;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.pinot.common.metrics.MinionMeter;
import org.apache.pinot.common.metrics.MinionMetrics;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.common.MinionConstants.UpsertCompactionTask;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.minion.MinionContext;
import org.apache.pinot.minion.event.MinionEventObserver;
import org.apache.pinot.plugin.minion.tasks.MinionTaskTestUtils;
import org.apache.pinot.plugin.minion.tasks.MinionTaskUtils;
import org.apache.pinot.plugin.minion.tasks.SegmentConversionResult;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.Enablement;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.roaringbitmap.RoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class UpsertCompactionTaskExecutorTest {
  private static final String REALTIME_TABLE_NAME = "testTable_REALTIME";
  private static final String SEGMENT_NAME = "testSegment";
  private static final String CLUSTER_NAME = "testCluster";
  private static final String TASK_TYPE = UpsertCompactionTask.TASK_TYPE;
  private static final String EXPECTED_CRC = "1000";
  private static final String DATA_CRC = "5000";

  private MinionMetrics _minionMetrics;
  private AtomicReference<MinionMetrics> _minionMetricsInstance;
  private MinionMetrics _previousMinionMetrics;
  private MinionEventObserver _eventObserver;
  private File _tempDir;

  @BeforeMethod
  public void setUp()
      throws Exception {
    _minionMetrics = mock(MinionMetrics.class);
    // Force the process-global singleton so BaseTaskExecutor picks up the mock.
    // register() only wins when current is NOOP; tests may run after other classes registered.
    _minionMetricsInstance = minionMetricsInstance();
    _previousMinionMetrics = _minionMetricsInstance.get();
    _minionMetricsInstance.set(_minionMetrics);

    _eventObserver = MinionTaskTestUtils.getMinionProgressObserver();
    _tempDir = new File(FileUtils.getTempDirectory(), "UpsertCompactionTaskExecutorTest-" + System.nanoTime());
    Assert.assertTrue(_tempDir.mkdirs());
  }

  @AfterMethod(alwaysRun = true)
  public void tearDown() {
    // Put back whatever was registered before this class ran. register() only swaps away from NOOP, so a
    // leaked mock would stick for every later test sharing the JVM.
    if (_minionMetricsInstance != null) {
      _minionMetricsInstance.set(_previousMinionMetrics);
    }
    FileUtils.deleteQuietly(_tempDir);
  }

  /// The only place that reflects on the private `MinionMetrics.MINION_METRICS_INSTANCE` holder, so a future
  /// JDK encapsulation change breaks here and nowhere else.
  @SuppressWarnings("unchecked")
  private static AtomicReference<MinionMetrics> minionMetricsInstance()
      throws Exception {
    Field field = MinionMetrics.class.getDeclaredField("MINION_METRICS_INSTANCE");
    field.setAccessible(true);
    return (AtomicReference<MinionMetrics>) field.get(null);
  }

  @Test
  public void testGetServers() {
    ExternalView externalView = new ExternalView(REALTIME_TABLE_NAME);
    Map<String, Map<String, String>> externalViewSegmentAssignment = externalView.getRecord().getMapFields();
    Map<String, String> map = new HashMap<>();
    map.put("server1", SegmentStateModel.ONLINE);
    externalViewSegmentAssignment.put(SEGMENT_NAME, map);
    HelixAdmin clusterManagementTool = Mockito.mock(HelixAdmin.class);
    MinionContext minionContext = MinionContext.getInstance();
    Mockito.when(clusterManagementTool.getResourceExternalView(CLUSTER_NAME, REALTIME_TABLE_NAME))
        .thenReturn(externalView);
    HelixManager helixManager = Mockito.mock(HelixManager.class);
    Mockito.when(helixManager.getClusterName()).thenReturn(CLUSTER_NAME);
    Mockito.when(helixManager.getClusterManagmentTool()).thenReturn(clusterManagementTool);
    minionContext.setHelixManager(helixManager);

    List<String> servers = MinionTaskUtils.getServers(SEGMENT_NAME, REALTIME_TABLE_NAME,
        helixManager.getClusterManagmentTool(), helixManager.getClusterName());

    Assert.assertEquals(servers.get(0), "server1");

    // verify exception thrown with OFFLINE server
    map.put("server1", SegmentStateModel.OFFLINE);
    Assert.assertThrows(IllegalStateException.class,
        () -> MinionTaskUtils.getServers(SEGMENT_NAME, REALTIME_TABLE_NAME,
            helixManager.getClusterManagmentTool(), helixManager.getClusterName()));
  }

  @Test
  public void testValidateDeepStoreCrcMatch() {
    TestableExecutor executor = newExecutor();
    executor.validateDeepStoreCrc(REALTIME_TABLE_NAME, SEGMENT_NAME, EXPECTED_CRC, EXPECTED_CRC, DATA_CRC, false);
    verify(_minionMetrics, never()).addMeteredTableValue(anyString(), eq(MinionMeter.CRC_MISMATCH_DEEPSTORE),
        anyLong());
  }

  @Test
  public void testValidateDeepStoreCrcMismatchThrowsAndMeters() {
    TestableExecutor executor = newExecutor();
    executor._zkDataCrc = -1;
    try {
      executor.validateDeepStoreCrc(REALTIME_TABLE_NAME, SEGMENT_NAME, EXPECTED_CRC, "9999", DATA_CRC, false);
      Assert.fail("expected IllegalStateException");
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("Crc mismatched"));
    }
    verify(_minionMetrics).addMeteredTableValue(REALTIME_TABLE_NAME, MinionMeter.CRC_MISMATCH_DEEPSTORE, 1L);
  }

  @Test
  public void testValidateDeepStoreCrcDataCrcFallback() {
    // Segment CRCs differ but both sides report the same data CRC → match (index-only drift).
    TestableExecutor executor = newExecutor();
    executor._zkDataCrc = Long.parseLong(DATA_CRC);
    executor.validateDeepStoreCrc(REALTIME_TABLE_NAME, SEGMENT_NAME, EXPECTED_CRC, "9999", DATA_CRC, false);
    verify(_minionMetrics, never()).addMeteredTableValue(anyString(), eq(MinionMeter.CRC_MISMATCH_DEEPSTORE),
        anyLong());
  }

  @Test
  public void testValidateDeepStoreCrcIgnoreMismatch() {
    TestableExecutor executor = newExecutor();
    executor._zkDataCrc = -1;
    executor.validateDeepStoreCrc(REALTIME_TABLE_NAME, SEGMENT_NAME, EXPECTED_CRC, "9999", DATA_CRC, true);
    verify(_minionMetrics, never()).addMeteredTableValue(anyString(), eq(MinionMeter.CRC_MISMATCH_DEEPSTORE),
        anyLong());
  }

  @Test
  public void testFetchValidDocIdsRetrySucceedsAfterTransientCrcMismatch()
      throws Exception {
    TestableExecutor executor = newExecutor();
    executor._validDocIdsFetchMaxAttempts = 3;
    executor._validDocIdsFetchRetryDelayMs = 0L;
    RoaringBitmap bitmap = RoaringBitmap.bitmapOf(0, 1, 2);

    AtomicInteger calls = new AtomicInteger();
    try (MockedStatic<MinionTaskUtils> mocked = Mockito.mockStatic(MinionTaskUtils.class, Mockito.CALLS_REAL_METHODS)) {
      mocked.when(
              () -> MinionTaskUtils.getValidDocIdFromServerMatchingCrc(anyString(), anyString(), anyString(), any(),
                  anyString(), anyString(), anyString()))
          .thenAnswer(inv -> {
            if (calls.getAndIncrement() == 0) {
              throw new IllegalStateException("CRC mismatch for segment: " + SEGMENT_NAME);
            }
            return bitmap;
          });

      RoaringBitmap result =
          executor.fetchValidDocIdsWithRetry(createTaskConfig(false), REALTIME_TABLE_NAME, SEGMENT_NAME, "SNAPSHOT",
              EXPECTED_CRC, DATA_CRC, "EQUAL");
      Assert.assertEquals(result, bitmap);
      Assert.assertEquals(calls.get(), 2);
      verify(_minionMetrics, never()).addMeteredTableValue(anyString(), eq(MinionMeter.CRC_MISMATCH_SERVER_BITMAP),
          anyLong());
      verify(_minionMetrics, never()).addMeteredTableValue(anyString(), eq(MinionMeter.VALID_DOC_IDS_UNAVAILABLE),
          anyLong());
    }
  }

  @Test
  public void testFetchValidDocIdsRetryExhaustedMetersCrcMismatch() {
    TestableExecutor executor = newExecutor();
    executor._validDocIdsFetchMaxAttempts = 2;
    executor._validDocIdsFetchRetryDelayMs = 0L;

    try (MockedStatic<MinionTaskUtils> mocked = Mockito.mockStatic(MinionTaskUtils.class, Mockito.CALLS_REAL_METHODS)) {
      mocked.when(
              () -> MinionTaskUtils.getValidDocIdFromServerMatchingCrc(anyString(), anyString(), anyString(), any(),
                  anyString(), anyString(), anyString()))
          .thenThrow(new IllegalStateException("CRC mismatch for segment: " + SEGMENT_NAME + ", expected: 1000"));

      try {
        executor.fetchValidDocIdsWithRetry(createTaskConfig(false), REALTIME_TABLE_NAME, SEGMENT_NAME, "SNAPSHOT",
            EXPECTED_CRC, DATA_CRC, "EQUAL");
        Assert.fail("expected IllegalStateException");
      } catch (IllegalStateException e) {
        Assert.assertTrue(e.getMessage().contains("CRC mismatch"));
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      verify(_minionMetrics).addMeteredTableValue(REALTIME_TABLE_NAME, MinionMeter.CRC_MISMATCH_SERVER_BITMAP, 1L);
    }
  }

  @Test
  public void testFetchValidDocIdsNullAfterRetriesMetersUnavailable() {
    TestableExecutor executor = newExecutor();
    executor._validDocIdsFetchMaxAttempts = 2;
    executor._validDocIdsFetchRetryDelayMs = 0L;

    try (MockedStatic<MinionTaskUtils> mocked = Mockito.mockStatic(MinionTaskUtils.class, Mockito.CALLS_REAL_METHODS)) {
      mocked.when(
              () -> MinionTaskUtils.getValidDocIdFromServerMatchingCrc(anyString(), anyString(), anyString(), any(),
                  anyString(), anyString(), anyString()))
          .thenReturn(null);

      try {
        executor.fetchValidDocIdsWithRetry(createTaskConfig(false), REALTIME_TABLE_NAME, SEGMENT_NAME, "SNAPSHOT",
            EXPECTED_CRC, DATA_CRC, "UNSAFE");
        Assert.fail("expected IllegalStateException");
      } catch (IllegalStateException e) {
        Assert.assertTrue(e.getMessage().contains("No validDocIds"));
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      verify(_minionMetrics).addMeteredTableValue(REALTIME_TABLE_NAME, MinionMeter.VALID_DOC_IDS_UNAVAILABLE, 1L);
    }
  }

  @Test
  public void testClassifyValidDocIdsFailureMeter() {
    Assert.assertEquals(UpsertCompactionTaskExecutor.classifyValidDocIdsFailureMeter("CRC mismatch for segment: x"),
        MinionMeter.CRC_MISMATCH_SERVER_BITMAP);
    Assert.assertEquals(UpsertCompactionTaskExecutor.classifyValidDocIdsFailureMeter("No validDocIds found"),
        MinionMeter.VALID_DOC_IDS_UNAVAILABLE);
    Assert.assertEquals(UpsertCompactionTaskExecutor.classifyValidDocIdsFailureMeter(null),
        MinionMeter.VALID_DOC_IDS_UNAVAILABLE);
  }

  @Test
  public void testConvertEmptyValidDocIdsSkipsWithMeter()
      throws Exception {
    File indexDir = new File(_tempDir, "indexDir");
    File workingDir = new File(_tempDir, "workingDir");
    Assert.assertTrue(indexDir.mkdirs());
    Assert.assertTrue(workingDir.mkdirs());

    TestableExecutor executor = newExecutor();
    executor._validDocIdsFetchMaxAttempts = 1;
    executor._validDocIdsFetchRetryDelayMs = 0L;

    try (MockedConstruction<SegmentMetadataImpl> ignored = Mockito.mockConstruction(SegmentMetadataImpl.class,
        (mock, context) -> {
          when(mock.getCrc()).thenReturn(EXPECTED_CRC);
          when(mock.getDataCrc()).thenReturn(DATA_CRC);
          when(mock.getTotalDocs()).thenReturn(10);
        });
        MockedStatic<MinionTaskUtils> mocked = Mockito.mockStatic(MinionTaskUtils.class, Mockito.CALLS_REAL_METHODS)) {
      mocked.when(() -> MinionTaskUtils.getValidDocIdsType(any(), any(), anyString()))
          .thenReturn(org.apache.pinot.common.restlet.resources.ValidDocIdsType.SNAPSHOT);
      mocked.when(
              () -> MinionTaskUtils.getValidDocIdFromServerMatchingCrc(anyString(), anyString(), anyString(), any(),
                  anyString(), anyString(), anyString()))
          .thenReturn(new RoaringBitmap());

      SegmentConversionResult result = executor.convert(createTaskConfig(false), indexDir, workingDir);
      Assert.assertNull(result.getFile());
      Assert.assertEquals(result.getSegmentName(), SEGMENT_NAME);
      verify(_minionMetrics).addMeteredTableValue(REALTIME_TABLE_NAME, MinionMeter.COMPACTION_SKIP_EMPTY_VALID_DOCS,
          1L);
    }
  }

  @Test
  public void testConvertDeepStoreCrcMismatchThrows()
      throws Exception {
    File indexDir = new File(_tempDir, "indexDir2");
    File workingDir = new File(_tempDir, "workingDir2");
    Assert.assertTrue(indexDir.mkdirs());
    Assert.assertTrue(workingDir.mkdirs());

    TestableExecutor executor = newExecutor();
    executor._zkDataCrc = -1;

    try (MockedConstruction<SegmentMetadataImpl> ignored = Mockito.mockConstruction(SegmentMetadataImpl.class,
        (mock, context) -> {
          when(mock.getCrc()).thenReturn("9999");
          when(mock.getDataCrc()).thenReturn(DATA_CRC);
        });
        MockedStatic<MinionTaskUtils> mocked = Mockito.mockStatic(MinionTaskUtils.class, Mockito.CALLS_REAL_METHODS)) {
      mocked.when(() -> MinionTaskUtils.getValidDocIdsType(any(), any(), anyString()))
          .thenReturn(org.apache.pinot.common.restlet.resources.ValidDocIdsType.SNAPSHOT);

      try {
        executor.convert(createTaskConfig(false), indexDir, workingDir);
        Assert.fail("expected IllegalStateException");
      } catch (IllegalStateException e) {
        Assert.assertTrue(e.getMessage().contains("Crc mismatched"));
      }
      verify(_minionMetrics).addMeteredTableValue(REALTIME_TABLE_NAME, MinionMeter.CRC_MISMATCH_DEEPSTORE, 1L);
      mocked.verify(
          () -> MinionTaskUtils.getValidDocIdFromServerMatchingCrc(anyString(), anyString(), anyString(), any(),
              anyString(), anyString(), anyString()), never());
    }
  }

  @Test
  public void testConvertIgnoreCrcMismatchProceedsToValidDocIdsFetch()
      throws Exception {
    File indexDir = new File(_tempDir, "indexDir3");
    File workingDir = new File(_tempDir, "workingDir3");
    Assert.assertTrue(indexDir.mkdirs());
    Assert.assertTrue(workingDir.mkdirs());

    TestableExecutor executor = newExecutor();
    executor._zkDataCrc = -1;
    executor._validDocIdsFetchMaxAttempts = 1;
    executor._validDocIdsFetchRetryDelayMs = 0L;

    try (MockedConstruction<SegmentMetadataImpl> ignored = Mockito.mockConstruction(SegmentMetadataImpl.class,
        (mock, context) -> {
          when(mock.getCrc()).thenReturn("9999");
          when(mock.getDataCrc()).thenReturn(DATA_CRC);
          when(mock.getTotalDocs()).thenReturn(10);
        });
        MockedStatic<MinionTaskUtils> mocked = Mockito.mockStatic(MinionTaskUtils.class, Mockito.CALLS_REAL_METHODS)) {
      mocked.when(() -> MinionTaskUtils.getValidDocIdsType(any(), any(), anyString()))
          .thenReturn(org.apache.pinot.common.restlet.resources.ValidDocIdsType.SNAPSHOT);
      mocked.when(
              () -> MinionTaskUtils.getValidDocIdFromServerMatchingCrc(anyString(), anyString(), anyString(), any(),
                  anyString(), anyString(), anyString()))
          .thenReturn(new RoaringBitmap());

      SegmentConversionResult result = executor.convert(createTaskConfig(true), indexDir, workingDir);
      Assert.assertNull(result.getFile());
      verify(_minionMetrics, never()).addMeteredTableValue(anyString(), eq(MinionMeter.CRC_MISMATCH_DEEPSTORE),
          anyLong());
      verify(_minionMetrics).addMeteredTableValue(REALTIME_TABLE_NAME, MinionMeter.COMPACTION_SKIP_EMPTY_VALID_DOCS,
          1L);
    }
  }

  private TestableExecutor newExecutor() {
    TestableExecutor executor = new TestableExecutor();
    executor.setMinionEventObserver(_eventObserver);
    return executor;
  }

  private PinotTaskConfig createTaskConfig(boolean ignoreCrcMismatch) {
    Map<String, String> configs = new HashMap<>();
    configs.put(MinionConstants.TABLE_NAME_KEY, REALTIME_TABLE_NAME);
    configs.put(MinionConstants.SEGMENT_NAME_KEY, SEGMENT_NAME);
    configs.put(MinionConstants.ORIGINAL_SEGMENT_CRC_KEY, EXPECTED_CRC);
    configs.put(UpsertCompactionTask.IGNORE_CRC_MISMATCH_KEY, String.valueOf(ignoreCrcMismatch));
    configs.put(UpsertCompactionTask.VALID_DOC_IDS_TYPE, "SNAPSHOT");
    return new PinotTaskConfig(TASK_TYPE, configs);
  }

  private static TableConfig createUpsertTableConfig() {
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setSnapshot(Enablement.ENABLE);
    Map<String, Map<String, String>> taskTypeConfigs = new HashMap<>();
    taskTypeConfigs.put(TASK_TYPE, Map.of(UpsertCompactionTask.VALID_DOC_IDS_CONSENSUS_MODE_KEY, "EQUAL"));
    return new TableConfigBuilder(TableType.REALTIME).setTableName("testTable").setUpsertConfig(upsertConfig)
        .setTaskConfig(new TableTaskConfig(taskTypeConfigs)).build();
  }

  /// Test double that stubs ZK / table-config lookups so convert() can run without Helix property store.
  private static class TestableExecutor extends UpsertCompactionTaskExecutor {
    long _zkDataCrc = -1;

    @Override
    protected TableConfig getTableConfig(String tableNameWithType) {
      return createUpsertTableConfig();
    }

    @Override
    long getZkDataCrc(String tableNameWithType, String segmentName) {
      return _zkDataCrc;
    }
  }
}
