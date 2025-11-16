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
package org.apache.pinot.core.data.manager;

import java.io.File;
import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.core.data.manager.offline.OfflineTableDataManager;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.utils.SegmentLocks;
import org.apache.pinot.segment.local.utils.SegmentReloadSemaphore;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * Tests for async segment refresh enabled
 */
public class BaseTableDataManagerEnqueueSegmentToReplaceTest {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), "BaseTableDataManagerEnqueueSegmentToReplaceTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME).build();

  private OfflineTableDataManager _tableDataManager;
  private ExecutorService _segmentReloadRefreshExecutor;
  private InstanceDataManagerConfig _instanceDataManagerConfig;
  private HelixManager _helixManager;
  private ServerMetrics _serverMetrics;

  @BeforeClass
  public void setUp() {
    ServerMetrics.register(mock(ServerMetrics.class));
  }

  @BeforeMethod
  public void setUpMethod() throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(TEMP_DIR);

    // Create a real ExecutorService for testing
    _segmentReloadRefreshExecutor = Executors.newSingleThreadExecutor();

    // Mock dependencies
    _instanceDataManagerConfig = mock(InstanceDataManagerConfig.class);
    when(_instanceDataManagerConfig.getInstanceDataDir()).thenReturn(TEMP_DIR.getAbsolutePath());
    when(_instanceDataManagerConfig.getInstanceId()).thenReturn("testInstance");

    _helixManager = mock(HelixManager.class);

    @SuppressWarnings("unchecked")
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(_helixManager.getHelixPropertyStore()).thenReturn(propertyStore);

    // Mock ServerMetrics for metric validation
    _serverMetrics = mock(ServerMetrics.class);

    // Create table data manager with segment refresh executor
    _tableDataManager = spy(new OfflineTableDataManager());
    _tableDataManager.init(_instanceDataManagerConfig, _helixManager, new SegmentLocks(), TABLE_CONFIG,
        SCHEMA, new SegmentReloadSemaphore(1), _segmentReloadRefreshExecutor, null, null, null, true);

    // Inject the mocked ServerMetrics into the table data manager using reflection
    try {
      Field serverMetricsField = BaseTableDataManager.class.getDeclaredField("_serverMetrics");
      serverMetricsField.setAccessible(true);
      serverMetricsField.set(_tableDataManager, _serverMetrics);
    } catch (Exception e) {
      throw new RuntimeException("Failed to inject mocked ServerMetrics", e);
    }
  }

  @AfterMethod
  public void tearDownMethod() throws Exception {
    if (_segmentReloadRefreshExecutor != null && !_segmentReloadRefreshExecutor.isShutdown()) {
      _segmentReloadRefreshExecutor.shutdownNow();
      _segmentReloadRefreshExecutor.awaitTermination(5, TimeUnit.SECONDS);
    }
    if (_tableDataManager != null) {
      _tableDataManager.shutDown();
    }
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  /**
   * Test successful enqueueing and execution of segment replacement
   */
  @Test
  public void testEnqueueSegmentToReplaceSuccess() throws Exception {
    // Setup mocks for successful segment replacement
    SegmentDataManager mockSegmentDataManager = mock(ImmutableSegmentDataManager.class);
    when(mockSegmentDataManager.getSegmentName()).thenReturn(SEGMENT_NAME);

    ImmutableSegment mockSegment = mock(ImmutableSegment.class);
    when(mockSegmentDataManager.getSegment()).thenReturn(mockSegment);

    SegmentMetadata mockMetadata = mock(SegmentMetadata.class);
    when(mockSegment.getSegmentMetadata()).thenReturn(mockMetadata);
    when(mockMetadata.getCrc()).thenReturn("12345");

    SegmentZKMetadata mockZkMetadata = mock(SegmentZKMetadata.class);
    when(mockZkMetadata.getSegmentName()).thenReturn(SEGMENT_NAME);
    when(mockZkMetadata.getCrc()).thenReturn(12345L);

    // Add the segment to the manager's internal map
    _tableDataManager._segmentDataManagerMap.put(SEGMENT_NAME, mockSegmentDataManager);

    // Mock the methods that will be called during segment replacement
    doAnswer(invocation -> mockZkMetadata).when(_tableDataManager).fetchZKMetadata(SEGMENT_NAME);
    doAnswer(invocation -> new IndexLoadingConfig()).when(_tableDataManager).fetchIndexLoadingConfig();

    // Use CountDownLatch to wait for async execution
    CountDownLatch latch = new CountDownLatch(1);
    AtomicBoolean replaceSegmentIfCrcMismatchCalled = new AtomicBoolean(false);

    doAnswer(invocation -> {
      replaceSegmentIfCrcMismatchCalled.set(true);
      latch.countDown();
      return null;
    }).when(_tableDataManager).replaceSegmentIfCrcMismatch(any(), any(), any());

    // Test the method
    _tableDataManager.replaceSegment(SEGMENT_NAME);

    // Wait for async execution to complete
    assertTrue(latch.await(10, TimeUnit.SECONDS), "Segment replacement with CRC mismatch should be executed");
    assertTrue(replaceSegmentIfCrcMismatchCalled.get(), "replaceSegmentIfCrcMismatch should have been called");

    // Verify that the segment replacement was attempted
    verify(_tableDataManager, times(1)).enqueueSegmentToReplace(SEGMENT_NAME);
    verify(_tableDataManager, times(1)).fetchZKMetadata(SEGMENT_NAME);
    verify(_tableDataManager, times(1)).fetchIndexLoadingConfig();
  }

  /**
   * Test behavior when table manager is shut down
   */
  @Test
  public void testEnqueueSegmentToReplaceWhenShutDown() throws Exception {
    // Shutdown the table data manager
    _tableDataManager.shutDown();

    // Mock replaceSegment to track if it's called
    doAnswer(invocation -> {
      throw new AssertionError("replaceSegment should not be called when shut down");
    }).when(_tableDataManager).replaceSegmentInternal(anyString());

    // Test the method - it should return early without enqueueing
    _tableDataManager.replaceSegment(SEGMENT_NAME);

    // Give some time for any potential async execution
    Thread.sleep(1000);

    // Verify that replaceSegment was never called
    verify(_tableDataManager, never()).replaceSegmentInternal(SEGMENT_NAME);
    verify(_tableDataManager, never()).replaceSegmentIfCrcMismatch(any(), any(), any());
  }

  /**
   * Test exception handling during segment replacement and metric increment
   */
  @Test
  public void testEnqueueSegmentToReplaceWithException() throws Exception {
    // Setup mocks to throw exception during replacement
    RuntimeException testException = new RuntimeException("Test exception during replacement");
    doThrow(testException).when(_tableDataManager).replaceSegmentInternal(SEGMENT_NAME);

    // Use CountDownLatch to wait for async execution
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Exception> caughtException = new AtomicReference<>();

    // Override the executor to capture exceptions
    ExecutorService spyExecutor = spy(_segmentReloadRefreshExecutor);
    doAnswer(invocation -> {
      Runnable task = invocation.getArgument(0);
      try {
        task.run();
      } catch (Exception e) {
        caughtException.set(e);
      } finally {
        latch.countDown();
      }
      return null;
    }).when(spyExecutor).submit(any(Runnable.class));

    // Replace the executor in the table data manager
    _tableDataManager._segmentReloadRefreshExecutor = spyExecutor;

    // Test the method
    _tableDataManager.replaceSegment(SEGMENT_NAME);

    // Wait for async execution to complete
    assertTrue(latch.await(10, TimeUnit.SECONDS), "Task should complete even with exception");

    // Verify that replaceSegment was called and exception was handled
    verify(_tableDataManager, times(1)).enqueueSegmentToReplace(SEGMENT_NAME);
    verify(_tableDataManager, times(1)).replaceSegmentInternal(SEGMENT_NAME);

    // Verify that the REFRESH_FAILURES metric was incremented
    verify(_serverMetrics, times(1)).addMeteredTableValue(
        eq(RAW_TABLE_NAME + "_OFFLINE"), eq(ServerMeter.REFRESH_FAILURES), eq(1L));

    // The exception should be caught and handled by the implementation, not propagated
    assertNull(caughtException.get());
  }

  /**
   * Test that executor is properly used for async execution
   */
  @Test
  public void testExecutorUsage() throws Exception {
    ExecutorService mockExecutor = mock(ExecutorService.class);
    _tableDataManager._segmentReloadRefreshExecutor = mockExecutor;

    // Test the method
    _tableDataManager.replaceSegment(SEGMENT_NAME);

    // Verify that the executor's submit method was called
    verify(mockExecutor, times(1)).submit(any(Runnable.class));
  }

  /**
   * Test with null segment refresh executor should trigger assertion error
   */
  @Test(expectedExceptions = AssertionError.class)
  public void testEnqueueSegmentToReplaceWithAsyncSegmentRefreshDisabled() {
    // Create a table data manager without segment refresh executor
    OfflineTableDataManager tableDataManagerWithoutExecutor = new OfflineTableDataManager();
    tableDataManagerWithoutExecutor.init(_instanceDataManagerConfig, _helixManager, new SegmentLocks(),
        TABLE_CONFIG, SCHEMA, new SegmentReloadSemaphore(1), Executors.newSingleThreadExecutor(),
        null, null, null, false);

    // This should trigger the assertion error
    tableDataManagerWithoutExecutor.enqueueSegmentToReplace(SEGMENT_NAME);
  }

  /**
   * Test multiple concurrent enqueue operations
   */
  @Test
  public void testConcurrentEnqueueOperations() throws Exception {
    int numConcurrentOperations = 5;
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch completionLatch = new CountDownLatch(numConcurrentOperations);

    // Mock replaceSegment to simulate some work and track calls
    doAnswer(invocation -> {
      Thread.sleep(50); // Simulate some work
      completionLatch.countDown();
      return null;
    }).when(_tableDataManager).replaceSegmentInternal(anyString());

    // Start multiple threads to enqueue operations
    for (int i = 0; i < numConcurrentOperations; i++) {
      final String segmentName = SEGMENT_NAME + "_" + i;
      new Thread(() -> {
        try {
          startLatch.await();
          _tableDataManager.replaceSegment(segmentName);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }).start();
    }

    // Start all operations
    startLatch.countDown();

    // Wait for all operations to complete
    assertTrue(completionLatch.await(10, TimeUnit.SECONDS), "All concurrent operations should complete");

    // Verify that replaceSegment was called for each segment
    for (int i = 0; i < numConcurrentOperations; i++) {
      verify(_tableDataManager, times(1)).replaceSegment(SEGMENT_NAME + "_" + i);
    }
  }
}
