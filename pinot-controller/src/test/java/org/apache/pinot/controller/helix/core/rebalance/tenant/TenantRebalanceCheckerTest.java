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
package org.apache.pinot.controller.helix.core.rebalance.tenant;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.controllerjob.ControllerJobTypes;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceConfig;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceJobConstants;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceResult;
import org.apache.pinot.controller.helix.core.rebalance.TableRebalanceContext;
import org.apache.pinot.controller.helix.core.rebalance.TableRebalanceProgressStats;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;


public class TenantRebalanceCheckerTest extends ControllerTest {
  private static final String TENANT_NAME = "TestTenant";
  private static final String JOB_ID = "test-tenant-rebalance-job-123";
  private static final String JOB_ID_2 = "test-tenant-rebalance-job-456";
  private static final String ORIGINAL_JOB_ID = "original-tenant-rebalance-job-123";
  private static final String TABLE_NAME_1 = "testTable1_OFFLINE";
  private static final String TABLE_NAME_2 = "testTable2_OFFLINE";
  private static final String NON_STUCK_TABLE_JOB_ID = "non-stuck-table-job-456";
  private static final String STUCK_TABLE_JOB_ID = "stuck-table-job-456";
  private static final String STUCK_TABLE_JOB_ID_2 = "stuck-table-job-789";

  @Mock
  private PinotHelixResourceManager _mockPinotHelixResourceManager;
  @Mock
  private TenantRebalancer _mockTenantRebalancer;
  @Mock
  private ControllerConf _mockControllerConf;

  private TenantRebalanceChecker _tenantRebalanceChecker;
  private ExecutorService _executorService;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    _executorService = Executors.newFixedThreadPool(2);

    // Setup default mock behaviors
    when(_mockControllerConf.getTenantRebalanceCheckerFrequencyInSeconds()).thenReturn(300);
    when(_mockControllerConf.getTenantRebalanceCheckerInitialDelayInSeconds()).thenReturn(300L);
    // mock ZK update success
    doReturn(true).when(_mockPinotHelixResourceManager)
        .addControllerJobToZK(anyString(), anyMap(), eq(ControllerJobTypes.TENANT_REBALANCE));
    doReturn(true).when(_mockPinotHelixResourceManager)
        .addControllerJobToZK(anyString(), anyMap(), eq(ControllerJobTypes.TENANT_REBALANCE), any());

    _tenantRebalanceChecker = new TenantRebalanceChecker(
        _mockControllerConf,
        _mockPinotHelixResourceManager,
        _mockTenantRebalancer
    );
  }

  @AfterMethod
  public void tearDown() {
    if (_executorService != null) {
      _executorService.shutdown();
    }
  }

  @Test
  public void testResumeStuckTenantRebalanceJob()
      throws Exception {
    // Create a stuck tenant rebalance context
    TenantRebalanceContext stuckContext = createStuckTenantRebalanceContext();
    TenantRebalanceProgressStats progressStats = createProgressStats();

    // Mock ZK metadata for the stuck job
    Map<String, String> jobZKMetadata = createTenantJobZKMetadata(stuckContext, progressStats);
    Map<String, Map<String, String>> allJobMetadata = new HashMap<>();
    allJobMetadata.put(JOB_ID, jobZKMetadata);

    // Mock stuck table rebalance job metadata
    Map<String, String> stuckTableJobMetadata = createStuckTableJobMetadata();

    // Setup mocks
    doReturn(allJobMetadata).when(_mockPinotHelixResourceManager)
        .getAllJobs(eq(Set.of(ControllerJobTypes.TENANT_REBALANCE)), any());
    doReturn(stuckTableJobMetadata).when(_mockPinotHelixResourceManager)
        .getControllerJobZKMetadata(eq(STUCK_TABLE_JOB_ID), eq(ControllerJobTypes.TABLE_REBALANCE));

    // Mock the tenant rebalancer to capture the resumed context
    ArgumentCaptor<TenantRebalanceContext> contextCaptor =
        ArgumentCaptor.forClass(TenantRebalanceContext.class);
    ArgumentCaptor<ZkBasedTenantRebalanceObserver> observerCaptor =
        ArgumentCaptor.forClass(ZkBasedTenantRebalanceObserver.class);

    // Execute the checker
    _tenantRebalanceChecker.runTask(new Properties());

    // Verify that the tenant rebalancer was called to resume the job
    verify(_mockTenantRebalancer, times(1)).rebalanceWithObserver(
        observerCaptor.capture());

    // Verify the resumed context
    TenantRebalanceContext resumedContext = observerCaptor.getValue().getTenantRebalanceContext();
    assertNotNull(resumedContext);
    assertEquals(resumedContext.getAttemptId(), TenantRebalanceContext.INITIAL_ATTEMPT_ID + 1);
    assertEquals(resumedContext.getOriginalJobId(), ORIGINAL_JOB_ID);
    assertEquals(resumedContext.getAttemptId(), 2); // Should be incremented from 1
    assertEquals(resumedContext.getConfig().getTenantName(), TENANT_NAME);

    // Verify that the stuck table job context was moved back to parallel queue
    TenantRebalancer.TenantTableRebalanceJobContext firstJobContextInParallelQueue =
        resumedContext.getParallelQueue().poll();
    assertNotNull(firstJobContextInParallelQueue);
    // because the stuck job is aborted, a new job ID is generated
    assertNotEquals(firstJobContextInParallelQueue.getJobId(), STUCK_TABLE_JOB_ID);
    assertEquals(firstJobContextInParallelQueue.getTableName(), TABLE_NAME_1);
    assertFalse(firstJobContextInParallelQueue.shouldRebalanceWithDowntime());
    assertTrue(resumedContext.getOngoingJobsQueue().isEmpty());

    // Verify the observer was created with correct parameters
    ZkBasedTenantRebalanceObserver observer = observerCaptor.getValue();
    assertNotNull(observer);
  }

  @Test
  public void testResumeStuckTenantRebalanceJobWithMultipleStuckTables()
      throws Exception {
    // Create a context with multiple stuck table jobs
    TenantRebalanceContext stuckContext = createStuckTenantRebalanceContextWithMultipleTables();
    TenantRebalanceProgressStats progressStats = createProgressStatsWithMultipleTables();

    // Mock ZK metadata
    Map<String, String> jobZKMetadata = createTenantJobZKMetadata(stuckContext, progressStats);
    Map<String, Map<String, String>> allJobMetadata = new HashMap<>();
    allJobMetadata.put(JOB_ID, jobZKMetadata);

    // Mock stuck table job metadata for both tables
    Map<String, String> stuckTableJobMetadata1 = createStuckTableJobMetadata();
    Map<String, String> stuckTableJobMetadata2 = createStuckTableJobMetadata();

    // Setup mocks
    doReturn(allJobMetadata).when(_mockPinotHelixResourceManager)
        .getAllJobs(eq(Set.of(ControllerJobTypes.TENANT_REBALANCE)), any());
    doReturn(stuckTableJobMetadata1).when(_mockPinotHelixResourceManager)
        .getControllerJobZKMetadata(eq(STUCK_TABLE_JOB_ID), eq(ControllerJobTypes.TABLE_REBALANCE));
    doReturn(stuckTableJobMetadata2).when(_mockPinotHelixResourceManager)
        .getControllerJobZKMetadata(eq(STUCK_TABLE_JOB_ID_2), eq(ControllerJobTypes.TABLE_REBALANCE));

    // Execute the checker
    _tenantRebalanceChecker.runTask(new Properties());

    // Verify that the tenant rebalancer was called
    ArgumentCaptor<TenantRebalanceContext> contextCaptor =
        ArgumentCaptor.forClass(TenantRebalanceContext.class);
    ArgumentCaptor<ZkBasedTenantRebalanceObserver> observerCaptor =
        ArgumentCaptor.forClass(ZkBasedTenantRebalanceObserver.class);
    verify(_mockTenantRebalancer, times(1)).rebalanceWithObserver(
        observerCaptor.capture());

    // Verify that both stuck table jobs were moved back to parallel queue
    TenantRebalanceContext resumedContext = observerCaptor.getValue().getTenantRebalanceContext();
    assertEquals(resumedContext.getAttemptId(), TenantRebalanceContext.INITIAL_ATTEMPT_ID + 1);
    assertEquals(resumedContext.getParallelQueue().size(), 2);
    assertTrue(resumedContext.getOngoingJobsQueue().isEmpty());
  }

  @Test
  public void testDoNotResumeNonStuckTenantRebalanceJob()
      throws Exception {
    // Create a non-stuck tenant rebalance context (no ongoing jobs)
    TenantRebalanceContext nonStuckContextWithoutOngoing = createNonStuckTenantRebalanceContextWithoutOngoing();
    TenantRebalanceProgressStats progressStats = createProgressStats();

    // Mock ZK metadata
    Map<String, String> tenantJobZKMetadataWithoutOngoing =
        createTenantJobZKMetadataWithRecentTimestamp(nonStuckContextWithoutOngoing, progressStats);
    Map<String, Map<String, String>> allJobMetadata = new HashMap<>();
    allJobMetadata.put(JOB_ID, tenantJobZKMetadataWithoutOngoing);

    // Setup mocks
    doReturn(allJobMetadata).when(_mockPinotHelixResourceManager)
        .getAllJobs(eq(Set.of(ControllerJobTypes.TENANT_REBALANCE)), any());

    // Execute the checker
    _tenantRebalanceChecker.runTask(new Properties());

    // Verify that the tenant rebalancer was NOT called
    verify(_mockTenantRebalancer, never()).rebalanceWithObserver(any());
  }

  @Test
  public void testDoNotResumeNonStuckTenantRebalanceJobWithOngoing()
      throws Exception {
    // Create a non-stuck tenant rebalance context (no ongoing jobs)
    TenantRebalanceContext nonStuckContextWithOngoing = createNonStuckTenantRebalanceContextWithOngoing();
    TenantRebalanceProgressStats progressStats = createProgressStats();

    // Mock ZK metadata
    Map<String, String> tenantJobZKMetadataWithOngoing =
        createTenantJobZKMetadataWithRecentTimestamp(nonStuckContextWithOngoing, progressStats);
    Map<String, Map<String, String>> allJobMetadata = new HashMap<>();
    allJobMetadata.put(JOB_ID, tenantJobZKMetadataWithOngoing);

    // Mock non-stuck table rebalance job metadata
    Map<String, String> nonStuckTableJobMetadata = createNonStuckTableJobMetadata();

    // Setup mocks
    doReturn(allJobMetadata).when(_mockPinotHelixResourceManager)
        .getAllJobs(eq(Set.of(ControllerJobTypes.TENANT_REBALANCE)), any());
    doReturn(nonStuckTableJobMetadata).when(_mockPinotHelixResourceManager)
        .getControllerJobZKMetadata(eq(NON_STUCK_TABLE_JOB_ID), eq(ControllerJobTypes.TABLE_REBALANCE));

    // Setup mocks
    doReturn(allJobMetadata).when(_mockPinotHelixResourceManager)
        .getAllJobs(eq(Set.of(ControllerJobTypes.TENANT_REBALANCE)), any());

    // Execute the checker
    _tenantRebalanceChecker.runTask(new Properties());

    // Verify that the tenant rebalancer was NOT called
    verify(_mockTenantRebalancer, never()).rebalanceWithObserver(any());
  }

  @Test
  public void testDoNotResumeTenantRebalanceJobWhileZKUpdateFailed()
      throws Exception {

    // Create a stuck tenant rebalance context
    TenantRebalanceContext stuckContext = createStuckTenantRebalanceContext();
    TenantRebalanceProgressStats progressStats = createProgressStats();

    // Mock ZK metadata for the stuck job
    Map<String, String> jobZKMetadata = createTenantJobZKMetadata(stuckContext, progressStats);
    Map<String, Map<String, String>> allJobMetadata = new HashMap<>();
    allJobMetadata.put(JOB_ID, jobZKMetadata);

    // Mock stuck table rebalance job metadata
    Map<String, String> stuckTableJobMetadata = createStuckTableJobMetadata();

    // Setup mocks
    doReturn(allJobMetadata).when(_mockPinotHelixResourceManager)
        .getAllJobs(eq(Set.of(ControllerJobTypes.TENANT_REBALANCE)), any());
    doReturn(stuckTableJobMetadata).when(_mockPinotHelixResourceManager)
        .getControllerJobZKMetadata(eq(STUCK_TABLE_JOB_ID), eq(ControllerJobTypes.TABLE_REBALANCE));
    doReturn(false).when(_mockPinotHelixResourceManager)
        .addControllerJobToZK(anyString(), anyMap(), eq(ControllerJobTypes.TENANT_REBALANCE), any());

    // Mock the tenant rebalancer to capture the resumed context
    ArgumentCaptor<TenantRebalanceContext> contextCaptor =
        ArgumentCaptor.forClass(TenantRebalanceContext.class);
    ArgumentCaptor<ZkBasedTenantRebalanceObserver> observerCaptor =
        ArgumentCaptor.forClass(ZkBasedTenantRebalanceObserver.class);

    // Execute the checker
    _tenantRebalanceChecker.runTask(new Properties());

    // Verify that the tenant rebalancer was NOT called because ZK update failed
    verify(_mockTenantRebalancer, never()).rebalanceWithObserver(any());
  }

  @Test
  public void testHandleJsonProcessingException()
      throws Exception {
    // Create ZK metadata with invalid JSON
    Map<String, String> invalidJsonJobZKMetadata = new HashMap<>();
    invalidJsonJobZKMetadata.put(CommonConstants.ControllerJob.JOB_ID, JOB_ID);
    invalidJsonJobZKMetadata.put(CommonConstants.ControllerJob.TENANT_NAME, TENANT_NAME);
    invalidJsonJobZKMetadata.put(CommonConstants.ControllerJob.SUBMISSION_TIME_MS,
        String.valueOf(System.currentTimeMillis()));
    invalidJsonJobZKMetadata.put(CommonConstants.ControllerJob.JOB_TYPE, ControllerJobTypes.TENANT_REBALANCE.name());
    invalidJsonJobZKMetadata.put(RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_CONTEXT, "invalid json");
    invalidJsonJobZKMetadata.put(RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_PROGRESS_STATS, "invalid json");

    Map<String, Map<String, String>> allJobMetadata = new HashMap<>();
    allJobMetadata.put(JOB_ID, invalidJsonJobZKMetadata);

    // Setup mocks
    doReturn(allJobMetadata).when(_mockPinotHelixResourceManager)
        .getAllJobs(eq(Set.of(ControllerJobTypes.TENANT_REBALANCE)), any());

    // Execute the checker - should not throw exception
    _tenantRebalanceChecker.runTask(new Properties());

    // Verify that the tenant rebalancer was NOT called
    verify(_mockTenantRebalancer, never()).rebalanceWithObserver(any());
  }

  @Test
  public void testDoNotRunMultipleTenantRebalanceRetry()
      throws Exception {

    // Create a stuck tenant rebalance context
    TenantRebalanceContext stuckContext = createStuckTenantRebalanceContext();
    TenantRebalanceProgressStats progressStats = createProgressStats();

    // Mock ZK metadata for the stuck job
    Map<String, Map<String, String>> allJobMetadata = new HashMap<>();
    allJobMetadata.put(JOB_ID, createTenantJobZKMetadata(stuckContext, progressStats, JOB_ID));
    allJobMetadata.put(JOB_ID_2, createTenantJobZKMetadata(stuckContext, progressStats, JOB_ID_2));

    // Mock stuck table rebalance job metadata
    Map<String, String> stuckTableJobMetadata = createStuckTableJobMetadata();

    // Setup mocks
    doReturn(allJobMetadata).when(_mockPinotHelixResourceManager)
        .getAllJobs(eq(Set.of(ControllerJobTypes.TENANT_REBALANCE)), any());
    doReturn(stuckTableJobMetadata).when(_mockPinotHelixResourceManager)
        .getControllerJobZKMetadata(eq(STUCK_TABLE_JOB_ID), eq(ControllerJobTypes.TABLE_REBALANCE));

    // Mock the tenant rebalancer to capture the resumed context
    ArgumentCaptor<TenantRebalanceContext> contextCaptor =
        ArgumentCaptor.forClass(TenantRebalanceContext.class);
    ArgumentCaptor<ZkBasedTenantRebalanceObserver> observerCaptor =
        ArgumentCaptor.forClass(ZkBasedTenantRebalanceObserver.class);

    // Execute the checker
    _tenantRebalanceChecker.runTask(new Properties());

    // Verify that the tenant rebalancer was called to resume the job
    verify(_mockTenantRebalancer, times(1)).rebalanceWithObserver(
        observerCaptor.capture());
    // The mockTenantRebalance never let the job done
    assertFalse(observerCaptor.getValue().isDone());

    _tenantRebalanceChecker.runTask(new Properties());
    // Since the previous job is not done, the rebalanceWithContext should not be called again as we have set the limit
    // to one tenant rebalance retry at a time
    verify(_mockTenantRebalancer, times(1)).rebalanceWithObserver(
        observerCaptor.capture());

    // Mark the job as done and run the checker again - should pick up another job now
    observerCaptor.getValue().setDone(true);
    _tenantRebalanceChecker.runTask(new Properties());

    verify(_mockTenantRebalancer, times(2)).rebalanceWithObserver(
        observerCaptor.capture());
  }

  // Helper methods to create test data

  private TenantRebalanceContext createStuckTenantRebalanceContext() {
    TenantRebalanceConfig config = new TenantRebalanceConfig();
    config.setTenantName(TENANT_NAME);
    config.setHeartbeatTimeoutInMs(300000L); // 5 minutes

    ConcurrentLinkedDeque<TenantRebalancer.TenantTableRebalanceJobContext> parallelQueue =
        new ConcurrentLinkedDeque<>();
    ConcurrentLinkedQueue<TenantRebalancer.TenantTableRebalanceJobContext> ongoingJobsQueue =
        new ConcurrentLinkedQueue<>();

    // Add a stuck table job to ongoing queue
    TenantRebalancer.TenantTableRebalanceJobContext stuckJobContext =
        new TenantRebalancer.TenantTableRebalanceJobContext(TABLE_NAME_1, STUCK_TABLE_JOB_ID, false);
    ongoingJobsQueue.add(stuckJobContext);

    return new TenantRebalanceContext(
        ORIGINAL_JOB_ID, config, 1, parallelQueue,
        new ConcurrentLinkedQueue<>(), ongoingJobsQueue);
  }

  private TenantRebalanceContext createStuckTenantRebalanceContextWithMultipleTables()
      throws JsonProcessingException {
    TenantRebalanceConfig config = new TenantRebalanceConfig();
    config.setTenantName(TENANT_NAME);
    config.setHeartbeatTimeoutInMs(300000L);

    ConcurrentLinkedDeque<TenantRebalancer.TenantTableRebalanceJobContext> parallelQueue =
        new ConcurrentLinkedDeque<>();
    ConcurrentLinkedQueue<TenantRebalancer.TenantTableRebalanceJobContext> ongoingJobsQueue =
        new ConcurrentLinkedQueue<>();

    // Add multiple stuck table jobs to ongoing queue
    ongoingJobsQueue.add(new TenantRebalancer.TenantTableRebalanceJobContext(TABLE_NAME_1, STUCK_TABLE_JOB_ID, false));
    ongoingJobsQueue.add(
        new TenantRebalancer.TenantTableRebalanceJobContext(TABLE_NAME_2, STUCK_TABLE_JOB_ID_2, false));

    return new TenantRebalanceContext(
        ORIGINAL_JOB_ID, config, 1, parallelQueue,
        new ConcurrentLinkedQueue<>(), ongoingJobsQueue);
  }

  private TenantRebalanceContext createNonStuckTenantRebalanceContextWithoutOngoing()
      throws JsonProcessingException {
    TenantRebalanceConfig config = new TenantRebalanceConfig();
    config.setTenantName(TENANT_NAME);
    config.setHeartbeatTimeoutInMs(300000L);

    ConcurrentLinkedDeque<TenantRebalancer.TenantTableRebalanceJobContext> parallelQueue =
        new ConcurrentLinkedDeque<>();
    // Add some jobs to parallel queue but none to ongoing queue
    parallelQueue.add(new TenantRebalancer.TenantTableRebalanceJobContext(TABLE_NAME_1, NON_STUCK_TABLE_JOB_ID, false));

    return new TenantRebalanceContext(
        ORIGINAL_JOB_ID, config, 1, parallelQueue,
        new ConcurrentLinkedQueue<>(), new ConcurrentLinkedQueue<>());
  }

  private TenantRebalanceContext createNonStuckTenantRebalanceContextWithOngoing()
      throws JsonProcessingException {
    TenantRebalanceConfig config = new TenantRebalanceConfig();
    config.setTenantName(TENANT_NAME);
    config.setHeartbeatTimeoutInMs(300000L);

    ConcurrentLinkedQueue<TenantRebalancer.TenantTableRebalanceJobContext> ongoing =
        new ConcurrentLinkedQueue<>();
    // Add some jobs to parallel queue but none to ongoing queue
    ongoing.add(new TenantRebalancer.TenantTableRebalanceJobContext(TABLE_NAME_1, NON_STUCK_TABLE_JOB_ID, false));

    return new TenantRebalanceContext(
        ORIGINAL_JOB_ID, config, 1, new ConcurrentLinkedDeque<>(),
        new ConcurrentLinkedQueue<>(), ongoing);
  }

  private TenantRebalanceContext createRecentTenantRebalanceContext()
      throws JsonProcessingException {
    TenantRebalanceConfig config = new TenantRebalanceConfig();
    config.setTenantName(TENANT_NAME);
    config.setHeartbeatTimeoutInMs(300000L);

    return new TenantRebalanceContext(
        ORIGINAL_JOB_ID, config, 1,
        new ConcurrentLinkedDeque<>(), new ConcurrentLinkedQueue<>(), new ConcurrentLinkedQueue<>());
  }

  private TenantRebalanceProgressStats createProgressStats() {
    Set<String> tables = new HashSet<>();
    tables.add(TABLE_NAME_1);
    tables.add(TABLE_NAME_2);

    TenantRebalanceProgressStats stats = new TenantRebalanceProgressStats(tables);
    stats.setStartTimeMs(System.currentTimeMillis() - 60000); // 1 minute ago
    stats.updateTableStatus(TABLE_NAME_1, TenantRebalanceProgressStats.TableStatus.PROCESSING.name());
    stats.updateTableStatus(TABLE_NAME_2, TenantRebalanceProgressStats.TableStatus.UNPROCESSED.name());

    return stats;
  }

  private TenantRebalanceProgressStats createProgressStatsWithMultipleTables() {
    Set<String> tables = new HashSet<>();
    tables.add(TABLE_NAME_1);
    tables.add(TABLE_NAME_2);

    TenantRebalanceProgressStats stats = new TenantRebalanceProgressStats(tables);
    stats.setStartTimeMs(System.currentTimeMillis() - 60000);
    stats.updateTableStatus(TABLE_NAME_1, TenantRebalanceProgressStats.TableStatus.PROCESSING.name());
    stats.updateTableStatus(TABLE_NAME_2, TenantRebalanceProgressStats.TableStatus.PROCESSING.name());

    return stats;
  }

  private Map<String, String> createTenantJobZKMetadata(TenantRebalanceContext context,
      TenantRebalanceProgressStats progressStats)
      throws JsonProcessingException {
    return createTenantJobZKMetadata(context, progressStats, JOB_ID);
  }

  private Map<String, String> createTenantJobZKMetadata(TenantRebalanceContext context,
      TenantRebalanceProgressStats progressStats, String jobId)
      throws JsonProcessingException {
    Map<String, String> metadata = new HashMap<>();
    metadata.put(CommonConstants.ControllerJob.JOB_ID, jobId);
    metadata.put(CommonConstants.ControllerJob.TENANT_NAME, TENANT_NAME);
    metadata.put(CommonConstants.ControllerJob.SUBMISSION_TIME_MS,
        String.valueOf(System.currentTimeMillis() - 400000)); // 6+ minutes ago (beyond heartbeat timeout)
    metadata.put(CommonConstants.ControllerJob.JOB_TYPE, ControllerJobTypes.TENANT_REBALANCE.name());
    metadata.put(RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_CONTEXT,
        JsonUtils.objectToString(context));
    metadata.put(RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_PROGRESS_STATS,
        JsonUtils.objectToString(progressStats));

    return metadata;
  }

  private Map<String, String> createTenantJobZKMetadataWithRecentTimestamp(TenantRebalanceContext context,
      TenantRebalanceProgressStats progressStats)
      throws JsonProcessingException {
    Map<String, String> metadata = createTenantJobZKMetadata(context, progressStats);
    metadata.put(CommonConstants.ControllerJob.SUBMISSION_TIME_MS,
        String.valueOf(System.currentTimeMillis() - 60000)); // 1 minute ago (within heartbeat timeout)
    return metadata;
  }

  private Map<String, String> createStuckTableJobMetadata()
      throws JsonProcessingException {
    Map<String, String> metadata = new HashMap<>();
    metadata.put(CommonConstants.ControllerJob.JOB_ID, STUCK_TABLE_JOB_ID);
    metadata.put(CommonConstants.ControllerJob.SUBMISSION_TIME_MS,
        String.valueOf(System.currentTimeMillis() - 400000)); // 6+ minutes ago

    // Create stuck table rebalance progress stats
    TableRebalanceProgressStats tableStats = new TableRebalanceProgressStats();
    tableStats.setStatus(RebalanceResult.Status.IN_PROGRESS);
    tableStats.setStartTimeMs(System.currentTimeMillis() - 400000);

    // Create table rebalance context
    RebalanceConfig rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setHeartbeatTimeoutInMs(300000L);
    TableRebalanceContext tableContext = TableRebalanceContext.forInitialAttempt(
        "original-table-job", rebalanceConfig, true);

    metadata.put(RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_PROGRESS_STATS,
        JsonUtils.objectToString(tableStats));
    metadata.put(RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_CONTEXT,
        JsonUtils.objectToString(tableContext));

    return metadata;
  }

  private Map<String, String> createNonStuckTableJobMetadata()
      throws JsonProcessingException {
    Map<String, String> metadata = createStuckTableJobMetadata();
    metadata.put(CommonConstants.ControllerJob.JOB_ID, NON_STUCK_TABLE_JOB_ID);
    metadata.put(CommonConstants.ControllerJob.SUBMISSION_TIME_MS,
        String.valueOf(System.currentTimeMillis() - 60000)); // 1 minutes ago

    return metadata;
  }
}
