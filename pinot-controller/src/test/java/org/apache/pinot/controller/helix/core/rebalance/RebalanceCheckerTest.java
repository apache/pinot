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
package org.apache.pinot.controller.helix.core.rebalance;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.controllerjob.ControllerJobType;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class RebalanceCheckerTest {

  @Test
  public void testGetRetryDelayInMs() {
    assertEquals(RebalanceChecker.getRetryDelayInMs(0, 0), 0);
    assertEquals(RebalanceChecker.getRetryDelayInMs(0, 1), 0);
    assertEquals(RebalanceChecker.getRetryDelayInMs(0, 2), 0);

    for (long initDelayMs : new long[]{1, 30000, 3600000}) {
      long delayMs = RebalanceChecker.getRetryDelayInMs(initDelayMs, 0);
      assertTrue(delayMs >= initDelayMs && delayMs < initDelayMs * 2);
      delayMs = RebalanceChecker.getRetryDelayInMs(initDelayMs, 1);
      assertTrue(delayMs >= initDelayMs * 2 && delayMs < initDelayMs * 4);
      delayMs = RebalanceChecker.getRetryDelayInMs(initDelayMs, 2);
      assertTrue(delayMs >= initDelayMs * 4 && delayMs < initDelayMs * 8);
    }
  }

  @Test
  public void testGetCandidateJobs()
      throws Exception {
    String tableName = "table01";
    Map<String, Map<String, String>> allJobMetadata = new HashMap<>();

    // Original job run as job1, and all its retry jobs failed too.
    RebalanceConfig jobCfg = new RebalanceConfig();
    jobCfg.setJobId("job1");
    jobCfg.setMaxRetry(3);
    TableRebalanceProgressStats stats = new TableRebalanceProgressStats();
    stats.setStatus("FAILED");
    stats.setStartTimeMs(1000);
    TableRebalanceRetryConfig retryCfg = TableRebalanceRetryConfig.forInitialRun("job1", jobCfg);
    Map<String, String> jobMetadata =
        ZkBasedTableRebalanceObserver.createJobMetadata(tableName, "job1", stats, retryCfg);
    allJobMetadata.put("job1", jobMetadata);
    // 3 failed retry runs for job1
    jobMetadata = createDummyRetryJob(tableName, "job1_1", "job1", 1, 1100, "FAILED");
    allJobMetadata.put("job1_1", jobMetadata);
    jobMetadata = createDummyRetryJob(tableName, "job1_2", "job1", 2, 1200, "FAILED");
    allJobMetadata.put("job1_2", jobMetadata);
    jobMetadata = createDummyRetryJob(tableName, "job1_3", "job1", 3, 1300, "FAILED");
    allJobMetadata.put("job1_3", jobMetadata);

    // Original job run as job2, and its retry job job2_1 completed.
    jobCfg = new RebalanceConfig();
    jobCfg.setJobId("job2");
    jobCfg.setMaxRetry(3);
    stats = new TableRebalanceProgressStats();
    stats.setStatus("FAILED");
    stats.setStartTimeMs(2000);
    retryCfg = TableRebalanceRetryConfig.forInitialRun("job2", jobCfg);
    jobMetadata = ZkBasedTableRebalanceObserver.createJobMetadata(tableName, "job2", stats, retryCfg);
    allJobMetadata.put("job2", jobMetadata);
    jobMetadata = createDummyRetryJob(tableName, "job2_1", "job2", 1, 2100, "DONE");
    allJobMetadata.put("job2_1", jobMetadata);

    // Original job run as job3, and failed to send out heartbeat in time.
    jobCfg = new RebalanceConfig();
    jobCfg.setJobId("job3");
    jobCfg.setMaxRetry(3);
    stats = new TableRebalanceProgressStats();
    stats.setStatus("IN_PROGRESS");
    stats.setStartTimeMs(3000);
    retryCfg = TableRebalanceRetryConfig.forInitialRun("job3", jobCfg);
    jobMetadata = ZkBasedTableRebalanceObserver.createJobMetadata(tableName, "job3", stats, retryCfg);
    jobMetadata.put(CommonConstants.ControllerJob.SUBMISSION_TIME_MS, "3000");
    allJobMetadata.put("job3", jobMetadata);

    // Original job run as job4, which didn't have retryJobCfg as from old version of the code.
    stats = new TableRebalanceProgressStats();
    stats.setStatus("FAILED");
    stats.setStartTimeMs(4000);
    jobMetadata = ZkBasedTableRebalanceObserver.createJobMetadata(tableName, "job4", stats, null);
    jobMetadata.remove(RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_RETRY_CONFIG);
    allJobMetadata.put("job4", jobMetadata);

    // Only need to retry job1 and job3, as job2 is completed and job4 is from old version of code.
    Map<String, Set<Pair<TableRebalanceRetryConfig, Long>>> jobs =
        RebalanceChecker.getCandidateJobs(tableName, allJobMetadata);
    assertEquals(jobs.size(), 2);
    assertTrue(jobs.containsKey("job1"));
    assertTrue(jobs.containsKey("job3"));
    assertEquals(jobs.get("job1").size(), 4); // four runs including job1,job1_1,job1_2,job1_3
    assertEquals(jobs.get("job3").size(), 1); // just a single run job3

    // Add latest job5 that's already done, thus no need to retry for table.
    jobCfg = new RebalanceConfig();
    jobCfg.setJobId("job5");
    jobCfg.setMaxRetry(3);
    stats = new TableRebalanceProgressStats();
    stats.setStatus("DONE");
    stats.setStartTimeMs(5000);
    retryCfg = TableRebalanceRetryConfig.forInitialRun("job5", jobCfg);
    jobMetadata = ZkBasedTableRebalanceObserver.createJobMetadata(tableName, "job5", stats, retryCfg);
    allJobMetadata.put("job5", jobMetadata);
    jobs = RebalanceChecker.getCandidateJobs(tableName, allJobMetadata);
    assertEquals(jobs.size(), 0);
  }

  @Test
  public void testGetLatestJob() {
    Map<String, Set<Pair<TableRebalanceRetryConfig, Long>>> jobs = new HashMap<>();
    // The most recent job run is job1_2, and within 3 maxRetry.
    jobs.put("job1", ImmutableSet.of(Pair.of(createDummyRetryJobCfg("job1", "job1", 0), 10L),
        Pair.of(createDummyRetryJobCfg("job1_1", "job1", 1), 20L),
        Pair.of(createDummyRetryJobCfg("job1_2", "job1", 2), 1020L)));
    jobs.put("job2", ImmutableSet.of(Pair.of(createDummyRetryJobCfg("job2", "job2", 0), 1000L)));
    Pair<TableRebalanceRetryConfig, Long> jobTime = RebalanceChecker.getLatestJob(jobs);
    assertNotNull(jobTime);
    assertEquals(jobTime.getLeft().getConfig().getJobId(), "job1_2");

    // The most recent job run is job1_3, but reached 3 maxRetry.
    jobs.put("job1", ImmutableSet.of(Pair.of(createDummyRetryJobCfg("job1", "job1", 0), 10L),
        Pair.of(createDummyRetryJobCfg("job1_1", "job1", 1), 20L),
        Pair.of(createDummyRetryJobCfg("job1_2", "job1", 2), 1020L),
        Pair.of(createDummyRetryJobCfg("job1_3", "job1", 3), 2020L)));
    jobTime = RebalanceChecker.getLatestJob(jobs);
    assertNotNull(jobTime);
    assertEquals(jobTime.getLeft().getConfig().getJobId(), "job2");

    // Add job3 that's started more recently.
    jobs.put("job3", ImmutableSet.of(Pair.of(createDummyRetryJobCfg("job3", "job3", 0), 3000L)));
    jobTime = RebalanceChecker.getLatestJob(jobs);
    assertNotNull(jobTime);
    assertEquals(jobTime.getLeft().getConfig().getJobId(), "job3");

    // Remove job2 and job3, and we'd have no job to retry then.
    jobs.remove("job2");
    jobs.remove("job3");
    jobTime = RebalanceChecker.getLatestJob(jobs);
    assertNull(jobTime);
  }

  @Test
  public void testRetryRebalance()
      throws Exception {
    String tableName = "table01";
    LeadControllerManager leadController = mock(LeadControllerManager.class);
    ControllerMetrics metrics = mock(ControllerMetrics.class);
    ExecutorService exec = Executors.newCachedThreadPool();
    ControllerConf cfg = new ControllerConf();
    cfg.setRebalanceCheckerCheckOnly(false);

    Map<String, Map<String, String>> allJobMetadata = new HashMap<>();
    // Original job run as job1, and all its retry jobs failed too.
    RebalanceConfig jobCfg = new RebalanceConfig();
    jobCfg.setJobId("job1");
    jobCfg.setMaxRetry(3);
    TableRebalanceProgressStats stats = new TableRebalanceProgressStats();
    stats.setStatus("FAILED");
    stats.setStartTimeMs(1000);
    TableRebalanceRetryConfig retryCfg = TableRebalanceRetryConfig.forInitialRun("job1", jobCfg);
    Map<String, String> jobMetadata =
        ZkBasedTableRebalanceObserver.createJobMetadata(tableName, "job1", stats, retryCfg);
    allJobMetadata.put("job1", jobMetadata);
    // 3 failed retry runs for job1
    jobMetadata = createDummyRetryJob(tableName, "job1_1", "job1", 1, 1100, "FAILED");
    allJobMetadata.put("job1_1", jobMetadata);
    jobMetadata = createDummyRetryJob(tableName, "job1_2", "job1", 2, 1200, "FAILED");
    allJobMetadata.put("job1_2", jobMetadata);
    jobMetadata = createDummyRetryJob(tableName, "job1_3", "job1", 3, 5300, "FAILED");
    allJobMetadata.put("job1_3", jobMetadata);

    // Original job run as job2, and its retry job job2_1 completed.
    jobCfg = new RebalanceConfig();
    jobCfg.setJobId("job2");
    jobCfg.setMaxRetry(3);
    stats = new TableRebalanceProgressStats();
    stats.setStatus("FAILED");
    stats.setStartTimeMs(2000);
    retryCfg = TableRebalanceRetryConfig.forInitialRun("job2", jobCfg);
    jobMetadata = ZkBasedTableRebalanceObserver.createJobMetadata(tableName, "job2", stats, retryCfg);
    allJobMetadata.put("job2", jobMetadata);
    jobMetadata = createDummyRetryJob(tableName, "job2_1", "job2", 1, 2100, "DONE");
    allJobMetadata.put("job2_1", jobMetadata);

    // Original job run as job3, and failed to send out heartbeat in time.
    jobCfg = new RebalanceConfig();
    jobCfg.setJobId("job3");
    jobCfg.setMaxRetry(3);
    stats = new TableRebalanceProgressStats();
    stats.setStatus("IN_PROGRESS");
    stats.setStartTimeMs(3000);
    retryCfg = TableRebalanceRetryConfig.forInitialRun("job3", jobCfg);
    jobMetadata = ZkBasedTableRebalanceObserver.createJobMetadata(tableName, "job3", stats, retryCfg);
    jobMetadata.put(CommonConstants.ControllerJob.SUBMISSION_TIME_MS, "3000");
    allJobMetadata.put("job3", jobMetadata);

    PinotHelixResourceManager helixManager = mock(PinotHelixResourceManager.class);
    when(helixManager.getAllJobsForTable(tableName,
        Collections.singleton(ControllerJobType.TABLE_REBALANCE))).thenReturn(allJobMetadata);
    TableConfig tableConfig = mock(TableConfig.class);
    RebalanceChecker checker = new RebalanceChecker(helixManager, leadController, cfg, metrics, exec);
    // Although job1_3 was submitted most recently but job1 had exceeded maxRetry. Chose job3 to retry, which got
    // stuck at in progress status.
    checker.retryRebalanceTable(tableName, tableConfig);
    // The new retry job is for job3 and retryNum is increased to 1.
    ArgumentCaptor<ZkBasedTableRebalanceObserver> observerCaptor =
        ArgumentCaptor.forClass(ZkBasedTableRebalanceObserver.class);
    verify(helixManager, times(1)).rebalanceTable(eq(tableName), any(), anyString(), any(), observerCaptor.capture());
    ZkBasedTableRebalanceObserver observer = observerCaptor.getValue();
    retryCfg = observer.getTableRebalanceJobRetryConfig();
    assertEquals(retryCfg.getOriginalJobId(), "job3");
    assertEquals(retryCfg.getRetryNum(), 1);
  }

  @Test
  public void testRetryRebalanceWithBackoff()
      throws Exception {
    String tableName = "table01";
    LeadControllerManager leadController = mock(LeadControllerManager.class);
    ControllerMetrics metrics = mock(ControllerMetrics.class);
    ExecutorService exec = Executors.newCachedThreadPool();
    ControllerConf cfg = new ControllerConf();
    cfg.setRebalanceCheckerCheckOnly(false);

    Map<String, Map<String, String>> allJobMetadata = new HashMap<>();
    // Original job run as job1, and all its retry jobs failed too.
    RebalanceConfig jobCfg = new RebalanceConfig();
    jobCfg.setJobId("job1");
    jobCfg.setMaxRetry(3);
    long nowMs = System.currentTimeMillis();
    TableRebalanceProgressStats stats = new TableRebalanceProgressStats();
    stats.setStatus("FAILED");
    stats.setStartTimeMs(nowMs);
    TableRebalanceRetryConfig retryCfg = TableRebalanceRetryConfig.forInitialRun("job1", jobCfg);
    Map<String, String> jobMetadata =
        ZkBasedTableRebalanceObserver.createJobMetadata(tableName, "job1", stats, retryCfg);
    allJobMetadata.put("job1", jobMetadata);

    PinotHelixResourceManager helixManager = mock(PinotHelixResourceManager.class);
    when(helixManager.getAllJobsForTable(tableName,
        Collections.singleton(ControllerJobType.TABLE_REBALANCE))).thenReturn(allJobMetadata);
    TableConfig tableConfig = mock(TableConfig.class);
    RebalanceChecker checker = new RebalanceChecker(helixManager, leadController, cfg, metrics, exec);
    checker.retryRebalanceTable(tableName, tableConfig);
    // Retry for job1 is delayed with 5min backoff.
    ArgumentCaptor<ZkBasedTableRebalanceObserver> observerCaptor =
        ArgumentCaptor.forClass(ZkBasedTableRebalanceObserver.class);
    verify(helixManager, times(0)).rebalanceTable(eq(tableName), any(), anyString(), any(), observerCaptor.capture());

    // Set initial delay to 0 to disable retry backoff.
    jobCfg.setRetryInitialDelayInMs(0);
    jobMetadata =
        ZkBasedTableRebalanceObserver.createJobMetadata(tableName, "job1", stats, retryCfg);
    allJobMetadata.put("job1", jobMetadata);
    checker.retryRebalanceTable(tableName, tableConfig);
    // Retry for job1 is delayed with 5min backoff.
    observerCaptor = ArgumentCaptor.forClass(ZkBasedTableRebalanceObserver.class);
    verify(helixManager, times(1)).rebalanceTable(eq(tableName), any(), anyString(), any(), observerCaptor.capture());
  }

  @Test
  public void testAddUpdateControllerJobsForTable() {
    ControllerConf cfg = new ControllerConf();
    cfg.setZkStr("localhost:2181");
    cfg.setHelixClusterName("cluster01");
    PinotHelixResourceManager pinotHelixManager = new PinotHelixResourceManager(cfg);
    HelixManager helixZkManager = mock(HelixManager.class);
    ZkHelixPropertyStore propertyStore = mock(ZkHelixPropertyStore.class);
    String zkPath = ZKMetadataProvider.constructPropertyStorePathForControllerJob(ControllerJobType.TABLE_REBALANCE);
    ZNRecord jobsZnRecord = new ZNRecord("jobs");
    when(propertyStore.get(eq(zkPath), any(), eq(AccessOption.PERSISTENT))).thenReturn(jobsZnRecord);
    when(helixZkManager.getClusterManagmentTool()).thenReturn(mock(HelixAdmin.class));
    when(helixZkManager.getHelixPropertyStore()).thenReturn(propertyStore);
    when(helixZkManager.getHelixDataAccessor()).thenReturn(mock(HelixDataAccessor.class));
    pinotHelixManager.start(helixZkManager, null);

    pinotHelixManager.addControllerJobToZK("job1",
        ImmutableMap.of("jobId", "job1", "submissionTimeMs", "1000", "tableName", "table01"), zkPath, jmd -> true);
    pinotHelixManager.addControllerJobToZK("job2",
        ImmutableMap.of("jobId", "job2", "submissionTimeMs", "2000", "tableName", "table01"), zkPath, jmd -> false);
    pinotHelixManager.addControllerJobToZK("job3",
        ImmutableMap.of("jobId", "job3", "submissionTimeMs", "3000", "tableName", "table02"), zkPath, jmd -> true);
    pinotHelixManager.addControllerJobToZK("job4",
        ImmutableMap.of("jobId", "job4", "submissionTimeMs", "4000", "tableName", "table02"), zkPath, jmd -> true);
    Map<String, Map<String, String>> jmds = jobsZnRecord.getMapFields();
    assertEquals(jmds.size(), 3);
    assertTrue(jmds.containsKey("job1"));
    assertTrue(jmds.containsKey("job3"));
    assertTrue(jmds.containsKey("job4"));

    Set<String> expectedJobs01 = new HashSet<>();
    pinotHelixManager.updateAllJobsForTable("table01", zkPath, jmd -> expectedJobs01.add(jmd.get("jobId")));
    assertEquals(expectedJobs01.size(), 1);
    assertTrue(expectedJobs01.contains("job1"));

    Set<String> expectedJobs02 = new HashSet<>();
    pinotHelixManager.updateAllJobsForTable("table02", zkPath, jmd -> expectedJobs02.add(jmd.get("jobId")));
    assertEquals(expectedJobs02.size(), 2);
    assertTrue(expectedJobs02.contains("job3"));
    assertTrue(expectedJobs02.contains("job4"));
  }

  private static TableRebalanceRetryConfig createDummyRetryJobCfg(String retryJobId, String originalJobId,
      int retryNum) {
    TableRebalanceRetryConfig retryCfg = new TableRebalanceRetryConfig();
    RebalanceConfig cfg = new RebalanceConfig();
    cfg.setMaxRetry(3);
    cfg.setJobId(retryJobId);
    retryCfg.setOriginalJobId(originalJobId);
    retryCfg.setRetryNum(retryNum);
    retryCfg.setConfig(cfg);
    return retryCfg;
  }

  private static Map<String, String> createDummyRetryJob(String tableName, String retryJobId, String originalJobId,
      int retryNum, long startTimeMs, String status) {
    RebalanceConfig cfg = new RebalanceConfig();
    cfg.setJobId(retryJobId);
    cfg.setMaxRetry(3);
    TableRebalanceProgressStats stats = new TableRebalanceProgressStats();
    stats.setStatus(status);
    stats.setStartTimeMs(startTimeMs);
    TableRebalanceRetryConfig retryCfg = TableRebalanceRetryConfig.forRetryRun(originalJobId, cfg, retryNum);
    return ZkBasedTableRebalanceObserver.createJobMetadata(tableName, retryJobId, stats, retryCfg);
  }
}
