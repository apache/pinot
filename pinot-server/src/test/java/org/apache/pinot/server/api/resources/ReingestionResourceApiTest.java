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
package org.apache.pinot.server.api.resources;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.core.data.manager.realtime.RealtimeTableDataManager;
import org.apache.pinot.server.api.BaseResourceTest;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.mockito.stubbing.Answer;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.server.api.resources.ReingestionResource.MAX_PARALLEL_REINGESTIONS;
import static org.apache.pinot.server.api.resources.ReingestionResource.waitForCondition;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/// Tests that the [ReingestionResource] is served by the admin API with its injected dependencies, keeps track of the
/// re-ingestion jobs across requests, and stops them when the admin API stops.
public class ReingestionResourceApiTest extends BaseResourceTest {
  private static final String REINGESTION_RAW_TABLE_NAME = "reingestionTable";
  private static final String REINGESTION_REALTIME_TABLE_NAME = REINGESTION_RAW_TABLE_NAME + "_REALTIME";
  // Makes the segment names unique across tests, since a segment is held until its previous job finishes
  private static final AtomicInteger SEQUENCE_NUMBER = new AtomicInteger();

  /// Table availability when a queued re-ingestion starts.
  private enum TableState {
    AVAILABLE, REMOVED, SHUT_DOWN
  }

  @Test
  public void testRunningReingestionIsTrackedAcrossRequests()
      throws Exception {
    // Hold the job on the re-ingestion thread, where the segment build semaphore is fetched
    CountDownLatch jobStarted = new CountDownLatch(1);
    CountDownLatch releaseJob = new CountDownLatch(1);
    addTableDataManager(invocation -> {
      jobStarted.countDown();
      releaseJob.await();
      return null;
    });

    try {
      String segmentName = segmentName(0);
      assertThat(postReingestion(segmentName)).isEqualTo(Response.Status.OK.getStatusCode());
      assertThat(jobStarted.await(10, TimeUnit.SECONDS)).isTrue();

      // Only visible to the next requests when they are served by the same resource instance
      assertThat(postReingestion(segmentName)).isEqualTo(Response.Status.CONFLICT.getStatusCode());
      assertThat(getJobs()).contains(segmentName);
    } finally {
      releaseJob.countDown();
      _tableDataManagerMap.remove(REINGESTION_REALTIME_TABLE_NAME);
    }
  }

  @Test
  public void testQueuedReingestionIsListed() {
    // Hold every re-ingestion thread so that the last job waits in the queue
    CountDownLatch releaseJobs = new CountDownLatch(1);
    addTableDataManager(invocation -> {
      releaseJobs.await();
      return null;
    });

    try {
      String queuedSegmentName = null;
      for (int partitionId = 0; partitionId <= MAX_PARALLEL_REINGESTIONS; partitionId++) {
        queuedSegmentName = segmentName(partitionId);
        assertThat(postReingestion(queuedSegmentName)).isEqualTo(Response.Status.OK.getStatusCode());
      }
      assertThat(getJobs()).contains(queuedSegmentName);
    } finally {
      releaseJobs.countDown();
      _tableDataManagerMap.remove(REINGESTION_REALTIME_TABLE_NAME);
    }
  }

  @DataProvider
  public Object[][] staleQueuedReingestions() {
    return new Object[][]{
        {TableState.AVAILABLE, Status.DONE, null},
        {TableState.AVAILABLE, null, null},
        {TableState.AVAILABLE, Status.IN_PROGRESS, null},
        {TableState.AVAILABLE, Status.COMMITTING, "http://deep-store/segment"},
        {TableState.REMOVED, Status.COMMITTING, null},
        {TableState.SHUT_DOWN, Status.COMMITTING, null}
    };
  }

  @Test(dataProvider = "staleQueuedReingestions")
  public void testQueuedReingestionSkipsObsoleteJob(TableState tableState, @Nullable Status status,
      @Nullable String downloadUrl)
      throws Exception {
    waitForCondition(() -> getJobs().equals("[]"), "previous re-ingestions to finish", 10, 10_000);
    CountDownLatch jobsStarted = new CountDownLatch(MAX_PARALLEL_REINGESTIONS);
    CountDownLatch releaseJobs = new CountDownLatch(1);
    addTableDataManager(invocation -> {
      jobsStarted.countDown();
      releaseJobs.await();
      throw new IllegalStateException("Finish blocker without building a segment");
    });

    String targetRawTableName = "queuedReingestionTable";
    String targetTableName = targetRawTableName + "_REALTIME";
    String targetSegmentName = segmentName(targetRawTableName, 0);
    AtomicReference<SegmentZKMetadata> metadata = new AtomicReference<>(segmentMetadata(targetSegmentName));
    RealtimeTableDataManager targetTableDataManager = mock(RealtimeTableDataManager.class);
    when(targetTableDataManager.fetchZKMetadata(targetSegmentName)).thenAnswer(invocation -> metadata.get());
    when(targetTableDataManager.fetchZKMetadataNullable(targetSegmentName)).thenAnswer(invocation -> metadata.get());
    _tableDataManagerMap.put(targetTableName, targetTableDataManager);

    try {
      for (int partitionId = 0; partitionId < MAX_PARALLEL_REINGESTIONS; partitionId++) {
        assertThat(postReingestion(segmentName(partitionId))).isEqualTo(Response.Status.OK.getStatusCode());
      }
      assertThat(jobsStarted.await(10, TimeUnit.SECONDS)).isTrue();
      assertThat(postReingestion(targetSegmentName)).isEqualTo(Response.Status.OK.getStatusCode());
      assertThat(getJobs()).contains(targetSegmentName);

      // Change the metadata or table availability while the job is queued.
      SegmentZKMetadata updatedMetadata = null;
      if (status != null) {
        updatedMetadata = segmentMetadata(targetSegmentName);
        updatedMetadata.setStatus(status);
        updatedMetadata.setDownloadUrl(downloadUrl);
      }
      metadata.set(updatedMetadata);
      if (tableState == TableState.REMOVED) {
        _tableDataManagerMap.remove(targetTableName);
      } else if (tableState == TableState.SHUT_DOWN) {
        when(targetTableDataManager.isShutDown()).thenReturn(true);
      }
      releaseJobs.countDown();
      waitForCondition(() -> !getJobs().contains(targetSegmentName), "queued re-ingestion to be removed", 10, 10_000);

      verify(targetTableDataManager).fetchZKMetadata(targetSegmentName);
      verify(targetTableDataManager, tableState == TableState.AVAILABLE ? times(1) : never())
          .fetchZKMetadataNullable(targetSegmentName);
      verify(targetTableDataManager, never()).fetchIndexLoadingConfig();
      verify(targetTableDataManager, never()).getSegmentBuildSemaphore();
      verify(_serverInstance.getServerMetrics(), never())
          .addMeteredTableValue(targetTableName, ServerMeter.SEGMENT_REINGESTION_FAILURE, 1);
    } finally {
      releaseJobs.countDown();
      _tableDataManagerMap.remove(REINGESTION_REALTIME_TABLE_NAME);
      _tableDataManagerMap.remove(targetTableName);
    }
  }

  // Runs after the other tests since it stops the admin API
  @Test(priority = 1)
  public void testRunningReingestionIsStoppedOnShutdown()
      throws Exception {
    // Hold the job on the re-ingestion thread until it is interrupted
    AtomicReference<Thread> reingestionThread = new AtomicReference<>();
    CountDownLatch jobStarted = new CountDownLatch(1);
    addTableDataManager(invocation -> {
      reingestionThread.set(Thread.currentThread());
      jobStarted.countDown();
      new CountDownLatch(1).await();
      return null;
    });
    assertThat(postReingestion(segmentName(0))).isEqualTo(Response.Status.OK.getStatusCode());
    assertThat(jobStarted.await(10, TimeUnit.SECONDS)).isTrue();

    _adminApiApplication.stop();

    reingestionThread.get().join(10_000);
    assertThat(reingestionThread.get().isAlive()).isFalse();
  }

  /// Adds a table data manager whose segments are pending re-ingestion, and which answers the segment build semaphore
  /// fetched on the re-ingestion thread with the given answer.
  private void addTableDataManager(Answer<Semaphore> segmentBuildSemaphoreAnswer) {
    RealtimeTableDataManager tableDataManager = mock(RealtimeTableDataManager.class);
    when(tableDataManager.fetchZKMetadata(anyString()))
        .thenAnswer(invocation -> segmentMetadata(invocation.getArgument(0)));
    when(tableDataManager.fetchZKMetadataNullable(anyString()))
        .thenAnswer(invocation -> segmentMetadata(invocation.getArgument(0)));
    when(tableDataManager.getSegmentBuildSemaphore()).thenAnswer(segmentBuildSemaphoreAnswer);
    _tableDataManagerMap.put(REINGESTION_REALTIME_TABLE_NAME, tableDataManager);
  }

  private static String segmentName(int partitionId) {
    return segmentName(REINGESTION_RAW_TABLE_NAME, partitionId);
  }

  private static String segmentName(String rawTableName, int partitionId) {
    return new LLCSegmentName(rawTableName, partitionId, SEQUENCE_NUMBER.getAndIncrement(),
        System.currentTimeMillis()).getSegmentName();
  }

  private static SegmentZKMetadata segmentMetadata(String segmentName) {
    SegmentZKMetadata metadata = new SegmentZKMetadata(segmentName);
    metadata.setStatus(Status.COMMITTING);
    metadata.setStartOffset("0");
    metadata.setEndOffset("100");
    return metadata;
  }

  private int postReingestion(String segmentName) {
    try (Response response = _webTarget.path("/reingestSegment/" + segmentName).request().post(Entity.json(""))) {
      return response.getStatus();
    }
  }

  private String getJobs() {
    return _webTarget.path("/reingestSegment/jobs").request().get(String.class);
  }
}
