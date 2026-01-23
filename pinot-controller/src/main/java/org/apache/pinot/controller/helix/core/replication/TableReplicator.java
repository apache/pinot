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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.controller.api.resources.CopyTablePayload;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.WatermarkInductionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Replicates a table from a source cluster to a destination cluster.
 */
public class TableReplicator {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableReplicator.class);
  private static final String SEGMENT_ZK_METADATA_ENDPOINT_TEMPLATE = "/segments/%s/zkmetadata";

  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final ExecutorService _executorService;
  private final SegmentCopier _segmentCopier;
  private final HttpClient _httpClient;

  public TableReplicator(PinotHelixResourceManager pinotHelixResourceManager, ExecutorService executorService,
      SegmentCopier segmentCopier) {
    this(pinotHelixResourceManager, executorService, segmentCopier, HttpClient.getInstance());
  }

  public TableReplicator(PinotHelixResourceManager pinotHelixResourceManager, ExecutorService executorService,
      SegmentCopier segmentCopier, HttpClient httpClient) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _executorService = executorService;
    _segmentCopier = segmentCopier;
    _httpClient = httpClient;
  }

  /**
   * Replicates the table by copying segments from source to destination.
   *
   * This method performs the following steps:
   * 1. Fetch ZK metadata for all segments of the table from the source cluster.
   * 2. Register a new controller job in Zookeeper to track the replication progress.
   * 3. Initialize a {@link ZkBasedTableReplicationObserver} to update the job status in Zookeeper.
   * 4. Submit tasks to the executor service to copy segments in parallel.
   * 5. Each task copies a segment and triggers the observer to update the progress.
   *
   * @param jobId The job ID.
   * @param tableNameWithType The table name with type.
   * @param copyTablePayload The payload containing the source and destination cluster information.
   * @param res The watermark induction result.
   * @throws Exception If an error occurs during replication.
   */
  public void replicateTable(String jobId, String tableNameWithType, CopyTablePayload copyTablePayload,
      WatermarkInductionResult res)
      throws Exception {
    // TODO: throw IllegalStateException if any previous jobs doesn't expire.
    // TODO: replication job canceling mechanism
    LOGGER.info("[copyTable] Start replicating table: {} with jobId: {}", tableNameWithType, jobId);
    URI zkMetadataUri = new URI(copyTablePayload.getSourceClusterUri()
        + String.format(SEGMENT_ZK_METADATA_ENDPOINT_TEMPLATE, tableNameWithType));
    SimpleHttpResponse zkMetadataResponse = HttpClient.wrapAndThrowHttpException(
        _httpClient.sendGetRequest(zkMetadataUri, copyTablePayload.getHeaders()));
    String zkMetadataJson = zkMetadataResponse.getResponse();
    Map<String, Map<String, String>> zkMetadataMap =
        new ObjectMapper().readValue(zkMetadataJson, new TypeReference<Map<String, Map<String, String>>>() {
        });
    LOGGER.info("[copyTable] Fetched ZK metadata for {} segments", zkMetadataMap.size());

    List<String> segments = new ArrayList<>(res.getHistoricalSegments());
    long submitTS = System.currentTimeMillis();

    if (!_pinotHelixResourceManager.addNewTableReplicationJob(tableNameWithType, jobId, submitTS, res)) {
      throw new Exception("Failed to add segments to replicated table");
    }
    ZkBasedTableReplicationObserver observer = new ZkBasedTableReplicationObserver(jobId, tableNameWithType, res,
        _pinotHelixResourceManager);
    observer.onTrigger(TableReplicationObserver.Trigger.START_TRIGGER, null);
    ConcurrentLinkedQueue<String> q = new ConcurrentLinkedQueue<>(segments);
    int parallelism = copyTablePayload.getBackfillParallism() != null
        ? copyTablePayload.getBackfillParallism()
        : res.getWatermarks().size();
    for (int i = 0; i < parallelism; i++) {
      _executorService.submit(() -> {
        while (true) {
          String segment = q.poll();
          if (segment == null) {
            break;
          }
          try {
            LOGGER.info("[copyTable] Starting to copy segment: {} for table: {}", segment, tableNameWithType);
            Map<String, String> segmentZKMetadata = zkMetadataMap.get(segment);
            if (segmentZKMetadata == null) {
              throw new RuntimeException("Segment ZK metadata not found for segment: " + segment);
            }
            _segmentCopier.copy(tableNameWithType, segment, copyTablePayload, segmentZKMetadata);
            observer.onTrigger(TableReplicationObserver.Trigger.SEGMENT_REPLICATE_COMPLETED_TRIGGER, segment);
          } catch (Exception e) {
            LOGGER.error("Caught exception while replicating table segment", e);
            observer.onTrigger(TableReplicationObserver.Trigger.SEGMENT_REPLICATE_ERRORED_TRIGGER, segment);
          }
        }
      });
    }
    LOGGER.info("[copyTable] Submitted replication tasks to executor service for job: {}", jobId);
  }
}
