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
package org.apache.pinot.integration.tests.realtime.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.function.BooleanSupplier;
import javax.annotation.Nullable;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.core.data.manager.realtime.RealtimeTableDataManager;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DelayInjectingRealtimeTableDataManager extends RealtimeTableDataManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(DelayInjectingRealtimeTableDataManager.class);

  /**
   * Tracks whether the delayed target segment was present in the data manager when a subsequent segment's
   * OFFLINE-CONSUMING transition started. Only recorded on the delayed server (where _delayConfig != null).
   *
   * <p>Key: raw table name. Value: map of segment sequence number to whether the target segment was present
   * in {@code _segmentDataManagerMap} at the time the consuming transition began for that segment.
   */
  public static final Map<String, Map<Integer, Boolean>> SEGMENT_STATE_AT_CONSUME_START =
      new ConcurrentHashMap<>();

  /**
   * Tracks whether the locally-built immutable segment's CRC matches the ZK CRC (set by the committing server).
   * Captured inside {@link #replaceSegmentIfCrcMismatch}, which is called during {@code doAddOnlineSegment} when
   * the segment is already immutable (i.e., it was locally built and committed before the CONSUMING→ONLINE
   * transition arrives).
   *
   * <p>Key: raw table name. Value: map of segment sequence number to list of CRC match booleans (one per server).
   * A mismatch indicates the server built its local segment from different consumed data than the committing server.
   */
  public static final Map<String, Map<Integer, List<Boolean>>> CRC_MATCH_AFTER_ONLINE =
      new ConcurrentHashMap<>();

  public static void resetTracking() {
    SEGMENT_STATE_AT_CONSUME_START.clear();
    CRC_MATCH_AFTER_ONLINE.clear();
  }

  private final DelayInjectingTableConfig _delayConfig;

  public DelayInjectingRealtimeTableDataManager(Semaphore segmentBuildSemaphore,
      BooleanSupplier isServerReadyToServeQueries,
      @Nullable DelayInjectingTableConfig delayConfig) {
    super(segmentBuildSemaphore, isServerReadyToServeQueries);
    _delayConfig = delayConfig;
  }

  @Override
  public void addConsumingSegment(String segmentName)
      throws Exception {
    if (_delayConfig != null) {
      LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
      int seqNum = llcSegmentName.getSequenceNumber();
      int targetSeq = _delayConfig.getTargetSequenceNumber();

      // For segments after the delayed target, record whether the target is already loaded.
      // This captures the out-of-order condition: if targetPresent=false, the segment will
      // start consuming without the target's data in the upsert metadata.
      if (seqNum > targetSeq) {
        boolean targetPresent = isSegmentWithSequencePresent(targetSeq);
        SEGMENT_STATE_AT_CONSUME_START
            .computeIfAbsent(llcSegmentName.getTableName(), k -> new ConcurrentHashMap<>())
            .put(seqNum, targetPresent);
        LOGGER.info(
            "Before consuming segment {} (seq {}): target segment (seq {}) present: {}",
            segmentName, seqNum, targetSeq, targetPresent);
      }

      // Inject the consuming delay for the target segment
      if (seqNum == targetSeq && _delayConfig.getConsumingDelayMs() > 0) {
        LOGGER.info("Injecting consuming delay of {}ms for segment {} (seq {})",
            _delayConfig.getConsumingDelayMs(), segmentName, seqNum);
        Thread.sleep(_delayConfig.getConsumingDelayMs());
      }
    }
    super.addConsumingSegment(segmentName);
  }

  @Override
  protected void doAddOnlineSegment(String segmentName)
      throws Exception {
    if (_delayConfig != null && _delayConfig.getDelayMs() > 0) {
      LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
      if (llcSegmentName.getSequenceNumber() == _delayConfig.getTargetSequenceNumber()) {
        LOGGER.info("Injecting online delay of {}ms for segment {} (seq {})",
            _delayConfig.getDelayMs(), segmentName, llcSegmentName.getSequenceNumber());
        Thread.sleep(_delayConfig.getDelayMs());
      }
    }
    super.doAddOnlineSegment(segmentName);
  }

  /**
   * Intercepts the CRC mismatch check that runs inside {@code doAddOnlineSegment} when the segment is already
   * immutable. This is called for the committing server (whose locally-built segment was committed before
   * CONSUMING→ONLINE arrives) and records whether the local CRC matches the ZK CRC before {@code super} can
   * download a replacement and mask the mismatch.
   */
  @Override
  protected void replaceSegmentIfCrcMismatch(SegmentDataManager segmentDataManager, SegmentZKMetadata zkMetadata,
      IndexLoadingConfig indexLoadingConfig)
      throws Exception {
    String segmentName = segmentDataManager.getSegmentName();
    LLCSegmentName llcSegmentName = LLCSegmentName.of(segmentName);
    if (llcSegmentName != null) {
      try {
        SegmentMetadata localMetadata = segmentDataManager.getSegment().getSegmentMetadata();
        String localCrc = localMetadata.getCrc();
        long zkCrc = zkMetadata.getCrc();
        boolean match = localCrc != null && (zkCrc == Long.parseLong(localCrc));
        LOGGER.info("CRC check for segment {} (seq {}): localCrc={}, zkCrc={}, match={}",
            segmentName, llcSegmentName.getSequenceNumber(), localCrc, zkCrc, match);
        CRC_MATCH_AFTER_ONLINE
            .computeIfAbsent(llcSegmentName.getTableName(), k -> new ConcurrentHashMap<>())
            .computeIfAbsent(llcSegmentName.getSequenceNumber(),
                k -> Collections.synchronizedList(new ArrayList<>()))
            .add(match);
      } catch (Exception e) {
        LOGGER.warn("Failed to track CRC for segment {}: {}", segmentName, e.getMessage());
      }
    }
    super.replaceSegmentIfCrcMismatch(segmentDataManager, zkMetadata, indexLoadingConfig);
  }

  private boolean isSegmentWithSequencePresent(int targetSequenceNumber) {
    for (String name : _segmentDataManagerMap.keySet()) {
      LLCSegmentName llcName = LLCSegmentName.of(name);
      if (llcName != null && llcName.getSequenceNumber() == targetSequenceNumber) {
        return true;
      }
    }
    return false;
  }
}
