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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Uninterruptibles;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.server.realtime.ServerSegmentCompletionProtocolHandler;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Extend the lease for build time. Keep a map of segments for which lease needs to be extended.
 * Repeat lease extension periodically as often as necessary until the segment name is removed
 * from the map.
 */
public class SegmentBuildTimeLeaseExtender {
  private static final int MAX_NUM_ATTEMPTS = 3;
  // Always request 120s of extra build time
  private static final int EXTRA_TIME_SECONDS = 120;
  // Retransmit lease request 10% before lease expires.
  private static final int REPEAT_REQUEST_PERIOD_SEC = (EXTRA_TIME_SECONDS * 9 / 10);
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentBuildTimeLeaseExtender.class);
  private static final Map<String, SegmentBuildTimeLeaseExtender> TABLE_TO_LEASE_EXTENDER = new ConcurrentHashMap<>();
  private static ScheduledExecutorService _executor;

  private final Map<String, Future> _segmentToFutureMap = new ConcurrentHashMap<>();
  private final String _instanceId;
  private final String _tableNameWithType;
  private final ServerSegmentCompletionProtocolHandler _protocolHandler;

  public static void initExecutor() {
    _executor = new ScheduledThreadPoolExecutor(1);
    LOGGER.info("Initialized segment build time lease extender executor");
  }

  public static void shutdownExecutor() {
    if (_executor != null) {
      _executor.shutdownNow();
      _executor = null;
    }
    LOGGER.info("Shut down segment build time lease extender executor");
  }

  @VisibleForTesting
  public static boolean isExecutorShutdown() {
    return _executor == null;
  }

  public static SegmentBuildTimeLeaseExtender getLeaseExtender(final String tableNameWithType) {
    return TABLE_TO_LEASE_EXTENDER.get(tableNameWithType);
  }

  public static SegmentBuildTimeLeaseExtender getOrCreate(final String instanceId, ServerMetrics serverMetrics,
      String tableNameWithType) {
    return TABLE_TO_LEASE_EXTENDER.compute(tableNameWithType, (k, v) -> {
      if (v == null) {
        SegmentBuildTimeLeaseExtender leaseExtender =
            new SegmentBuildTimeLeaseExtender(instanceId, serverMetrics, tableNameWithType);
        LOGGER.info("Created lease extender for table: {}", tableNameWithType);
        return leaseExtender;
      } else {
        LOGGER.warn("Lease extender for table: {} already exists", tableNameWithType);
        return v;
      }
    });
  }

  private SegmentBuildTimeLeaseExtender(String instanceId, ServerMetrics serverMetrics, String tableNameWithType) {
    _instanceId = instanceId;
    _tableNameWithType = tableNameWithType;
    _protocolHandler = new ServerSegmentCompletionProtocolHandler(serverMetrics, tableNameWithType);
  }

  public void shutDown() {
    for (Map.Entry<String, Future> entry : _segmentToFutureMap.entrySet()) {
      Future future = entry.getValue();
      boolean cancelled = future.cancel(true);
      if (!cancelled) {
        LOGGER.warn("Task could not be cancelled for [{}]", entry.getKey());
      }
    }
    _segmentToFutureMap.clear();
    TABLE_TO_LEASE_EXTENDER.remove(_tableNameWithType);
  }

  /**
   * Adds a segment for periodic lease request.
   * The first lease request is sent before {@param initialBuildTimeMs} exipres. Subsequent lease requests are sent
   * within two minutes.
   * @param segmentId is the name of he segment that is being built
   * @param initialBuildTimeMs is the initial time budget that SegmentCompletionManager has allocated.
   * @param offset The offset at which this segment is being built.
   */
  public void addSegment(String segmentId, long initialBuildTimeMs, StreamPartitionMsgOffset offset) {
    final long initialDelayMs = initialBuildTimeMs * 9 / 10;
    final SegmentCompletionProtocol.Request.Params reqParams = new SegmentCompletionProtocol.Request.Params();
    reqParams.withStreamPartitionMsgOffset(offset.toString()).withSegmentName(segmentId)
        .withExtraTimeSec(EXTRA_TIME_SECONDS).withInstanceId(_instanceId);
    Future future = _executor
        .scheduleWithFixedDelay(new LeaseExtender(reqParams), initialDelayMs, REPEAT_REQUEST_PERIOD_SEC * 1000L,
            TimeUnit.MILLISECONDS);
    _segmentToFutureMap.put(segmentId, future);
  }

  public void removeSegment(final String segmentId) {
    Future future = _segmentToFutureMap.get(segmentId);
    if (future != null) {
      boolean cancelled = future.cancel(true);
      if (!cancelled) {
        LOGGER.warn("Task could not be cancelled for {}", segmentId);
      }
    }
    _segmentToFutureMap.remove(segmentId);
  }

  private class LeaseExtender implements Runnable {

    private final SegmentCompletionProtocol.Request.Params _params;

    private LeaseExtender(final SegmentCompletionProtocol.Request.Params params) {
      _params = params;
    }

    @Override
    public void run() {
      int nAttempts = 0;
      SegmentCompletionProtocol.ControllerResponseStatus status =
          SegmentCompletionProtocol.ControllerResponseStatus.NOT_SENT;
      final String segmentId = _params.getSegmentName();

      // Attempt to send a lease renewal message for MAX_NUM_ATTEMPTS number of times. If unsuccessful,
      // log a warning and let things take their course. At worst, the segment that is built will be rejected
      // in favor of another server.
      while (status != SegmentCompletionProtocol.ControllerResponseStatus.PROCESSED && nAttempts < MAX_NUM_ATTEMPTS) {
        try {
          SegmentCompletionProtocol.Response response = _protocolHandler.extendBuildTime(_params);
          status = response.getStatus();
        } catch (Exception e) {
          LOGGER.warn("Exception trying to send lease renewal for {}", segmentId);
        }
        if (status != SegmentCompletionProtocol.ControllerResponseStatus.PROCESSED) {
          Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
          LOGGER.warn("Retrying lease extension for {} because controller status {}", segmentId, status.toString());
          nAttempts++;
        }
      }
      if (nAttempts >= MAX_NUM_ATTEMPTS) {
        LOGGER.error("Failed to send lease extension for {}", segmentId);
      }
    }
  }
}
