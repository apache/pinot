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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Uninterruptibles;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.controller.helix.core.util.FailureInjectionUtils;
import org.apache.pinot.spi.config.table.TableConfig;


public class FailureInjectingPinotLLCRealtimeSegmentManager extends PinotLLCRealtimeSegmentManager {
  @VisibleForTesting
  private final Map<String, String> _failureConfig;
  private final Set<String> _forceExpiredSegments = ConcurrentHashMap.newKeySet();
  // Non-null while table config reads are blocked, see blockTableConfigReads()
  private volatile CountDownLatch _tableConfigReadLatch;

  public FailureInjectingPinotLLCRealtimeSegmentManager(PinotHelixResourceManager helixResourceManager,
      ControllerConf controllerConf, ControllerMetrics controllerMetrics) {
    super(helixResourceManager, controllerConf, controllerMetrics);
    _failureConfig = new ConcurrentHashMap<>();
  }

  /// Treats force-expired segments as exceeding the max segment completion time regardless of how recently their
  /// segment ZK metadata was updated. This makes them immediately eligible for repair while keeping their in-flight
  /// commit attempts rejected, without shortening the completion time for any other segment.
  @Override
  protected boolean isExceededMaxSegmentCompletionTime(String realtimeTableName, String segmentName,
      long currentTimeMs) {
    return _forceExpiredSegments.contains(segmentName)
        || super.isExceededMaxSegmentCompletionTime(realtimeTableName, segmentName, currentTimeMs);
  }

  public void forceExpireSegments(Collection<String> segmentNames) {
    _forceExpiredSegments.addAll(segmentNames);
  }

  public void clearForceExpiredSegments() {
    _forceExpiredSegments.clear();
  }

  /// Makes table config reads wait until [#unblockTableConfigReads()] is called, like reads that queue behind a burst
  /// of other requests on the controller's ZooKeeper session.
  public void blockTableConfigReads() {
    _tableConfigReadLatch = new CountDownLatch(1);
  }

  public void unblockTableConfigReads() {
    CountDownLatch latch = _tableConfigReadLatch;
    if (latch != null) {
      _tableConfigReadLatch = null;
      latch.countDown();
    }
  }

  @Override
  public TableConfig getTableConfig(String realtimeTableName) {
    CountDownLatch latch = _tableConfigReadLatch;
    if (latch != null) {
      // Wait longer than the server's request timeout, but not forever in case a test does not unblock the reads
      Uninterruptibles.awaitUninterruptibly(latch, 1, TimeUnit.MINUTES);
    }
    return super.getTableConfig(realtimeTableName);
  }

  @VisibleForTesting
  public void enableTestFault(String faultType) {
    if (faultType != null) {
      _failureConfig.put(faultType, "true");
    }
  }

  @VisibleForTesting
  public void disableTestFault(String faultType) {
    if (faultType != null) {
      _failureConfig.remove(faultType);
    }
  }

  @Override
  protected void preProcessNewSegmentZKMetadata() {
    FailureInjectionUtils.injectFailure(FailureInjectionUtils.FAULT_BEFORE_NEW_SEGMENT_METADATA_CREATION,
        _failureConfig);
  }

  @Override
  protected void preProcessCommitIdealStateUpdate() {
    FailureInjectionUtils.injectFailure(FailureInjectionUtils.FAULT_BEFORE_IDEAL_STATE_UPDATE, _failureConfig);
  }

  @Override
  protected void preProcessCommitSegmentEndMetadata() {
    FailureInjectionUtils.injectFailure(FailureInjectionUtils.FAULT_BEFORE_COMMIT_END_METADATA, _failureConfig);
  }
}
