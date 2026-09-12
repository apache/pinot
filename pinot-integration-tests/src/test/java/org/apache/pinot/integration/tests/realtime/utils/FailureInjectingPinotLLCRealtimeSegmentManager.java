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
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.controller.helix.core.util.FailureInjectionUtils;


public class FailureInjectingPinotLLCRealtimeSegmentManager extends PinotLLCRealtimeSegmentManager {
  @VisibleForTesting
  private final Map<String, String> _failureConfig;
  private final Set<String> _forceExpiredSegments = ConcurrentHashMap.newKeySet();

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
