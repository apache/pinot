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
package org.apache.pinot.controller.helix.core.version;

import javax.annotation.Nullable;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;


/**
 * Controller-side service that aggregates per-instance version data from Helix and provides
 * cluster-wide version summaries and rollout-order compatibility checks.
 *
 * <p>Implementations cache the result of Helix reads for a configurable TTL to avoid
 * excessive ZooKeeper traffic on high-volume API calls.  The snapshot is eventually
 * consistent and may differ across controller instances by up to the cache TTL.
 *
 * <p>The service also implements {@link PinotClusterConfigChangeListener} so that the cache
 * TTL can be reconfigured at runtime via Helix cluster config without a controller restart.
 *
 * <p>In v1 all compatibility checks are advisory (warning-only). No operation is blocked
 * based on the results returned by this service.
 */
public interface VersionCompatibilityService extends PinotClusterConfigChangeListener {

  /**
   * Returns a cluster-wide version snapshot grouped by component type.
   *
   * <p>Results are served from an in-memory cache that refreshes after the configured TTL.
   * If the last Helix read failed and no cache is available, the returned summary will have
   * {@link ClusterVersionSummary#isDataAvailable()} set to {@code false}.
   */
  ClusterVersionSummary getClusterVersionSummary();

  /**
   * Returns version information for a single instance, or {@code null} if the instance is
   * not registered in Helix.
   *
   * <p>This always reads from the cached snapshot; call {@link #invalidateCache()} first if
   * a fresh read is required.
   */
  @Nullable
  InstanceVersionInfo getInstanceVersionInfo(String instanceName);

  /**
   * Evaluates the recommended rollout-order invariant:
   * {@code min(controller) >= min(broker) >= min(server)} and {@code min(broker) >= min(minion)}.
   *
   * <p>SERVER and MINION are peers beneath BROKER — neither is required to be at least the
   * version of the other.
   *
   * <p>Only live instances with known versions participate in the comparison.
   * Instances with UNKNOWN versions are reported separately in the warnings list
   * but do not block the check from completing.
   *
   * @return a result whose {@link CompatibilityCheckResult#isOk()} is {@code true} when no
   *         violations are detected
   */
  CompatibilityCheckResult checkRolloutOrderCompatibility();

  /**
   * Drops the cached snapshot, forcing the next read to re-query Helix.
   * Intended for testing and explicit operator-driven refresh.
   */
  void invalidateCache();
}
