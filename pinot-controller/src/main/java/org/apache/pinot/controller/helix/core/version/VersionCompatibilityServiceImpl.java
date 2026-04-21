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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.common.version.ParsedVersion;
import org.apache.pinot.common.version.PinotVersion;
import org.apache.pinot.common.version.VersionParsingUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Default implementation of {@link VersionCompatibilityService}.
 *
 * <p><b>Caching:</b> Helix is queried at most once per {@link #_cacheTtlMs} milliseconds.
 * The TTL is seeded from {@link ControllerConf} at construction time and may be overridden at
 * runtime by the Helix cluster config key
 * {@link ControllerConf.ControllerPeriodicTasksConf#VERSION_HEALTH_CHECK_CACHE_TTL_SECONDS}.
 *
 * <p><b>Live-instance filtering:</b> Only instances with an active Helix LiveInstance ZNode
 * participate in min/max version calculations.  Dead instances with stale InstanceConfigs are
 * excluded to avoid misleading compatibility signals during rolling upgrades.
 *
 * <p><b>Thread safety:</b> All public methods are safe to call concurrently.
 */
public class VersionCompatibilityServiceImpl implements VersionCompatibilityService {
  private static final Logger LOGGER = LoggerFactory.getLogger(VersionCompatibilityServiceImpl.class);

  static final String CHECK_TYPE_ROLLOUT_ORDER = "ROLLOUT_ORDER";
  // Fallback cluster name used only when Helix is unreachable AND no cached snapshot exists.
  // Distinct from PinotVersion.UNKNOWN and InstanceVersionInfo.UNKNOWN_TYPE to avoid cognitive
  // collision in downstream JSON output.
  private static final String UNKNOWN_CLUSTER = "UNKNOWN_CLUSTER";
  private static final String CONTROLLER = "CONTROLLER";
  private static final String BROKER = "BROKER";
  private static final String SERVER = "SERVER";
  private static final String MINION = "MINION";
  // Iteration order when constructing the per-component summary map.
  private static final List<String> COMPONENT_TYPES = List.of(CONTROLLER, BROKER, SERVER, MINION);

  // Upper bound on cache TTL to prevent a misconfiguration from keeping stale data indefinitely.
  private static final long MAX_CACHE_TTL_MS = 60L * 60L * 1000L; // 1 hour

  private final PinotHelixResourceManager _helixResourceManager;
  private volatile long _cacheTtlMs;

  // Atomic so that reads never block and the reference is always consistent.
  private final AtomicReference<CachedSnapshot> _cacheRef = new AtomicReference<>();

  public VersionCompatibilityServiceImpl(PinotHelixResourceManager helixResourceManager,
      ControllerConf controllerConf) {
    _helixResourceManager = helixResourceManager;
    _cacheTtlMs = clampTtlMs(controllerConf.getVersionHealthCheckCacheTtlSeconds() * 1000L);
  }

  private static long clampTtlMs(long ttlMs) {
    if (ttlMs <= 0) {
      LOGGER.warn("Non-positive version compatibility cache TTL ({}ms); clamping to 1s", ttlMs);
      return 1000L;
    }
    if (ttlMs > MAX_CACHE_TTL_MS) {
      LOGGER.warn("Version compatibility cache TTL ({}ms) exceeds max ({}ms); clamping",
          ttlMs, MAX_CACHE_TTL_MS);
      return MAX_CACHE_TTL_MS;
    }
    return ttlMs;
  }

  // -------------------------------------------------------------------------
  // VersionCompatibilityService interface
  // -------------------------------------------------------------------------

  @Override
  public ClusterVersionSummary getClusterVersionSummary() {
    return getOrRefreshCache()._summary;
  }

  @Override
  @Nullable
  public InstanceVersionInfo getInstanceVersionInfo(String instanceName) {
    return getOrRefreshCache()._instanceIndex.get(instanceName);
  }

  @Override
  public CompatibilityCheckResult checkRolloutOrderCompatibility() {
    ClusterVersionSummary summary = getOrRefreshCache()._summary;
    if (!summary.isDataAvailable()) {
      String msg = "Version data is unavailable (Helix read failed).";
      // Include the message as the first warning too, so clients that only inspect `warnings`
      // still see the cause rather than an empty list.
      return new CompatibilityCheckResult(CHECK_TYPE_ROLLOUT_ORDER, false, msg,
          Collections.singletonList(msg), summary);
    }

    List<String> warnings = new ArrayList<>();

    // UNKNOWN-version reporting: surface these separately so operators can investigate.
    for (String type : COMPONENT_TYPES) {
      ComponentVersionSummary comp = summary.getComponentSummaries().get(type);
      if (comp != null && comp.getUnknownVersionCount() > 0) {
        warnings.add(String.format(
            "%s has %d live instance(s) with UNKNOWN version. These cannot participate in "
                + "rollout-order validation.",
            type, comp.getUnknownVersionCount()));
      }
    }

    // Invariant is a DAG, not a sliding window:
    //   min(BROKER)  <= min(CONTROLLER)
    //   min(SERVER)  <= min(BROKER) (or <= min(CONTROLLER) if no brokers are live)
    //   min(MINION)  <= min(BROKER) (or <= min(CONTROLLER) if no brokers are live)
    String controllerMin = minVersion(summary, CONTROLLER);
    String brokerMin = minVersion(summary, BROKER);
    String serverMin = minVersion(summary, SERVER);
    String minionMin = minVersion(summary, MINION);

    if (brokerMin != null && controllerMin != null
        && VersionParsingUtils.compare(brokerMin, controllerMin) > 0) {
      warnings.add(String.format(
          "Rollout order violation: BROKER min version (%s) is newer than CONTROLLER min version "
              + "(%s). Recommended upgrade order: CONTROLLER, then BROKER, then SERVER/MINION.",
          brokerMin, controllerMin));
    }
    // Server and minion anchor on broker when present, else controller.
    String parentVersion = brokerMin != null ? brokerMin : controllerMin;
    String parentName = brokerMin != null ? BROKER : CONTROLLER;
    if (serverMin != null && parentVersion != null
        && VersionParsingUtils.compare(serverMin, parentVersion) > 0) {
      warnings.add(String.format(
          "Rollout order violation: SERVER min version (%s) is newer than %s min version (%s). "
              + "Recommended upgrade order: CONTROLLER, then BROKER, then SERVER/MINION.",
          serverMin, parentName, parentVersion));
    }
    if (minionMin != null && parentVersion != null
        && VersionParsingUtils.compare(minionMin, parentVersion) > 0) {
      warnings.add(String.format(
          "Rollout order violation: MINION min version (%s) is newer than %s min version (%s). "
              + "Recommended upgrade order: CONTROLLER, then BROKER, then SERVER/MINION.",
          minionMin, parentName, parentVersion));
    }

    boolean ok = warnings.isEmpty();
    String message = ok
        ? "All component versions satisfy the recommended rollout order."
        : String.format("%d rollout-order warning(s) detected. These are advisory only.",
            warnings.size());
    return new CompatibilityCheckResult(CHECK_TYPE_ROLLOUT_ORDER, ok, message, warnings, summary);
  }

  @Override
  public void invalidateCache() {
    _cacheRef.set(null);
    LOGGER.debug("Version compatibility cache invalidated");
  }

  /**
   * Cluster-config change callback. Updates the cache TTL if the relevant key was modified.
   */
  @Override
  public void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    if (!changedConfigs.contains(
        ControllerConf.ControllerPeriodicTasksConf.VERSION_HEALTH_CHECK_CACHE_TTL_SECONDS)) {
      return;
    }
    String raw = clusterConfigs.get(
        ControllerConf.ControllerPeriodicTasksConf.VERSION_HEALTH_CHECK_CACHE_TTL_SECONDS);
    long newTtlSec;
    try {
      newTtlSec = Long.parseLong(raw);
    } catch (NumberFormatException e) {
      LOGGER.warn("Ignoring invalid cluster config value for {}: {}",
          ControllerConf.ControllerPeriodicTasksConf.VERSION_HEALTH_CHECK_CACHE_TTL_SECONDS, raw);
      return;
    }
    if (newTtlSec <= 0) {
      LOGGER.warn("Ignoring non-positive cluster config value for {}: {}",
          ControllerConf.ControllerPeriodicTasksConf.VERSION_HEALTH_CHECK_CACHE_TTL_SECONDS, newTtlSec);
      return;
    }
    long newTtlMs = clampTtlMs(newTtlSec * 1000L);
    _cacheTtlMs = newTtlMs;
    LOGGER.info("Version compatibility cache TTL updated to {}ms via cluster config", newTtlMs);
    invalidateCache();
  }

  // -------------------------------------------------------------------------
  // Internal helpers
  // -------------------------------------------------------------------------

  @Nullable
  private static String minVersion(ClusterVersionSummary summary, String componentType) {
    ComponentVersionSummary comp = summary.getComponentSummaries().get(componentType);
    if (comp == null || comp.getLiveInstanceCount() == 0) {
      return null;
    }
    return comp.getMinVersion();
  }

  private CachedSnapshot getOrRefreshCache() {
    CachedSnapshot current = _cacheRef.get();
    if (current != null && !current.isExpired(_cacheTtlMs)) {
      return current;
    }
    return refresh();
  }

  /**
   * Queries Helix and rebuilds the cached snapshot.  Synchronized so that multiple threads
   * do not all attempt a Helix read simultaneously on cache miss. On failure, if a previous
   * good snapshot exists it is retained (stale-but-good), and only the {@link CachedSnapshot}'s
   * {@code _fetchedAtMs} (the TTL-back-off timestamp) is updated so we don't hammer Helix on
   * every request during a sustained outage. The underlying data's {@code snapshotTimeMs}
   * remains at the original capture time so callers can still observe staleness. An unavailable
   * snapshot is published only when there is no good snapshot to serve.
   */
  private synchronized CachedSnapshot refresh() {
    // Double-check: another thread may have refreshed while we waited.
    CachedSnapshot current = _cacheRef.get();
    if (current != null && !current.isExpired(_cacheTtlMs)) {
      return current;
    }

    long now = System.currentTimeMillis();
    try {
      List<InstanceConfig> allConfigs = fetchAllInstanceConfigs();
      Set<String> liveNames = new HashSet<>(fetchLiveInstanceNames());
      String clusterName = fetchClusterName();

      ClusterVersionSummary summary = buildSummary(clusterName, allConfigs, liveNames, now);
      CachedSnapshot snapshot = new CachedSnapshot(summary, buildInstanceIndex(summary), now);
      _cacheRef.set(snapshot);
      return snapshot;
    } catch (Exception e) {
      LOGGER.error("Failed to refresh version compatibility cache from Helix", e);
      if (current != null && current._summary.isDataAvailable()) {
        // Stale-but-good: keep the previously cached summary; only advance the TTL back-off
        // timestamp so we don't hammer Helix on every request during a sustained outage.
        LOGGER.warn("Serving stale version snapshot from {}ms ago due to Helix read failure",
            now - current._summary.getSnapshotTimeMs());
        CachedSnapshot stale = new CachedSnapshot(current._summary, current._instanceIndex, now);
        _cacheRef.set(stale);
        return stale;
      }
      // No good snapshot to fall back to — publish an unavailable snapshot with a fresh
      // timestamp so subsequent calls respect the TTL back-off.
      String clusterName = safeGetClusterName();
      ClusterVersionSummary empty = new ClusterVersionSummary(
          clusterName, now, false, Collections.emptyMap());
      CachedSnapshot snapshot = new CachedSnapshot(empty, Collections.emptyMap(), now);
      _cacheRef.set(snapshot);
      return snapshot;
    }
  }

  // Test seams: subclasses may override to supply fake data.
  protected List<InstanceConfig> fetchAllInstanceConfigs() {
    return HelixHelper.getInstanceConfigs(_helixResourceManager.getHelixZkManager());
  }

  protected List<String> fetchLiveInstanceNames() {
    return _helixResourceManager.getAllLiveInstances();
  }

  protected String fetchClusterName() {
    return _helixResourceManager.getHelixClusterName();
  }

  private String safeGetClusterName() {
    try {
      return fetchClusterName();
    } catch (Exception e) {
      return UNKNOWN_CLUSTER;
    }
  }

  private ClusterVersionSummary buildSummary(String clusterName, List<InstanceConfig> allConfigs,
      Set<String> liveNames, long snapshotTimeMs) {
    // Group InstanceConfigs by component type. Instances with unrecognized prefixes are skipped
    // — the version framework should not silently bucket them into SERVER.
    Map<String, List<InstanceConfig>> liveByType = new LinkedHashMap<>();
    Map<String, List<InstanceConfig>> offlineByType = new LinkedHashMap<>();
    for (String type : COMPONENT_TYPES) {
      liveByType.put(type, new ArrayList<>());
      offlineByType.put(type, new ArrayList<>());
    }

    int unrecognizedCount = 0;
    for (InstanceConfig config : allConfigs) {
      String type = InstanceVersionInfo.componentTypeOf(config.getInstanceName());
      if (InstanceVersionInfo.UNKNOWN_TYPE.equals(type)) {
        unrecognizedCount++;
        continue;
      }
      if (liveNames.contains(config.getInstanceName())) {
        liveByType.get(type).add(config);
      } else {
        offlineByType.get(type).add(config);
      }
    }
    if (unrecognizedCount > 0) {
      LOGGER.debug("Skipped {} instance(s) with unrecognized prefix during version summary build",
          unrecognizedCount);
    }

    Map<String, ComponentVersionSummary> componentSummaries = new LinkedHashMap<>();
    for (String type : COMPONENT_TYPES) {
      componentSummaries.put(type, buildComponentSummary(type, liveByType.get(type), offlineByType.get(type)));
    }
    return new ClusterVersionSummary(clusterName, snapshotTimeMs, true, componentSummaries);
  }

  private ComponentVersionSummary buildComponentSummary(String type, List<InstanceConfig> liveConfigs,
      List<InstanceConfig> offlineConfigs) {
    List<InstanceVersionInfo> allInstances = new ArrayList<>(liveConfigs.size() + offlineConfigs.size());
    for (InstanceConfig cfg : liveConfigs) {
      allInstances.add(toVersionInfo(cfg, true));
    }
    for (InstanceConfig cfg : offlineConfigs) {
      allInstances.add(toVersionInfo(cfg, false));
    }

    Map<String, Integer> versionCounts = new LinkedHashMap<>();
    ParsedVersion minParsed = null;
    ParsedVersion maxParsed = null;
    String minRaw = null;
    String maxRaw = null;
    int unknownCount = 0;

    for (InstanceConfig cfg : liveConfigs) {
      String raw = getRawVersion(cfg);
      ParsedVersion pv = VersionParsingUtils.parse(raw);
      if (pv == null) {
        unknownCount++;
        versionCounts.merge(PinotVersion.UNKNOWN, 1, Integer::sum);
      } else {
        versionCounts.merge(raw, 1, Integer::sum);
        if (minParsed == null || pv.compareTo(minParsed) < 0) {
          minParsed = pv;
          minRaw = raw;
        }
        if (maxParsed == null || pv.compareTo(maxParsed) > 0) {
          maxParsed = pv;
          maxRaw = raw;
        }
      }
    }

    return new ComponentVersionSummary(type, liveConfigs.size(), offlineConfigs.size(), versionCounts,
        minRaw, maxRaw, unknownCount, allInstances);
  }

  private static Map<String, InstanceVersionInfo> buildInstanceIndex(ClusterVersionSummary summary) {
    Map<String, InstanceVersionInfo> index = new HashMap<>();
    for (ComponentVersionSummary comp : summary.getComponentSummaries().values()) {
      for (InstanceVersionInfo info : comp.getInstances()) {
        index.put(info.getInstanceName(), info);
      }
    }
    return index;
  }

  private InstanceVersionInfo toVersionInfo(InstanceConfig config, boolean isLive) {
    String raw = getRawVersion(config);
    ParsedVersion pv = VersionParsingUtils.parse(raw);
    String type = InstanceVersionInfo.componentTypeOf(config.getInstanceName());
    return new InstanceVersionInfo(config.getInstanceName(), type, raw, isLive, pv);
  }

  private static String getRawVersion(InstanceConfig config) {
    String v = config.getRecord().getSimpleField(CommonConstants.Helix.Instance.PINOT_VERSION_KEY);
    return (v == null || v.isBlank()) ? PinotVersion.UNKNOWN : v;
  }

  // -------------------------------------------------------------------------
  // Cache container
  // -------------------------------------------------------------------------

  private static final class CachedSnapshot {
    final ClusterVersionSummary _summary;
    final Map<String, InstanceVersionInfo> _instanceIndex;
    final long _fetchedAtMs;

    CachedSnapshot(ClusterVersionSummary summary, Map<String, InstanceVersionInfo> instanceIndex,
        long fetchedAtMs) {
      _summary = summary;
      _instanceIndex = instanceIndex;
      _fetchedAtMs = fetchedAtMs;
    }

    boolean isExpired(long ttlMs) {
      return System.currentTimeMillis() - _fetchedAtMs > ttlMs;
    }
  }
}
