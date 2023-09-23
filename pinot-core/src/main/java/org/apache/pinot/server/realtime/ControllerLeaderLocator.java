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
package org.apache.pinot.server.realtime;

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.pinot.common.utils.helix.LeadControllerUtils;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Helix keeps the old controller around for 30s before electing a new one, so we will keep getting
// the old controller as leader, and it will keep returning NOT_LEADER.

// Singleton class.
public class ControllerLeaderLocator {
  private static ControllerLeaderLocator _instance = null;
  public static final Logger LOGGER = LoggerFactory.getLogger(ControllerLeaderLocator.class);

  // Minimum millis which must elapse between consecutive invalidation of cache
  private static final long MIN_INVALIDATE_INTERVAL_MS = 30_000L;

  private final HelixManager _helixManager;

  // Co-ordinates of the last known controller leader for each of the lead-controller every partitions,
  // with partition number being the key and controller hostname and port pair being the value.  If the lead
  // controller resource is disabled in the configuration then this map contains helix cluster leader co-ordinates
  // for all partitions of leadControllerResource.
  private final Map<Integer, Pair<String, Integer>> _cachedControllerLeaderMap;

  // Indicates whether cached controller leader(s) value is(are) valid.
  private volatile boolean _cachedControllerLeaderValid = false;
  // Time in millis when cache invalidate was last set
  private volatile long _lastCacheInvalidationTimeMs = 0L;

  ControllerLeaderLocator(HelixManager helixManager) {
    _helixManager = helixManager;
    _cachedControllerLeaderMap = new HashMap<>();
  }

  /**
   * To be called once when the server starts
   * @param helixManager should already be started
   */
  public static void create(HelixManager helixManager) {
    if (_instance != null) {
      // We create multiple server instances in the hybrid cluster integration tests, so allow the call to create an
      // instance even if there is already one.
      LOGGER.warn("Already created");
      return;
    }
    _instance = new ControllerLeaderLocator(helixManager);
  }

  public static ControllerLeaderLocator getInstance() {
    if (_instance == null) {
      throw new RuntimeException("Not yet created");
    }
    return _instance;
  }

  /**
   * Locates the controller leader so that we can send LLC segment completion requests to it.
   * Checks the {@link ControllerLeaderLocator::_cachedControllerLeaderValid} flag and fetches the leaders to
   * {@link ControllerLeaderLocator::_cachedControllerLeaderMap} from helix if cached value is invalid
   * @param rawTableName table name without type.
   * @return The host-port pair of the current controller leader.
   */
  public synchronized Pair<String, Integer> getControllerLeader(String rawTableName) {
    int partitionId = LeadControllerUtils.getPartitionIdForTable(rawTableName);
    if (_cachedControllerLeaderValid) {
      return _cachedControllerLeaderMap.get(partitionId);
    }

    // No controller leader cached, fetches a fresh copy of external view and then gets the leader for the given table.
    refreshControllerLeaderMap();
    return _cachedControllerLeaderValid ? _cachedControllerLeaderMap.get(partitionId) : null;
  }

  /**
   * Checks whether lead controller resource has been enabled or not.
   * If yes, updates lead controller pairs from the external view of lead controller resource.
   * Otherwise, updates lead controller pairs from Helix cluster leader.
   * Note: Exception may happen due to Helix/ZK disconnect. If so, we should NOT regress the behavior back to false.
   * Thus, simply exiting the method should be enough. Retry will be done in the next request.
   */
  private void refreshControllerLeaderMap() {
    try {
      // Checks whether lead controller resource has been enabled or not.
      if (LeadControllerUtils.isLeadControllerResourceEnabled(_helixManager)) {
        refreshControllerLeaderMapFromLeadControllerResource();
      } else {
        refreshControllerLeaderMapFromHelixClusterLeader();
      }
    } catch (Exception e) {
      LOGGER.error("Exception when checking whether lead controller resource is enable or not.", e);
    }
  }

  /**
   * Updates lead controller pairs from the external view of lead controller resource.
   */
  private void refreshControllerLeaderMapFromLeadControllerResource() {
    try {
      HelixDataAccessor dataAccessor = _helixManager.getHelixDataAccessor();
      PropertyKey.Builder keyBuilder = dataAccessor.keyBuilder();
      ExternalView leadControllerResourceExternalView =
          dataAccessor.getProperty(keyBuilder.externalView(Helix.LEAD_CONTROLLER_RESOURCE_NAME));
      if (leadControllerResourceExternalView == null) {
        LOGGER.error("External view of {} is null.", Helix.LEAD_CONTROLLER_RESOURCE_NAME);
        return;
      }
      Map<String, Map<String, String>> partitionStateMap =
          leadControllerResourceExternalView.getRecord().getMapFields();
      if (partitionStateMap.size() != Helix.NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE) {
        LOGGER.error("The partition size of {} is not {}. Actual size: {}", Helix.LEAD_CONTROLLER_RESOURCE_NAME,
            Helix.NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE, partitionStateMap.size());
        return;
      }
      for (Map.Entry<String, Map<String, String>> entry : partitionStateMap.entrySet()) {
        String partitionName = entry.getKey();
        Map<String, String> instanceStateMap = entry.getValue();
        String masterInstanceId = null;
        for (Map.Entry<String, String> instanceStateEntry : instanceStateMap.entrySet()) {
          if (instanceStateEntry.getValue().equals(MasterSlaveSMD.States.MASTER.name())) {
            masterInstanceId = instanceStateEntry.getKey();
            break;
          }
        }
        if (masterInstanceId == null) {
          // It's ok to log a warning since we can be in this state for some small time during the migration.
          // Otherwise, we are attempted to mark this as an error.
          LOGGER.warn("There is no controller in MASTER state for partition: {} in {}", partitionName,
              Helix.LEAD_CONTROLLER_RESOURCE_NAME);
          return;
        }
        InstanceConfig instanceConfig = dataAccessor.getProperty(keyBuilder.instanceConfig(masterInstanceId));
        if (instanceConfig == null) {
          LOGGER.error("Failed to find instance config for MASTER controller: {}", masterInstanceId);
          return;
        }
        int partitionId = LeadControllerUtils.extractPartitionId(partitionName);
        Pair<String, Integer> hostnamePortPair =
            Pair.of(instanceConfig.getHostName(), Integer.parseInt(instanceConfig.getPort()));
        _cachedControllerLeaderMap.put(partitionId, hostnamePortPair);
      }
      _cachedControllerLeaderValid = true;
      LOGGER.info("Refreshed controller leader map successfully.");
    } catch (Exception e) {
      LOGGER.error("Caught exception when getting lead controller instance Id from external view of {}",
          Helix.LEAD_CONTROLLER_RESOURCE_NAME, e);
    }
  }

  /**
   * Updates lead controller pairs from Helix cluster leader.
   */
  private void refreshControllerLeaderMapFromHelixClusterLeader() {
    Pair<String, Integer> helixClusterLeader = getHelixClusterLeader();
    if (helixClusterLeader == null) {
      LOGGER.error("Failed to refresh the controller leader map.");
      return;
    }
    for (int i = 0; i < Helix.NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE; i++) {
      _cachedControllerLeaderMap.put(i, helixClusterLeader);
    }
    _cachedControllerLeaderValid = true;
    LOGGER.info("Refreshed controller leader map successfully.");
  }

  /**
   * Gets Helix leader in the cluster. Null if there is no leader.
   * @return instance id of Helix cluster leader, e.g. localhost_9000.
   */
  private Pair<String, Integer> getHelixClusterLeader() {
    String helixLeader = LeadControllerUtils.getHelixClusterLeader(_helixManager);
    return convertToHostAndPortPair(helixLeader);
  }

  /**
   * Converts instance id to a pair of hostname and port.
   * @param instanceId instance id without any prefix, e.g. localhost_9000
   */
  private Pair<String, Integer> convertToHostAndPortPair(String instanceId) {
    // TODO: improve the exception handling.
    if (instanceId == null) {
      return null;
    }
    int index = instanceId.lastIndexOf('_');
    String leaderHost = instanceId.substring(0, index);
    int leaderPort = Integer.parseInt(instanceId.substring(index + 1));
    return Pair.of(leaderHost, leaderPort);
  }

  /**
   * Invalidates the cached controller leader value by setting the {@link ControllerLeaderLocator
   * ::_cacheControllerLeadeInvalid} flag.
   * This flag is always checked first by {@link ControllerLeaderLocator::getControllerLeader()} method before
   * returning the leader. If set, leader is fetched from helix, else cached leader value is returned.
   *
   * Invalidates are not allowed more frequently than {@link ControllerLeaderLocator::MIN_INVALIDATE_INTERVAL_MS}
   * millis.
   * The cache is invalidated whenever server gets NOT_LEADER or NOT_SENT response. A NOT_LEADER response definitely
   * needs a cache refresh. However, a NOT_SENT response could also happen for reasons other than controller not
   * being leader.
   * Thus the frequency limiting is done to guard against frequent cache refreshes, in cases where we might be
   * getting too many NOT_SENT responses due to some other errors.
   */
  public synchronized void invalidateCachedControllerLeader() {
    long now = getCurrentTimeMs();
    long millisSinceLastInvalidate = now - _lastCacheInvalidationTimeMs;
    if (millisSinceLastInvalidate < MIN_INVALIDATE_INTERVAL_MS) {
      LOGGER.info("Millis since last controller cache value invalidate {} is less than allowed frequency {}. Skipping "
          + "invalidate.", millisSinceLastInvalidate, MIN_INVALIDATE_INTERVAL_MS);
    } else {
      LOGGER.info("Invalidating cached controller leader value");
      _cachedControllerLeaderValid = false;
      _lastCacheInvalidationTimeMs = now;
    }
  }

  @VisibleForTesting
  protected long getCurrentTimeMs() {
    return System.currentTimeMillis();
  }

  @VisibleForTesting
  protected boolean isCachedControllerLeaderValid() {
    return _cachedControllerLeaderValid;
  }

  @VisibleForTesting
  protected long getLastCacheInvalidationTimeMs() {
    return _lastCacheInvalidationTimeMs;
  }

  @VisibleForTesting
  protected long getMinInvalidateIntervalMs() {
    return MIN_INVALIDATE_INTERVAL_MS;
  }

  @VisibleForTesting
  public static void clear() {
    if (_instance != null) {
      _instance._cachedControllerLeaderMap.clear();
    }
    _instance = null;
  }
}
