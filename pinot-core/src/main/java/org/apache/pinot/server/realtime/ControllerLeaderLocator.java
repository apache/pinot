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
import java.util.Map;
import org.apache.helix.HelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.helix.LeadControllerUtils;
import org.apache.pinot.pql.parsers.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Helix keeps the old controller around for 30s before electing a new one, so we will keep getting
// the old controller as leader, and it will keep returning NOT_LEADER.

// Singleton class.
public class ControllerLeaderLocator {
  private static ControllerLeaderLocator _instance = null;
  public static final Logger LOGGER = LoggerFactory.getLogger(ControllerLeaderLocator.class);

  // Minimum millis which must elapse between consecutive invalidation of cache
  private static final long MILLIS_BETWEEN_INVALIDATE = 30_000;

  private final HelixManager _helixManager;

  // Indicates whether cached controller leader value is valid. If so, caches the host-port pair as the last known controller leader.
  // In general the number of tables having consuming segments in a host is smaller than the number of partitions in lead controller resource.
  // Thus, we use raw table name as the key instead of partition name of lead controller resource in the cache.
  private final Map<String, Pair<String, Integer>> _cachedValidControllerLeaderMap;

  // Time in millis when cache invalidate was last set
  private volatile long _lastCacheInvalidateMillis = 0;

  ControllerLeaderLocator(HelixManager helixManager) {
    _helixManager = helixManager;
    _cachedValidControllerLeaderMap = new ConcurrentHashMap<>();
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
   * Checks the {@link ControllerLeaderLocator::_cachedValidControllerLeaderMap} and fetches the leader from helix if cached value is invalid or missing.
   * @param rawTableName table name without type.
   * @return The host-port pair of the current controller leader.
   */
  public synchronized Pair<String, Integer> getControllerLeader(String rawTableName) {
    Pair<String, Integer> leaderPairForTable = _cachedValidControllerLeaderMap.get(rawTableName);
    if (leaderPairForTable != null) {
      return leaderPairForTable;
    }

    // No controller leader cached, fetches a fresh one for given table.
    leaderPairForTable = getLeaderForTable(rawTableName);
    if (leaderPairForTable == null) {
      LOGGER.warn("Failed to find a leader for Table: {}", rawTableName);
      _cachedValidControllerLeaderMap.remove(rawTableName);
      return null;
    } else {
      _cachedValidControllerLeaderMap.put(rawTableName, leaderPairForTable);
      LOGGER.info("Setting controller leader to be {}:{} for Table: {}", leaderPairForTable.getFirst(),
          leaderPairForTable.getSecond(), rawTableName);
      return leaderPairForTable;
    }
  }

  /**
   * Firstly checks whether lead controller resource has been enabled or not.
   * If yes, use this as the leader for realtime segment completion once partition leader exists.
   * Otherwise, try to use Helix leader.
   * @param rawTableName table name without type.
   * @return the controller leader id with hostname and port for this table, e.g. localhost_9000
   */
  private Pair<String, Integer> getLeaderForTable(String rawTableName) {
    // Checks whether lead controller resource has been enabled or not.
    boolean leadControllerResourceEnabled;
    try {
      leadControllerResourceEnabled = LeadControllerUtils.isLeadControllerResourceEnabled(_helixManager);
    } catch (Exception e) {
      LOGGER.error("Exception when checking whether lead controller resource is enabled or not.", e);
      return null;
    }

    if (leadControllerResourceEnabled) {
      // Gets leader from lead controller resource.
      return getLeaderFromLeadControllerResource(rawTableName);
    } else {
      // Gets Helix leader to be the leader to this table, otherwise returns null.
      return getHelixClusterLeader();
    }
  }

  /**
   * Gets leader from lead controller resource. Null if there is no leader.
   * @param rawTableName raw table name.
   * @return pair of instance hostname and port of Helix cluster leader, e.g. {localhost, 9000}.
   */
  private Pair<String, Integer> getLeaderFromLeadControllerResource(String rawTableName) {
    Pair<String, Integer> leaderHostAndPortPair = getLeadControllerInstanceIdForTable(rawTableName);
    if (leaderHostAndPortPair != null) {
      return leaderHostAndPortPair;
    } else {
      LOGGER.warn("Could not locate leader for table: {}", rawTableName);
      return null;
    }
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
   * Gets lead controller participant id for table from lead controller resource.
   * If the resource is disabled or no controller registered as participant, there is no instance in "MASTER" state.
   * @param rawTableName table name without type
   * @return Helix controller instance id for partition leader, e.g. localhost_9000. Null if not found or resource is disabled.
   */
  private Pair<String, Integer> getLeadControllerInstanceIdForTable(String rawTableName) {
    try {
      ExternalView leadControllerResourceExternalView = _helixManager.getClusterManagmentTool()
          .getResourceExternalView(_helixManager.getClusterName(), CommonConstants.Helix.LEAD_CONTROLLER_RESOURCE_NAME);
      if (leadControllerResourceExternalView == null) {
        LOGGER.warn("External view of lead controller resource is null!");
        return null;
      }
      int partitionId = LeadControllerUtils.getPartitionIdForTable(rawTableName);
      String partitionName = LeadControllerUtils.generatePartitionName(partitionId);
      Map<String, String> partitionStateMap = leadControllerResourceExternalView.getStateMap(partitionName);

      // Get master host from partition map. Return null if no master found.
      for (Map.Entry<String, String> entry : partitionStateMap.entrySet()) {
        if (MasterSlaveSMD.States.MASTER.name().equals(entry.getValue())) {
          // Found the controller in master state.
          // Converts participant id (with Prefix "Controller_") to controller id and assigns it as the leader,
          // since realtime segment completion protocol doesn't need the prefix in controller instance id.
          String participantInstanceId = entry.getKey();
          String controllerInstanceId = participantInstanceId.substring(participantInstanceId.indexOf('_') + 1);
          return convertToHostAndPortPair(controllerInstanceId);
        }
      }
      LOGGER
          .warn("There is no controller in MASTER state for partition: {} in lead controller resource", partitionName);
    } catch (Exception e) {
      LOGGER.warn("Caught exception when getting lead controller instance Id for Table: {}", rawTableName, e);
    }
    return null;
  }

  /**
   * Converts instance id to a pair of hostname and port.
   * @param instanceId instance id without any prefix, e.g. localhost_9000
   * */
  private Pair<String, Integer> convertToHostAndPortPair(String instanceId) {
    // TODO: improve the exception handling.
    if (instanceId == null) {
      return null;
    }
    int index = instanceId.lastIndexOf('_');
    String leaderHost = instanceId.substring(0, index);
    int leaderPort = Integer.valueOf(instanceId.substring(index + 1));
    return new Pair<>(leaderHost, leaderPort);
  }

  /**
   * Invalidates the cached controller leader value by removing the existing pair from {@link ControllerLeaderLocator::_cachedValidControllerLeaderMap}.
   * This flag is always checked first by {@link ControllerLeaderLocator::getControllerLeader()} method before returning the leader. If set, leader is fetched from helix, else cached leader value is returned.
   *
   * Invalidates are not allowed more frequently than {@link ControllerLeaderLocator::MILLIS_BETWEEN_INVALIDATE} millis.
   * The cache is invalidated whenever server gets NOT_LEADER or NOT_SENT response. A NOT_LEADER response definitely needs a cache refresh. However, a NOT_SENT response could also happen for reasons other than controller not being leader.
   * Thus the frequency limiting is done to guard against frequent cache refreshes, in cases where we might be getting too many NOT_SENT responses due to some other errors.
   * @param rawTableName raw table name.
   */
  public synchronized void invalidateCachedControllerLeader(String rawTableName) {
    long now = System.currentTimeMillis();
    long millisSinceLastInvalidate = now - _lastCacheInvalidateMillis;
    if (millisSinceLastInvalidate < MILLIS_BETWEEN_INVALIDATE) {
      LOGGER.info(
          "Millis since last controller cache value invalidate {} is less than allowed frequency {}. Skipping invalidate for Table: {}.",
          millisSinceLastInvalidate, MILLIS_BETWEEN_INVALIDATE, rawTableName);
    } else {
      LOGGER.info("Invalidating cached controller leader value for Table: {}", rawTableName);
      _cachedValidControllerLeaderMap.remove(rawTableName);
      _lastCacheInvalidateMillis = now;
    }
  }

  @VisibleForTesting
  protected boolean isCachedControllerLeaderInvalid(String rawTableName) {
    return !_cachedValidControllerLeaderMap.containsKey(rawTableName);
  }

  @VisibleForTesting
  protected long getLastCacheInvalidateMillis() {
    return _lastCacheInvalidateMillis;
  }

  @VisibleForTesting
  protected long getMillisBetweenInvalidate() {
    return MILLIS_BETWEEN_INVALIDATE;
  }

  @VisibleForTesting
  public void setLastCacheInvalidateMillis(long lastCacheInvalidateMillis) {
    _lastCacheInvalidateMillis = lastCacheInvalidateMillis;
  }
}
