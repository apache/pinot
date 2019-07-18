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
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.pinot.common.utils.helix.LeadControllerUtils;
import org.apache.pinot.core.query.utils.Pair;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.common.utils.CommonConstants.Helix.LEAD_CONTROLLER_RESOURCE_NAME;

// Helix keeps the old controller around for 30s before electing a new one, so we will keep getting
// the old controller as leader, and it will keep returning NOT_LEADER.

// Singleton class.
public class ControllerLeaderLocator {
  private static ControllerLeaderLocator _instance = null;
  public static final Logger LOGGER = LoggerFactory.getLogger(ControllerLeaderLocator.class);

  private final HelixManager _helixManager;
  private final String _clusterName;

  // Co-ordinates of the last known controller leader.
  private Pair<String, Integer> _controllerLeaderHostPort = null;

  // Indicates whether cached controller leader value is invalid
  private volatile boolean _cachedControllerLeaderInvalid = true;
  // Time in millis when cache invalidate was last set
  private volatile long _lastCacheInvalidateMillis = 0;
  // Minimum millis which must elapse between consecutive invalidation of cache
  private static final long MILLIS_BETWEEN_INVALIDATE = 30_000;

  ControllerLeaderLocator(HelixManager helixManager) {
    _helixManager = helixManager;
    _clusterName = helixManager.getClusterName();
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
   * Checks the {@link ControllerLeaderLocator::_cachedControllerLeaderInvalid} flag and fetches the leader from helix if cached value is invalid
   * @param rawTableName table name without type.
   * @return The host-port pair of the current controller leader.
   */
  public synchronized Pair<String, Integer> getControllerLeader(String rawTableName) {
    if (!_cachedControllerLeaderInvalid) {
      return _controllerLeaderHostPort;
    }

    String leaderForTable = getLeaderForTable(rawTableName);
    if (leaderForTable == null) {
      LOGGER.warn("Failed to find a leader for Table: {}", rawTableName);
      _cachedControllerLeaderInvalid = true;
      return null;
    } else {
      _controllerLeaderHostPort = generateControllerLeaderHostPortPair(leaderForTable);
      _cachedControllerLeaderInvalid = false;
      LOGGER.info("Setting controller leader to be {}:{}", _controllerLeaderHostPort.getFirst(),
          _controllerLeaderHostPort.getSecond());
      return _controllerLeaderHostPort;
    }
  }

  /**
   * If partition leader exists, use this as the leader for realtime segment completion.
   * Otherwise, try to use Helix leader.
   * @param rawTableName table name without type
   * @return the controller leader id with hostname and port for this table, e.g. localhost_9000
   */
  private String getLeaderForTable(String rawTableName) {
    String leaderForTable;
    ExternalView leadControllerResourceExternalView =
        _helixManager.getClusterManagmentTool().getResourceExternalView(_clusterName, LEAD_CONTROLLER_RESOURCE_NAME);
    String leadControllerInstance =
        LeadControllerUtils.getLeadControllerInstanceForTable(leadControllerResourceExternalView, rawTableName);
    if (leadControllerInstance != null) {
      // Converts participant id (with Prefix "Controller_") to controller id and assigns it as the leader,
      // since realtime segment completion protocol doesn't need the prefix in controller instance id.
      leaderForTable = LeadControllerUtils.extractLeadControllerHostNameAndPort(leadControllerInstance);
    } else {
      // Gets Helix leader to be the leader to this table, otherwise returns null.
      leaderForTable = getHelixClusterLeader();
    }
    return leaderForTable;
  }

  /**
   * Gets Helix leader in the cluster. Null if there is no leader.
   * @return instance id of Helix cluster leader, e.g. localhost_9000.
   */
  private String getHelixClusterLeader() {
    BaseDataAccessor<ZNRecord> dataAccessor = _helixManager.getHelixDataAccessor().getBaseDataAccessor();
    Stat stat = new Stat();
    try {
      ZNRecord znRecord =
          dataAccessor.get("/" + _clusterName + "/CONTROLLER/LEADER", stat, AccessOption.THROW_EXCEPTION_IFNOTEXIST);
      String helixLeader = znRecord.getId();
      LOGGER.info("Getting Helix leader: {} as per znode version {}, mtime {}", helixLeader, stat.getVersion(),
          stat.getMtime());
      return helixLeader;
    } catch (Exception e) {
      LOGGER.warn("Could not locate Helix leader", e);
      return null;
    }
  }

  /**
   * Generates a pair of hostname and port given a controller leader id.
   * @param controllerLeaderId controller leader id, e.g. localhost_9000
   */
  private Pair<String, Integer> generateControllerLeaderHostPortPair(String controllerLeaderId) {
    int index = controllerLeaderId.lastIndexOf('_');
    String leaderHost = controllerLeaderId.substring(0, index);
    int leaderPort = Integer.valueOf(controllerLeaderId.substring(index + 1));
    return new Pair<>(leaderHost, leaderPort);
  }

  /**
   * Invalidates the cached controller leader value by setting the {@link ControllerLeaderLocator::_cacheControllerLeadeInvalid} flag.
   * This flag is always checked first by {@link ControllerLeaderLocator::getControllerLeader()} method before returning the leader. If set, leader is fetched from helix, else cached leader value is returned.
   *
   * Invalidates are not allowed more frequently than {@link ControllerLeaderLocator::MILLIS_BETWEEN_INVALIDATE} millis.
   * The cache is invalidated whenever server gets NOT_LEADER or NOT_SENT response. A NOT_LEADER response definitely needs a cache refresh. However, a NOT_SENT response could also happen for reasons other than controller not being leader.
   * Thus the frequency limiting is done to guard against frequent cache refreshes, in cases where we might be getting too many NOT_SENT responses due to some other errors.
   */
  public synchronized void invalidateCachedControllerLeader() {
    long now = System.currentTimeMillis();
    long millisSinceLastInvalidate = now - _lastCacheInvalidateMillis;
    if (millisSinceLastInvalidate < MILLIS_BETWEEN_INVALIDATE) {
      LOGGER.info(
          "Millis since last controller cache value invalidate {} is less than allowed frequency {}. Skipping invalidate.",
          millisSinceLastInvalidate, MILLIS_BETWEEN_INVALIDATE);
    } else {
      LOGGER.info("Invalidating cached controller leader value");
      _cachedControllerLeaderInvalid = true;
      _lastCacheInvalidateMillis = now;
    }
  }

  @VisibleForTesting
  protected boolean isCachedControllerLeaderInvalid() {
    return _cachedControllerLeaderInvalid;
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
  protected void setLastCacheInvalidateMillis(long lastCacheInvalidateMillis) {
    _lastCacheInvalidateMillis = lastCacheInvalidateMillis;
  }
}
