/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.server.realtime;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.pinot.core.query.utils.Pair;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
   * Locate the controller leader so that we can send LLC segment completion requests to it.
   * Checks the {@link ControllerLeaderLocator::_cachedControllerLeaderInvalid} flag and fetches the leader from helix if cached value is invalid
   *
   * @return The host:port string of the current controller leader.
   */
  public synchronized Pair<String, Integer> getControllerLeader() {
    if (!_cachedControllerLeaderInvalid) {
      return _controllerLeaderHostPort;
    }

    BaseDataAccessor<ZNRecord> dataAccessor = _helixManager.getHelixDataAccessor().getBaseDataAccessor();
    Stat stat = new Stat();
    try {
      ZNRecord znRecord =
          dataAccessor.get("/" + _clusterName + "/CONTROLLER/LEADER", stat, AccessOption.THROW_EXCEPTION_IFNOTEXIST);
      String leader = znRecord.getId();
      int index = leader.lastIndexOf('_');
      String leaderHost = leader.substring(0, index);
      int leaderPort = Integer.valueOf(leader.substring(index + 1));
      _controllerLeaderHostPort = new Pair<>(leaderHost, leaderPort);
      _cachedControllerLeaderInvalid = false;
      return _controllerLeaderHostPort;
    } catch (Exception e) {
      LOGGER.warn("Could not locate controller leader, exception", e);
      _cachedControllerLeaderInvalid = true;
      return null;
    }
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
