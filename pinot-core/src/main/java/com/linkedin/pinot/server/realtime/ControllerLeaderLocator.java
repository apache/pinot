/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


// TODO
// Maybe we should have a separate thread that gets the controller leader address, not sure.
// We should have algorithms here that do not get the address multiple times within a short period.
// Helix keeps the old controller around for 30s before electing a new one, so we will keep getting
// the old controller as leader, and it will keep returning NOT_LEADER.

// Singleton class.
public class ControllerLeaderLocator {
  private static ControllerLeaderLocator _instance = null;
  public static final Logger LOGGER = LoggerFactory.getLogger(ControllerLeaderLocator.class);

  private final HelixManager _helixManager;
  private final String _clusterName;
  private String controllerLeaderHostPort = null;

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
   *
   * @param forceRefresh : If set to true, then makes a zk call to re-fetch the leadership information.
   * @return The host:port string of the current controller leader.
   */
  public String getControllerLeader(boolean forceRefresh) {
    if (controllerLeaderHostPort == null || forceRefresh) {
      BaseDataAccessor<ZNRecord> dataAccessor = _helixManager.getHelixDataAccessor().getBaseDataAccessor();
      Stat stat = new Stat();
      ZNRecord znRecord = dataAccessor.get("/" + _clusterName + "/CONTROLLER/LEADER", stat, AccessOption.THROW_EXCEPTION_IFNOTEXIST);
      String leader = znRecord.getId();
      int index = leader.lastIndexOf('_');
      controllerLeaderHostPort = leader.substring(0, index) + ":" + leader.substring(index+1);
    }
    return controllerLeaderHostPort;
  }
}
