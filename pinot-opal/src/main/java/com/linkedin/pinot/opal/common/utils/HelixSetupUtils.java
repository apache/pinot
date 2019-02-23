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
package com.linkedin.pinot.opal.common.utils;

import org.apache.helix.HelixManager;
import org.apache.helix.controller.HelixControllerMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelixSetupUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(HelixSetupUtils.class);

  public static synchronized HelixManager setup(String helixClusterName, String zkPath, String instanceId,
                                                boolean isUpdateStateModel) {
    try {
      createHelixClusterIfNeeded(helixClusterName, zkPath, isUpdateStateModel);
    } catch (final Exception ex) {
      LOGGER.error("failed to set up helix for opal components", ex);
      return null;
    }

    try {
      return startHelixControllerInStandadloneMode(helixClusterName, zkPath, instanceId);
    } catch (final Exception ex) {
      LOGGER.error("failed to start up helix controller for opal", ex);
      return null;
    }

  }

  public static void createHelixClusterIfNeeded(String helixClusterName, String zkPath, Boolean isUpdateStateModel) {
    //TODO implement this logic
  }

  private static HelixManager startHelixControllerInStandadloneMode(String helixClusterName, String zkUrl,
      String instanceId) {
    LOGGER.info("Starting Helix Standalone Controller ... ");
    return HelixControllerMain.startHelixController(zkUrl, helixClusterName, instanceId,
        HelixControllerMain.STANDALONE);
  }
}
