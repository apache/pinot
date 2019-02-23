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
package com.linkedin.pinot.opal.distributed.keyCoordinator.helix;

import org.apache.helix.HelixManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

public class KeyCoordinatorHelixManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(KeyCoordinatorHelixManager.class);

  private final String _helixZkURL;
  private final String _helixClusterName;
  private final String _keyCoordinatorId;

  private HelixManager _helixZkManager;

  public KeyCoordinatorHelixManager(@Nonnull String zkURL, @Nonnull String helixClusterName,
                                    @Nonnull String keyCoordinatorId) {
    _helixZkURL = zkURL;
    _helixClusterName = helixClusterName;
    _keyCoordinatorId = keyCoordinatorId;
  }

  public synchronized void start() {
    LOGGER.info("starting key coordinator");
    // TODO add helix related logics for fail over
    LOGGER.info("finished starting of key coordinator");
  }
}
