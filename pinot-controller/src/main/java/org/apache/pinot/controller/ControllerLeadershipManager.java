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
package org.apache.pinot.controller;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.helix.HelixManager;
import org.apache.helix.api.listeners.ControllerChangeListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Single place for listening on controller changes
 * This should be created at controller startup and everyone who wants to listen to controller changes should subscribe
 */
public class ControllerLeadershipManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ControllerLeadershipManager.class);

  private HelixManager _helixManager;
  private volatile boolean _amILeader = false;

  private Map<String, LeadershipChangeSubscriber> _subscribers = new ConcurrentHashMap<>();

  public ControllerLeadershipManager(HelixManager helixManager) {
    _helixManager = helixManager;
    _helixManager.addControllerListener((ControllerChangeListener) notificationContext -> onControllerChange());
  }

  /**
   * When stopping this service, if the controller is leader, invoke {@link ControllerLeadershipManager#onBecomingNonLeader()}
   */
  public void stop() {
    if (_amILeader) {
      onBecomingNonLeader();
    }
  }

  /**
   * Callback on changes in the controller
   */
  protected void onControllerChange() {
    if (_helixManager.isLeader()) {
      if (!_amILeader) {
        _amILeader = true;
        LOGGER.info("Became leader");
        onBecomingLeader();
      } else {
        LOGGER.info("Already leader. Duplicate notification");
      }
    } else {
      if (_amILeader) {
        _amILeader = false;
        LOGGER.info("Lost leadership");
        onBecomingNonLeader();
      } else {
        LOGGER.info("Already not leader. Duplicate notification");
      }
    }
  }

  public boolean isLeader() {
    return _amILeader;
  }

  private void onBecomingLeader() {
    _subscribers.forEach((k, v) -> v.onBecomingLeader());
  }

  private void onBecomingNonLeader() {
    _subscribers.forEach((k, v) -> v.onBecomingNonLeader());
  }

  /**
   * Subscribe to changes in the controller leadership
   * If controller is already leader, invoke {@link LeadershipChangeSubscriber#onBecomingLeader()}
   * @param name
   * @param subscriber
   */
  public void subscribe(String name, LeadershipChangeSubscriber subscriber) {
    LOGGER.info("{} subscribing to leadership changes", name);
    _subscribers.put(name, subscriber);
    if (_amILeader) {
      subscriber.onBecomingLeader();
    }
  }
}
