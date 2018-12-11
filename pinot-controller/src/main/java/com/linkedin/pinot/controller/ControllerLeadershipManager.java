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

package com.linkedin.pinot.controller;

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

  private static ControllerLeadershipManager INSTANCE = null;

  private HelixManager _helixManager;
  private volatile boolean _amILeader = false;

  private Map<String, ControllerChangeSubscriber> _subscribers = new ConcurrentHashMap<>();

  private ControllerLeadershipManager(HelixManager helixManager) {
    _helixManager = helixManager;
    _helixManager.addControllerListener((ControllerChangeListener) notificationContext -> onControllerChange());
  }

  /**
   * Create an instance of ControllerLeadershipManager
   * @param helixManager
   */
  public static synchronized void createInstance(HelixManager helixManager) {
    if (INSTANCE != null) {
      throw new RuntimeException("Instance of ControllerLeadershipManager already created");
    }
    INSTANCE = new ControllerLeadershipManager(helixManager);
  }

  /**
   * Get the instance of ControllerLeadershipManager
   * @return
   */
  public static ControllerLeadershipManager getInstance() {
    if (INSTANCE == null) {
      throw new RuntimeException("Instance of ControllerLeadershipManager not yet created");
    }
    return INSTANCE;
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
      _amILeader = false;
      LOGGER.info("Lost leadership");
      onBecomingNonLeader();
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
   * @param name
   * @param subscriber
   */
  public void subscribe(String name, ControllerChangeSubscriber subscriber) {
    _subscribers.put(name, subscriber);
  }

  /**
   * Unsubscribe from changes in controller leadership
   * @param name
   */
  public void unsubscribe(String name) {
    _subscribers.remove(name);
  }
}
