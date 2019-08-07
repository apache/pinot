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

import java.util.HashMap;
import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Single place for listening on controller changes.
 * This should be created at controller startup and everyone who wants to listen to controller changes should subscribe.
 */
@ThreadSafe
public class ControllerLeadershipManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(ControllerLeadershipManager.class);
  private static final long CONTROLLER_LEADERSHIP_FETCH_INTERVAL = 60_000L;

  private final HelixManager _helixControllerManager;
  private final ControllerMetrics _controllerMetrics;
  private final Map<String, LeadershipChangeSubscriber> _subscribers = new HashMap<>();
  private final Thread _controllerLeadershipFetchingThread;

  private boolean _amILeader = false;
  private boolean _stopped = false;

  public ControllerLeadershipManager(HelixManager helixControllerManager, ControllerMetrics controllerMetrics) {
    _helixControllerManager = helixControllerManager;
    _controllerMetrics = controllerMetrics;
    _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.PINOT_CONTROLLER_LEADER, 0L);

    // Create a thread to periodically fetch controller leadership as a work-around of Helix callback delay
    _controllerLeadershipFetchingThread = new Thread("ControllerLeadershipFetchingThread") {
      @Override
      public void run() {
        while (true) {
          try {
            synchronized (ControllerLeadershipManager.this) {
              if (_stopped) {
                return;
              }
              if (_helixControllerManager.isLeader()) {
                if (!_amILeader) {
                  _amILeader = true;
                  LOGGER.warn("Becoming leader without getting Helix change callback");
                  _controllerMetrics
                      .addMeteredGlobalValue(ControllerMeter.CONTROLLER_LEADERSHIP_CHANGE_WITHOUT_CALLBACK, 1L);
                  onBecomingLeader();
                }
              } else {
                if (_amILeader) {
                  _amILeader = false;
                  LOGGER.warn("Losing leadership without getting Helix change callback");
                  _controllerMetrics
                      .addMeteredGlobalValue(ControllerMeter.CONTROLLER_LEADERSHIP_CHANGE_WITHOUT_CALLBACK, 1L);
                  onBecomingNonLeader();
                }
              }
              ControllerLeadershipManager.this.wait(CONTROLLER_LEADERSHIP_FETCH_INTERVAL);
            }
          } catch (Exception e) {
            // Ignore all exceptions. The thread keeps running until ControllerLeadershipManager.stop() is invoked.
            LOGGER.error("Caught exception within controller leadership fetching thread", e);
          }
        }
      }
    };
  }

  /**
   * Subscribes to changes in the controller leadership.
   * <p>If controller is already leader, invoke {@link LeadershipChangeSubscriber#onBecomingLeader()}
   */
  public synchronized void subscribe(String name, LeadershipChangeSubscriber subscriber) {
    LOGGER.info("{} subscribing to leadership changes", name);
    _subscribers.put(name, subscriber);
    if (_amILeader) {
      subscriber.onBecomingLeader();
    }
  }

  public boolean isLeader() {
    return _amILeader;
  }

  /**
   * Starts the service.
   */
  public synchronized void start() {
    onControllerChange();
    _controllerLeadershipFetchingThread.start();
  }

  /**
   * Stops the service.
   * <p>If controller is leader, invoke {@link ControllerLeadershipManager#onBecomingNonLeader()}
   */
  public void stop() {
    synchronized (this) {
      if (_amILeader) {
        onBecomingNonLeader();
      }
      _stopped = true;
      notify();
    }
    try {
      _controllerLeadershipFetchingThread.join();
    } catch (InterruptedException e) {
      LOGGER.error("Caught InterruptedException while waiting for controller leadership fetching thread to die");
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Callback on changes in the controller. Should be registered to the controller callback.
   */
  synchronized void onControllerChange() {
    if (_helixControllerManager.isLeader()) {
      if (!_amILeader) {
        _amILeader = true;
        LOGGER.info("Becoming leader");
        onBecomingLeader();
      }
    } else {
      if (_amILeader) {
        _amILeader = false;
        LOGGER.info("Losing leadership");
        onBecomingNonLeader();
      }
    }
  }

  private void onBecomingLeader() {
    long startTimeMs = System.currentTimeMillis();
    _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.PINOT_CONTROLLER_LEADER, 1L);
    for (LeadershipChangeSubscriber subscriber : _subscribers.values()) {
      subscriber.onBecomingLeader();
    }
    LOGGER.info("Finished on becoming leader in {}ms", System.currentTimeMillis() - startTimeMs);
  }

  private void onBecomingNonLeader() {
    long startTimeMs = System.currentTimeMillis();
    _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.PINOT_CONTROLLER_LEADER, 0L);
    for (LeadershipChangeSubscriber subscriber : _subscribers.values()) {
      subscriber.onBecomingNonLeader();
    }
    LOGGER.info("Finished on becoming non-leader in {}ms", System.currentTimeMillis() - startTimeMs);
  }
}
