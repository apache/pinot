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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.helix.LeadControllerUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class for handling lead controller assignments given the table names. This should be created at controller startup.
 */
@ThreadSafe
public class LeadControllerManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(LeadControllerManager.class);
  private static final long CONTROLLER_LEADERSHIP_FETCH_INTERVAL_MS = 60_000L;

  private final String _helixControllerInstanceId;
  private final HelixManager _helixManager;
  private final ControllerMetrics _controllerMetrics;
  private final Set<Integer> _leadForPartitions;
  private final Thread _controllerLeadershipFetchingThread;

  private volatile boolean _isLeadControllerResourceEnabled = false;
  private volatile boolean _amIHelixLeader = false;
  private volatile boolean _isShuttingDown = false;

  public LeadControllerManager(String helixControllerInstanceId, HelixManager helixManager,
      ControllerMetrics controllerMetrics) {
    _helixControllerInstanceId = helixControllerInstanceId;
    _helixManager = helixManager;
    _controllerMetrics = controllerMetrics;
    _leadForPartitions = ConcurrentHashMap.newKeySet();

    // Create a thread to periodically fetch controller leadership as a work-around of Helix callback delay
    _controllerLeadershipFetchingThread = new Thread("ControllerLeadershipFetchingThread") {
      @Override
      public void run() {
        while (true) {
          try {
            synchronized (LeadControllerManager.this) {
              if (_isShuttingDown) {
                return;
              }
              if (isHelixLeader()) {
                if (!_amIHelixLeader) {
                  _amIHelixLeader = true;
                  LOGGER.warn("Becoming leader without getting Helix change callback");
                  _controllerMetrics
                      .addMeteredGlobalValue(ControllerMeter.CONTROLLER_LEADERSHIP_CHANGE_WITHOUT_CALLBACK, 1L);
                }
                _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.PINOT_CONTROLLER_LEADER, 1L);
              } else {
                if (_amIHelixLeader) {
                  _amIHelixLeader = false;
                  LOGGER.warn("Losing leadership without getting Helix change callback");
                  _controllerMetrics
                      .addMeteredGlobalValue(ControllerMeter.CONTROLLER_LEADERSHIP_CHANGE_WITHOUT_CALLBACK, 1L);
                }
                _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.PINOT_CONTROLLER_LEADER, 0L);
              }
              LeadControllerManager.this.wait(CONTROLLER_LEADERSHIP_FETCH_INTERVAL_MS);
            }
          } catch (Exception e) {
            // Ignore all exceptions. The thread keeps running until LeadControllerManager.stop() is invoked.
            LOGGER.error("Caught exception within controller leadership fetching thread", e);
          }
        }
      }
    };
  }

  /**
   * Checks whether the current controller is the leader for the given table. Return true if current controller is
   * the leader for this table.
   * Otherwise check whether the current controller is helix leader if the resource is disabled.
   * @param tableName table name with/without table type.
   */
  public boolean isLeaderForTable(String tableName) {
    if (_isLeadControllerResourceEnabled) {
      String rawTableName = TableNameBuilder.extractRawTableName(tableName);
      int partitionId = LeadControllerUtils.getPartitionIdForTable(rawTableName);
      return _leadForPartitions.contains(partitionId);
    } else {
      // Checks if it's Helix leader if lead controller resource is disabled.
      return _amIHelixLeader;
    }
  }

  /**
   * Given a partition name, marks current controller as lead controller for this partition by caching the partition
   * id to current controller.
   * @param partitionName partition name in lead controller resource, e.g. leadControllerResource_0.
   */
  public synchronized void addPartitionLeader(String partitionName) {
    LOGGER.info("Add Partition: {} to LeadControllerManager", partitionName);
    int partitionId = LeadControllerUtils.extractPartitionId(partitionName);
    _leadForPartitions.add(partitionId);
    _controllerMetrics
        .setValueOfGlobalGauge(ControllerGauge.CONTROLLER_LEADER_PARTITION_COUNT, _leadForPartitions.size());
  }

  /**
   * Given a partition name, removes current controller as lead controller for this partition by removing the
   * partition id from current controller.
   * @param partitionName partition name in lead controller resource, e.g. leadControllerResource_0.
   */
  public synchronized void removePartitionLeader(String partitionName) {
    LOGGER.info("Remove Partition: {} from LeadControllerManager", partitionName);
    int partitionId = LeadControllerUtils.extractPartitionId(partitionName);
    _leadForPartitions.remove(partitionId);
    _controllerMetrics
        .setValueOfGlobalGauge(ControllerGauge.CONTROLLER_LEADER_PARTITION_COUNT, _leadForPartitions.size());
  }

  /**
   * Checks from ZK if the current controller host is Helix cluster leader.
   */
  private boolean isHelixLeader() {
    try {
      String helixLeaderInstanceId = LeadControllerUtils.getHelixClusterLeader(_helixManager);
      if (helixLeaderInstanceId == null) {
        LOGGER.warn("Helix leader ZNode is missing");
        return false;
      }
      return _helixControllerInstanceId.equals(helixLeaderInstanceId);
    } catch (Exception e) {
      LOGGER.error("Exception when getting Helix leader", e);
      return false;
    }
  }

  /**
   * Starts the fetching thread to actively fetch helix leadership and resource config of lead controller resource.
   */
  public synchronized void start() {
    _controllerLeadershipFetchingThread.start();
  }

  /**
   * Marks the cached indices invalid and isShuttingDown to be true.
   * Adding the synchronized block here and in the following callback methods
   * to make sure that {@link HelixManager} won't be closed when the callback changes happened.
   */
  public void stop() {
    synchronized (this) {
      _isShuttingDown = true;
      notify();
    }
    _leadForPartitions.clear();

    try {
      _controllerLeadershipFetchingThread.join();
    } catch (InterruptedException e) {
      LOGGER.error("Caught InterruptedException while waiting for controller leadership fetching thread to die");
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Callback on changes in the controller. Should be registered to the controller callback. This callback is not
   * needed when the resource is enabled.
   * However, the resource can be disabled sometime while the cluster is in operation, so we keep it here. Plus, it
   * does not add much overhead.
   * At some point in future when we stop supporting the disabled resource, we will remove this line altogether and
   * the logic that goes with it.
   */
  synchronized void onHelixControllerChange() {
    if (_isShuttingDown) {
      return;
    }
    if (isHelixLeader()) {
      if (!_amIHelixLeader) {
        _amIHelixLeader = true;
        LOGGER.info("Became Helix leader");
      } else {
        LOGGER.info("Already Helix leader. Duplicate notification");
      }
      _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.PINOT_CONTROLLER_LEADER, 1L);
    } else {
      if (_amIHelixLeader) {
        _amIHelixLeader = false;
        LOGGER.info("Lost Helix leadership");
      } else {
        LOGGER.info("Already not Helix leader. Duplicate notification");
      }
      _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.PINOT_CONTROLLER_LEADER, 0L);
    }
  }

  /**
   * Callback on changes in resource config.
   */
  synchronized void onResourceConfigChange() {
    if (_isShuttingDown) {
      return;
    }

    boolean leadControllerResourceEnabled;
    try {
      leadControllerResourceEnabled = LeadControllerUtils.isLeadControllerResourceEnabled(_helixManager);
    } catch (Exception e) {
      // Do not change the state if any exception happened.
      // Enabling the resource is always one-off. If administrator wants to enable it he will check the log.
      // Plus, it's quite common to have resource config changes because every time there's a Helix task generated,
      // the task will be written to resource config, which will trigger this notification as well.
      LOGGER.error("Exception when checking whether lead controller resource is enabled or not.", e);
      return;
    }

    if (leadControllerResourceEnabled) {
      LOGGER.info("Lead controller resource is enabled.");
      _isLeadControllerResourceEnabled = true;
      _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.PINOT_LEAD_CONTROLLER_RESOURCE_ENABLED, 1L);
    } else {
      LOGGER.info("Lead controller resource is disabled.");
      _isLeadControllerResourceEnabled = false;
      _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.PINOT_LEAD_CONTROLLER_RESOURCE_ENABLED, 0L);
    }
  }
}
