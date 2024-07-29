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
package org.apache.pinot.broker.broker.helix;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.helix.HelixConstants.ChangeType;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.listeners.BatchMode;
import org.apache.helix.api.listeners.ExternalViewChangeListener;
import org.apache.helix.api.listeners.IdealStateChangeListener;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.api.listeners.LiveInstanceChangeListener;
import org.apache.helix.api.listeners.PreFetch;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.BrokerTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code ClusterChangeMediator} handles the changes from Helix cluster.
 * <p>
 * <p>If there is no change callback in 1 hour, proactively check changes so that the changes are getting processed even
 * when callbacks stop working.
 * <p>NOTE: disable Helix batch-mode and perform deduplication in this class. This can save us the extra threads for
 * handling Helix batch-mode, and let us track the cluster change queue time.
 * <p>NOTE: disable Helix pre-fetch to reduce the ZK accesses.
 */
@BatchMode(enabled = false)
@PreFetch(enabled = false)
public class ClusterChangeMediator
    implements IdealStateChangeListener, ExternalViewChangeListener, InstanceConfigChangeListener,
               LiveInstanceChangeListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterChangeMediator.class);

  // If no change got for 1 hour, proactively check changes
  private static final long PROACTIVE_CHANGE_CHECK_INTERVAL_MS = 3600 * 1000L;

  private final Map<ChangeType, List<ClusterChangeHandler>> _changeHandlersMap;
  private final Map<ChangeType, Long> _lastChangeTimeMap = new ConcurrentHashMap<>();
  private final Map<ChangeType, Long> _lastProcessTimeMap = new HashMap<>();

  private final Thread _clusterChangeHandlingThread;

  private volatile boolean _running;

  public ClusterChangeMediator(Map<ChangeType, List<ClusterChangeHandler>> changeHandlersMap,
      BrokerMetrics brokerMetrics) {
    _changeHandlersMap = changeHandlersMap;

    // Initialize last process time map
    long initTime = System.currentTimeMillis();
    for (ChangeType changeType : changeHandlersMap.keySet()) {
      _lastProcessTimeMap.put(changeType, initTime);
    }

    _clusterChangeHandlingThread = new Thread(() -> {
      while (_running) {
        try {
          for (Map.Entry<ChangeType, List<ClusterChangeHandler>> entry : _changeHandlersMap.entrySet()) {
            if (!_running) {
              return;
            }
            ChangeType changeType = entry.getKey();
            List<ClusterChangeHandler> changeHandlers = entry.getValue();
            long currentTime = System.currentTimeMillis();
            Long lastChangeTime = _lastChangeTimeMap.remove(changeType);
            if (lastChangeTime != null) {
              brokerMetrics.addTimedValue(BrokerTimer.CLUSTER_CHANGE_QUEUE_TIME, currentTime - lastChangeTime,
                  TimeUnit.MILLISECONDS);
              processClusterChange(changeType, changeHandlers);
            } else {
              long lastProcessTime = _lastProcessTimeMap.get(changeType);
              if (currentTime - lastProcessTime > PROACTIVE_CHANGE_CHECK_INTERVAL_MS) {
                LOGGER.info("Proactive check {} change", changeType);
                brokerMetrics.addMeteredGlobalValue(BrokerMeter.PROACTIVE_CLUSTER_CHANGE_CHECK, 1L);
                processClusterChange(changeType, changeHandlers);
              }
            }
          }
          synchronized (_lastChangeTimeMap) {
            if (!_running) {
              return;
            }
            // Wait for at most 1/10 of proactive change check interval if no new event received. This can guarantee
            // that the proactive change check will not be delayed for more than 1/10 of the interval. In case of
            // spurious wakeup, execute the while loop again for the proactive change check.
            if (_lastChangeTimeMap.isEmpty()) {
              _lastChangeTimeMap.wait(PROACTIVE_CHANGE_CHECK_INTERVAL_MS / 10);
            }
          }
        } catch (Exception e) {
          if (_running) {
            // Ignore all exceptions. The thread keeps running until ClusterChangeMediator.stop() is invoked.
            LOGGER.error("Caught exception within cluster change handling thread", e);
          }
        }
      }
    }, "ClusterChangeHandlingThread");
    _clusterChangeHandlingThread.setDaemon(true);
  }

  private synchronized void processClusterChange(ChangeType changeType, List<ClusterChangeHandler> changeHandlers) {
    long startTime = System.currentTimeMillis();
    LOGGER.info("Start processing {} change", changeType);
    for (ClusterChangeHandler changeHandler : changeHandlers) {
      String handlerName = changeHandler.getClass().getSimpleName();
      try {
        long handlerStartTime = System.currentTimeMillis();
        changeHandler.processClusterChange(changeType);
        LOGGER.info("Finish handling {} change for handler: {} in {}ms", changeType, handlerName,
            System.currentTimeMillis() - handlerStartTime);
      } catch (Exception e) {
        LOGGER.error("Caught exception while handling {} change for handler: {}", changeType, handlerName, e);
      }
    }
    long endTime = System.currentTimeMillis();
    LOGGER.info("Finish processing {} change in {}ms", changeType, endTime - startTime);
    _lastProcessTimeMap.put(changeType, endTime);
  }

  /**
   * Starts the cluster change mediator.
   */
  public void start() {
    LOGGER.info("Starting ClusterChangeMediator");
    _running = true;
    _clusterChangeHandlingThread.start();
  }

  /**
   * Stops the cluster change mediator.
   */
  public void stop() {
    LOGGER.info("Stopping ClusterChangeMediator");
    _running = false;
    try {
      _clusterChangeHandlingThread.interrupt();
      _clusterChangeHandlingThread.join();
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted while waiting for cluster change handling thread to finish", e);
    }
  }

  @Override
  public void onIdealStateChange(List<IdealState> idealStateList, NotificationContext changeContext)
      throws InterruptedException {
    // Ideal state list should be empty because Helix pre-fetch is disabled
    assert idealStateList.isEmpty();

    enqueueChange(ChangeType.IDEAL_STATE);
  }

  @Override
  public void onExternalViewChange(List<ExternalView> externalViewList, NotificationContext changeContext) {
    // External view list should be empty because Helix pre-fetch is disabled
    assert externalViewList.isEmpty();

    enqueueChange(ChangeType.EXTERNAL_VIEW);
  }

  @Override
  public void onInstanceConfigChange(List<InstanceConfig> instanceConfigs, NotificationContext changeContext) {
    // Instance config list should be empty because Helix pre-fetch is disabled
    assert instanceConfigs.isEmpty();

    enqueueChange(ChangeType.INSTANCE_CONFIG);
  }

  @Override
  public void onLiveInstanceChange(List<LiveInstance> liveInstances, NotificationContext changeContext) {
    // Live instance list should be empty because Helix pre-fetch is disabled
    assert liveInstances.isEmpty();

    enqueueChange(ChangeType.LIVE_INSTANCE);
  }

  /**
   * Helper method to enqueue a change from the Helix callback to be processed by the cluster change handling thread. If
   * the handling thread is dead, directly process the change.
   *
   * @param changeType Type of the change
   */
  private void enqueueChange(ChangeType changeType) {
    // Do not enqueue or process changes if already stopped
    if (!_running) {
      LOGGER.warn("ClusterChangeMediator already stopped, skipping enqueuing the {} change", changeType);
      return;
    }
    if (_clusterChangeHandlingThread.isAlive()) {
      LOGGER.info("Enqueueing {} change", changeType);
      if (_lastChangeTimeMap.putIfAbsent(changeType, System.currentTimeMillis()) == null) {
        synchronized (_lastChangeTimeMap) {
          _lastChangeTimeMap.notify();
        }
      }
    } else {
      LOGGER.warn("Cluster change handling thread is not alive, directly process the {} change", changeType);
      processClusterChange(changeType, _changeHandlersMap.get(changeType));
    }
  }
}
