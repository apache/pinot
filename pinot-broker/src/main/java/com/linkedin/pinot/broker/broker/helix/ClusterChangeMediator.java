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

package com.linkedin.pinot.broker.broker.helix;

import com.linkedin.pinot.broker.routing.HelixExternalViewBasedRouting;
import com.linkedin.pinot.common.metrics.BrokerMetrics;
import com.linkedin.pinot.common.metrics.BrokerTimer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.ExternalViewChangeListener;
import org.apache.helix.InstanceConfigChangeListener;
import org.apache.helix.LiveInstanceChangeListener;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manages the interactions between Helix cluster changes, the routing table and the connection pool.
 */
public class ClusterChangeMediator implements LiveInstanceChangeListener, ExternalViewChangeListener, InstanceConfigChangeListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterChangeMediator.class);
  private final HelixExternalViewBasedRouting _helixExternalViewBasedRouting;

  private enum UpdateType {
    EXTERNAL_VIEW,
    INSTANCE_CONFIG
  }

  private final LinkedBlockingQueue<Pair<UpdateType, Long>> _clusterChangeQueue = new LinkedBlockingQueue<>(1000);

  private Thread _deferredClusterUpdater = null;

  public ClusterChangeMediator(HelixExternalViewBasedRouting helixExternalViewBasedRouting,
      final BrokerMetrics brokerMetrics) {
    _helixExternalViewBasedRouting = helixExternalViewBasedRouting;

    // Simple thread that polls every 10 seconds to check if there are any cluster updates to apply
    _deferredClusterUpdater = new Thread("Deferred cluster state updater") {
      @Override
      public void run() {
        while (true) {
          try {
            // Wait for at least one update
            Pair<UpdateType, Long> firstUpdate = _clusterChangeQueue.take();

            // Update the queue time metrics
            long queueTime = System.currentTimeMillis() - firstUpdate.getValue();
            brokerMetrics.addTimedValue(BrokerTimer.ROUTING_TABLE_UPDATE_QUEUE_TIME, queueTime, TimeUnit.MILLISECONDS);

            // Take all other updates also present
            List<Pair<UpdateType, Long>> allUpdates = new ArrayList<>();
            allUpdates.add(firstUpdate);
            _clusterChangeQueue.drainTo(allUpdates);

            // Gather all update types
            boolean externalViewUpdated = false;
            boolean instanceConfigUpdated = false;

            for (Pair<UpdateType, Long> update : allUpdates) {
              if (update.getKey() == UpdateType.EXTERNAL_VIEW) {
                externalViewUpdated = true;
              } else if (update.getKey() == UpdateType.INSTANCE_CONFIG) {
                instanceConfigUpdated = true;
              }
            }

            if (externalViewUpdated) {
              try {
                _helixExternalViewBasedRouting.processExternalViewChange();
              } catch (Exception e) {
                LOGGER.warn("Caught exception while updating external view", e);
              }
            }

            if (instanceConfigUpdated) {
              try {
                _helixExternalViewBasedRouting.processInstanceConfigChange();
              } catch (Exception e) {
                LOGGER.warn("Caught exception while processing instance config", e);
              }
            }
          } catch (InterruptedException e) {
            LOGGER.warn("Was interrupted while waiting for a cluster change", e);
            break;
          }
        }

        LOGGER.warn("Stopping deferred cluster state update thread");
        _deferredClusterUpdater = null;
      }
    };

    _deferredClusterUpdater.start();
  }

  @Override
  public void onExternalViewChange(List<ExternalView> externalViewList, NotificationContext changeContext) {
    // If the deferred update thread is alive, defer the update
    if (_deferredClusterUpdater != null && _deferredClusterUpdater.isAlive()) {
      try {
        _clusterChangeQueue.put(new ImmutablePair<>(UpdateType.EXTERNAL_VIEW, System.currentTimeMillis()));
      } catch (InterruptedException e) {
        LOGGER.warn("Was interrupted while trying to add external view change to queue", e);
      }
    } else {
      LOGGER.warn("Deferred cluster updater thread is null or stopped, not deferring external view routing table rebuild");
      _helixExternalViewBasedRouting.processExternalViewChange();
    }
  }

  @Override
  public void onInstanceConfigChange(List<InstanceConfig> instanceConfigs, NotificationContext context) {
    // If the deferred update thread is alive, defer the update
    if (_deferredClusterUpdater != null && _deferredClusterUpdater.isAlive()) {
      try {
        _clusterChangeQueue.put(new ImmutablePair<>(UpdateType.INSTANCE_CONFIG, System.currentTimeMillis()));
      } catch (InterruptedException e) {
        LOGGER.warn("Was interrupted while trying to add external view change to queue", e);
      }
    } else {
      LOGGER.warn("Deferred cluster updater thread is null or stopped, not deferring instance config change notification");
      _helixExternalViewBasedRouting.processInstanceConfigChange();
    }
  }

  @Override
  public void onLiveInstanceChange(List<LiveInstance> liveInstances, NotificationContext changeContext) {
  }
}
