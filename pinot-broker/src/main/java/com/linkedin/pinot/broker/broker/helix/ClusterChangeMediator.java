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

import com.linkedin.pinot.routing.HelixExternalViewBasedRouting;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
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

  private final AtomicInteger _externalViewChangeCount = new AtomicInteger(0);
  private final AtomicInteger _instanceConfigChangeCount = new AtomicInteger(0);
  private long _lastExternalViewUpdateTime = 0L;
  private long _lastInstanceConfigUpdateTime = 0L;
  private static final long MAX_TIME_BEFORE_IMMEDIATE_UPDATE_MILLIS = 30000L;

  private Thread _deferredClusterUpdater = null;

  public ClusterChangeMediator(HelixExternalViewBasedRouting helixExternalViewBasedRouting) {
    _helixExternalViewBasedRouting = helixExternalViewBasedRouting;

    // Simple thread that polls every 10 seconds to check if there are any cluster updates to apply
    _deferredClusterUpdater = new Thread("Deferred cluster state updater") {
      @Override
      public void run() {
        int lastExternalViewChangeCount = 0;
        int lastInstanceConfigChangeCount = 0;

        while (true) {
          // Check if we need to update the external view
          final int currentExternalViewChangeCount = _externalViewChangeCount.get();
          if (currentExternalViewChangeCount != lastExternalViewChangeCount) {
            try {
              _helixExternalViewBasedRouting.processExternalViewChange();
              _lastExternalViewUpdateTime = System.currentTimeMillis();
            } catch (Exception e) {
              LOGGER.warn("Caught exception when processing external view change", e);
            }

            lastExternalViewChangeCount = currentExternalViewChangeCount;
          }

          // Check if we need to update the instance configs
          final int currentInstanceConfigChangeCount = _instanceConfigChangeCount.get();
          if (currentInstanceConfigChangeCount != lastInstanceConfigChangeCount) {
            try {
              _helixExternalViewBasedRouting.processInstanceConfigChange();
              _lastInstanceConfigUpdateTime = System.currentTimeMillis();
            } catch (Exception e) {
              LOGGER.warn("Caught exception when processing instance config change", e);
            }

            lastInstanceConfigChangeCount = currentInstanceConfigChangeCount;
          }

          // Sleep for a bit
          try {
            Thread.sleep(10000);
          } catch (InterruptedException e) {
            LOGGER.info("Exiting deferred cluster state thread due to interruption", e);

            // Mark the deferred cluster updating thread as defunct, in case some non shutdown-related interruption
            // causes this thread to be stopped. This will disable update deferrals but will keep on working otherwise.
            _deferredClusterUpdater = null;

            break;
          }
        }
      }
    };

    _deferredClusterUpdater.start();
  }

  @Override
  public void onExternalViewChange(List<ExternalView> externalViewList, NotificationContext changeContext) {
    // If it's been a while since we last updated the external view, update the routing table immediately
    if (MAX_TIME_BEFORE_IMMEDIATE_UPDATE_MILLIS < System.currentTimeMillis() - _lastExternalViewUpdateTime) {
      _helixExternalViewBasedRouting.processExternalViewChange();
      _lastExternalViewUpdateTime = System.currentTimeMillis();
    } else if (_deferredClusterUpdater != null && _deferredClusterUpdater.isAlive()) {
      // Otherwise defer the update
      _externalViewChangeCount.incrementAndGet();
    } else {
      LOGGER.warn("Deferred cluster updater thread is null or stopped, not deferring external view routing table rebuild");
      _helixExternalViewBasedRouting.processExternalViewChange();
    }
  }

  @Override
  public void onInstanceConfigChange(List<InstanceConfig> instanceConfigs, NotificationContext context) {
    // If it's been a while since we last updated the instance configs, update the routing table immediately
    if (MAX_TIME_BEFORE_IMMEDIATE_UPDATE_MILLIS < System.currentTimeMillis() - _lastInstanceConfigUpdateTime) {
      _helixExternalViewBasedRouting.processInstanceConfigChange();
      _lastInstanceConfigUpdateTime = System.currentTimeMillis();
    } else if (_deferredClusterUpdater != null && _deferredClusterUpdater.isAlive()) {
      // Otherwise defer the update
      _instanceConfigChangeCount.incrementAndGet();
    } else {
      LOGGER.warn("Deferred cluster updater thread is null or stopped, not deferring instance config change notification");
      _helixExternalViewBasedRouting.processInstanceConfigChange();
    }
  }

  @Override
  public void onLiveInstanceChange(List<LiveInstance> liveInstances, NotificationContext changeContext) {
  }
}
