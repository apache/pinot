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
package org.apache.pinot.query.runtime;

import java.util.List;
import java.util.Locale;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.listeners.BatchMode;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.api.listeners.LiveInstanceChangeListener;
import org.apache.helix.api.listeners.PreFetch;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.pinot.common.utils.helix.ClusterVersionTracker;
import org.apache.pinot.common.version.PinotVersion;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;

/// A class used to determine whether MSE should to send stats or not.
///
/// The stat mechanism used in MSE is very efficient, so contrary to what we do in SSE, we decided to always collect and
/// send stats in MSE. However, there are some versions of Pinot that have known issues with the stats mechanism, so we
/// created this class as a mechanism to disable stats sending in case of problematic versions.
///
/// Specifically, Pinot 1.3.0 and lower have known issues when they receive unexpected stats from upstream stages, but
/// even these versions are prepared to receive empty stats from upstream stages.
/// Therefore the cleanest and safer solution is to not send stats when we know a problematic version is in the cluster.
///
/// We support three modes:
/// - ALWAYS: This is the default mode. In this mode, we will always send stats, regardless of the version of the
///  cluster.
/// - SAFE: In this mode, we will send stats unless we detect a problematic version in the cluster, which means any
///  instance reporting a version other than this one. This doesn't require human intervention, and is the mode to
///  use for a cluster that may still run versions older than 1.4. SAFE waits for an initial live-version snapshot
///  and disables stats after Helix FINALIZE until tracking is initialized again.
/// - NEVER: In this mode, we will never send stats, regardless of the version of the cluster. This is useful for
/// testing purposes or if for whatever reason you want to disable stats.
public abstract class SendStatsPredicate implements InstanceConfigChangeListener, LiveInstanceChangeListener {

  public abstract boolean isSendStats();

  public abstract boolean needWatchForInstanceConfigChange();

  @Override
  public void onLiveInstanceChange(List<LiveInstance> instances, NotificationContext context) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  // NOTE: When this method is called, the helix manager is not yet connected.
  public static SendStatsPredicate create(PinotConfiguration serverConf, HelixManager helixManager) {
    String modeStr = serverConf.getProperty(
        CommonConstants.MultiStageQueryRunner.KEY_OF_SEND_STATS_MODE,
        CommonConstants.MultiStageQueryRunner.DEFAULT_SEND_STATS_MODE).toUpperCase(Locale.ENGLISH);
    Mode mode;
    try {
      mode = Mode.valueOf(modeStr.trim().toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid value " + modeStr + " for "
          + CommonConstants.MultiStageQueryRunner.KEY_OF_SEND_STATS_MODE, e);
    }
    return mode.create(helixManager);
  }

  public enum Mode {
    /// Sends stats only if all the cluster participants use the same known version.
    // ALWAYS is strictly better than SAFE if all servers are already on versions >= 1.4
    SAFE {
      @Override
      public SendStatsPredicate create(HelixManager helixManager) {
        return new Safe(helixManager);
      }
    },
    ALWAYS {
      @Override
      public SendStatsPredicate create(HelixManager helixManager) {
        return new SendStatsPredicate() {
          @Override
          public boolean isSendStats() {
            return true;
          }

          @Override
          public boolean needWatchForInstanceConfigChange() {
            return false;
          }

          @Override
          public void onInstanceConfigChange(List<InstanceConfig> instanceConfigs, NotificationContext context) {
            throw new UnsupportedOperationException("Should not be invoked");
          }
        };
      }
    },
    NEVER {
      @Override
      public SendStatsPredicate create(HelixManager helixManager) {
        return new SendStatsPredicate() {
          @Override
          public boolean isSendStats() {
            return false;
          }

          @Override
          public boolean needWatchForInstanceConfigChange() {
            return false;
          }

          @Override
          public void onInstanceConfigChange(List<InstanceConfig> instanceConfigs, NotificationContext context) {
            throw new UnsupportedOperationException("Should not be invoked");
          }
        };
      }
    };

    public abstract SendStatsPredicate create(HelixManager helixManager);
  }

  @BatchMode(enabled = false)
  @PreFetch(enabled = false)
  private static class Safe extends SendStatsPredicate {
    private final ClusterVersionTracker _versionTracker;

    public Safe(HelixManager helixManager) {
      // SAFE stats permits homogeneous SNAPSHOT versions and does not require either instance role to be present.
      _versionTracker = new ClusterVersionTracker(helixManager, PinotVersion.VERSION, true, false);
    }

    @Override
    public boolean isSendStats() {
      return _versionTracker.getAsBoolean();
    }

    @Override
    public boolean needWatchForInstanceConfigChange() {
      return true;
    }

    @Override
    public void onInstanceConfigChange(List<InstanceConfig> configs, NotificationContext context) {
      _versionTracker.onInstanceConfigChange(configs, context);
    }

    @Override
    public void onLiveInstanceChange(List<LiveInstance> instances, NotificationContext context) {
      _versionTracker.onLiveInstanceChange(instances, context);
    }
  }
}
