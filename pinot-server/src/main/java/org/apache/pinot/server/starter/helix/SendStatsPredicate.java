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
package org.apache.pinot.server.starter.helix;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.listeners.BatchMode;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.api.listeners.PreFetch;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.version.PinotVersion;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.InstanceTypeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
/// - SAFE: This is the default mode. In this mode, we will send stats unless we detect a problematic version in the
///  cluster. This doesn't require human intervention and is the recommended mode.
/// - ALWAYS: In this mode, we will always send stats, regardless of the version of the cluster. This mimics the
///  behavior in 1.3.0 and lower versions.
/// - NEVER: In this mode, we will never send stats, regardless of the version of the cluster. This is useful for
/// testing purposes or if for whatever reason you want to disable stats.
public abstract class SendStatsPredicate implements InstanceConfigChangeListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(SendStatsPredicate.class);

  public abstract boolean isSendStats();

  public abstract boolean needWatchForInstanceConfigChange();

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
    private final HelixManager _helixManager;
    private final String _clusterName;
    private final Map<String, String> _problematicVersionsById = new HashMap<>();

    private HelixAdmin _helixAdmin;
    private volatile boolean _sendStats = true;

    public Safe(HelixManager helixManager) {
      _helixManager = helixManager;
      _clusterName = helixManager.getClusterName();
    }

    @Override
    public boolean isSendStats() {
      return _sendStats;
    }

    @Override
    public boolean needWatchForInstanceConfigChange() {
      return true;
    }

    @Override
    public synchronized void onInstanceConfigChange(List<InstanceConfig> instanceConfigs, NotificationContext context) {
      if (_helixAdmin == null) {
        _helixAdmin = _helixManager.getClusterManagmentTool();
      }
      NotificationContext.Type type = context.getType();
      if (type != NotificationContext.Type.INIT && type != NotificationContext.Type.CALLBACK) {
        LOGGER.warn("Ignoring notification type: {} for instance config change", type);
        return;
      }
      if (type == NotificationContext.Type.INIT || context.getIsChildChange()) {
        _problematicVersionsById.clear();
        for (String instance : _helixAdmin.getInstancesInCluster(_clusterName)) {
          if (needVersionCheck(instance)) {
            InstanceConfig instanceConfig;
            try {
              instanceConfig = _helixAdmin.getInstanceConfig(_clusterName, instance);
            } catch (Exception e) {
              LOGGER.warn("Failed to get instance config for instance: {}, continue", instance, e);
              continue;
            }
            String version = getVersion(instanceConfig);
            if (isProblematicVersion(version)) {
              _problematicVersionsById.put(instance, version);
            }
          }
        }
      } else {
        String pathChanged = context.getPathChanged();
        String instanceName = pathChanged.substring(pathChanged.lastIndexOf('/') + 1);
        if (needVersionCheck(instanceName)) {
          InstanceConfig instanceConfig;
          try {
            instanceConfig = _helixAdmin.getInstanceConfig(_clusterName, instanceName);
            String version = getVersion(instanceConfig);
            if (isProblematicVersion(version)) {
              _problematicVersionsById.put(instanceName, version);
            } else {
              _problematicVersionsById.remove(instanceName);
            }
          } catch (Exception e) {
            LOGGER.warn("Failed to get instance config for instance: {}, treating it as non-problematic", instanceName,
                e);
            _problematicVersionsById.remove(instanceName);
          }
        }
      }
      boolean sendStats = _problematicVersionsById.isEmpty();
      if (_sendStats != sendStats) {
        _sendStats = sendStats;
        if (sendStats) {
          LOGGER.warn("Send MSE stats is now enabled");
        } else {
          LOGGER.warn("Send MSE stats is now disabled (problematic versions: {})", _problematicVersionsById);
        }
      }
    }

    private boolean needVersionCheck(String instanceName) {
      InstanceType instanceType = InstanceTypeUtils.getInstanceType(instanceName);
      return instanceType == InstanceType.BROKER || instanceType == InstanceType.SERVER;
    }

    @Nullable
    private String getVersion(InstanceConfig instanceConfig) {
      return instanceConfig.getRecord().getStringField(CommonConstants.Helix.Instance.PINOT_VERSION_KEY, null);
    }

    /// Returns true if the version is problematic
    ///
    /// Ideally [PinotVersion] should have a way to extract versions in comparable format, but given it doesn't we
    /// need to parse the string here. In case version doesn't match `1\.x\..*`, we treat is as a problematic version
    private boolean isProblematicVersion(@Nullable String versionStr) {
      if (versionStr == null) {
        return true;
      }
      if (versionStr.equals(PinotVersion.UNKNOWN)) {
        return true;
      }
      if (versionStr.equals(PinotVersion.VERSION)) {
        return false;
      }
      // Lets try to parse 1.x versions
      String[] splits = versionStr.trim().split("\\.");
      if (splits.length < 2) {
        return true;
      }
      // Versions less than 1.x are problematic for sure
      if (!splits[0].equals("1")) {
        return true;
      }
      try {
        // Versions less than 1.4 are problematic
        if (Integer.parseInt(splits[1]) < 4) {
          return true;
        }
      } catch (NumberFormatException e) {
        return true;
      }
      return false;
    }
  }
}
