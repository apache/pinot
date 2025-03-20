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
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.version.PinotVersion;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.InstanceTypeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class SendStatsPredicate implements InstanceConfigChangeListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(SendStatsPredicate.class);

  public abstract boolean getSendStats();

  public static SendStatsPredicate create(PinotConfiguration configuration) {
    String modeStr = configuration.getProperty(
        CommonConstants.MultiStageQueryRunner.KEY_OF_SEND_STATS_MODE,
        CommonConstants.MultiStageQueryRunner.DEFAULT_SEND_STATS_MODE).toUpperCase(Locale.ENGLISH);
    Mode mode;
    try {
      mode = Mode.valueOf(modeStr.trim().toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid value " + modeStr + " for "
          + CommonConstants.MultiStageQueryRunner.KEY_OF_SEND_STATS_MODE, e);
    }
    return mode.create();
  }

  public enum Mode {
    SAFE {
      @Override
      public SendStatsPredicate create() {
        return new Safe();
      }
    },
    ALWAYS {
      @Override
      public SendStatsPredicate create() {
        return new SendStatsPredicate() {
          @Override
          public boolean getSendStats() {
            return false;
          }

          @Override
          public void onInstanceConfigChange(List<InstanceConfig> instanceConfigs, NotificationContext context) {
            // Nothing to do
          }
        };
      }
    },
    NEVER {
      @Override
      public SendStatsPredicate create() {
        return new SendStatsPredicate() {
          @Override
          public boolean getSendStats() {
            return false;
          }

          @Override
          public void onInstanceConfigChange(List<InstanceConfig> instanceConfigs, NotificationContext context) {
            // Nothing to do
          }
        };
      }
    };

    public abstract SendStatsPredicate create();
  }

  private static class Safe extends SendStatsPredicate {
    private final AtomicBoolean _sendStats = new AtomicBoolean(true);

    @Override
    public boolean getSendStats() {
      return _sendStats.get();
    }

    @Override
    public void onInstanceConfigChange(List<InstanceConfig> instanceConfigs, NotificationContext context) {
      Map<String, String> problematicVersionsById = new HashMap<>();
      for (InstanceConfig instanceConfig : instanceConfigs) {
        switch (InstanceTypeUtils.getInstanceType(instanceConfig.getInstanceName())) {
          case BROKER:
          case SERVER:
            String otherVersion = instanceConfig.getRecord()
                .getStringField(CommonConstants.Helix.Instance.PINOT_VERSION_KEY, null);
            if (isProblematicVersion(otherVersion)) {
              problematicVersionsById.put(instanceConfig.getInstanceName(), otherVersion);
            }
            break;
          default:
            continue;
        }
      }
      boolean sendStats = problematicVersionsById.isEmpty();
      if (_sendStats.getAndSet(sendStats) != sendStats) {
        if (sendStats) {
          LOGGER.warn("Send MSE stats is now enabled");
        } else {
          LOGGER.warn("Send MSE stats is now disabled (problematic versions: {})", problematicVersionsById);
        }
      }
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
        return true;
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
