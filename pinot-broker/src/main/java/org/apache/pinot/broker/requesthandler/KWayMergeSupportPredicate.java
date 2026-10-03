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
package org.apache.pinot.broker.requesthandler;

import java.util.List;
import java.util.function.BooleanSupplier;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.listeners.BatchMode;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.api.listeners.PreFetch;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.version.PinotVersion;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.InstanceTypeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Enables new merge plan nodes only after a complete homogeneous broker/server release-version snapshot.
/// Uses the SAFE stats policy's version comparison, but fails closed on initialization and read failures.
/// Snapshot builds are excluded because equal snapshot versions can contain different protocol implementations.
/// Helix drives updates serially; query threads read the volatile result without locking.
@BatchMode(enabled = false)
@PreFetch(enabled = false)
public class KWayMergeSupportPredicate implements InstanceConfigChangeListener, BooleanSupplier {
  private static final Logger LOGGER = LoggerFactory.getLogger(KWayMergeSupportPredicate.class);
  private final HelixManager _manager;
  private final String _version;
  private volatile boolean _supported;

  public KWayMergeSupportPredicate(HelixManager manager) {
    this(manager, PinotVersion.VERSION);
  }

  KWayMergeSupportPredicate(HelixManager manager, String version) {
    _manager = manager;
    _version = version;
  }

  @Override
  public boolean getAsBoolean() {
    return _supported && _manager.isConnected();
  }

  @Override
  public synchronized void onInstanceConfigChange(List<InstanceConfig> configs, NotificationContext context) {
    // Clear first: no partial snapshot or failed refresh may leave the new wire node enabled.
    _supported = false;
    if (context.getType() != NotificationContext.Type.INIT
        && context.getType() != NotificationContext.Type.CALLBACK) {
      return;
    }
    if (_version == null || _version.equals(PinotVersion.UNKNOWN) || _version.contains("SNAPSHOT")) {
      return;
    }
    try {
      HelixAdmin admin = _manager.getClusterManagmentTool();
      String cluster = _manager.getClusterName();
      List<String> instances = admin.getInstancesInCluster(cluster);
      boolean foundServer = false;
      boolean foundBroker = false;
      for (String instance : instances) {
        InstanceType type = InstanceTypeUtils.getInstanceType(instance);
        if (type != InstanceType.BROKER && type != InstanceType.SERVER) {
          continue;
        }
        InstanceConfig config = admin.getInstanceConfig(cluster, instance);
        String version = config.getRecord().getStringField(CommonConstants.Helix.Instance.PINOT_VERSION_KEY, null);
        if (!_version.equals(version)) {
          return;
        }
        foundServer |= type == InstanceType.SERVER;
        foundBroker |= type == InstanceType.BROKER;
      }
      _supported = foundServer && foundBroker;
    } catch (Exception e) {
      LOGGER.warn("Cannot verify homogeneous versions; k-way merge remains disabled", e);
    }
  }
}
