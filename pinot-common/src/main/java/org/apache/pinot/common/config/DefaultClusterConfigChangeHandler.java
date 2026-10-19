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
package org.apache.pinot.common.config;

import com.google.common.base.Preconditions;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.listeners.BatchMode;
import org.apache.helix.api.listeners.ClusterConfigChangeListener;
import org.apache.helix.model.ClusterConfig;
import org.apache.pinot.common.config.provider.ClusterConfigCache;
import org.apache.pinot.common.utils.ExceptionUtils;


/// Bridges Helix cluster config callbacks to [ClusterConfigCache]. All access is serialized on this
/// instance, so a listener sees the snapshot handed to it at registration and every later change in order.
@BatchMode(enabled = false)
public class DefaultClusterConfigChangeHandler extends ClusterConfigCache implements ClusterConfigChangeListener {
  private volatile boolean _initialSnapshotApplied;
  private boolean _subscriptionFailed;

  /// Registers this fresh handler at startup. Helix delivers INIT synchronously but can swallow initialization errors.
  /// On failure, runs the policy cleanup before disconnecting, then releases resources in the given order.
  public void subscribe(HelixManager manager, Runnable beforeDisconnect, Runnable... failureCleanup) {
    try {
      manager.addClusterfigChangeListener(this);
      Preconditions.checkState(_initialSnapshotApplied, "Helix did not deliver the initial cluster config");
    } catch (Exception e) {
      synchronized (this) {
        // Prevent late callbacks, including FINALIZE during disconnect, from overwriting fail-closed cleanup.
        _subscriptionFailed = true;
      }
      runCleanup(beforeDisconnect, e);
      runCleanup(manager::disconnect, e);
      for (Runnable cleanup : failureCleanup) {
        runCleanup(cleanup, e);
      }
      throw new IllegalStateException("Failed to subscribe to cluster config", e);
    }
  }

  private static void runCleanup(Runnable cleanup, Exception cause) {
    try {
      cleanup.run();
    } catch (RuntimeException e) {
      ExceptionUtils.suppress(e, cause);
    }
  }

  @Override
  public synchronized void onClusterConfigChange(ClusterConfig clusterConfig, NotificationContext context) {
    if (_subscriptionFailed) {
      return;
    }
    updateClusterConfigs(clusterConfig.getRecord().getSimpleFields());
    if (context != null && context.getType() == NotificationContext.Type.INIT) {
      _initialSnapshotApplied = true;
    }
  }
}
