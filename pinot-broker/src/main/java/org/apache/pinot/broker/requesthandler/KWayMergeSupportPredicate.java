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


/// Enables new merge plan nodes only after a complete homogeneous live broker/server release-version snapshot.
/// Shares live version tracking with the SAFE stats policy and fails closed on initialization and read failures.
/// Snapshot builds are excluded because equal snapshot versions can contain different protocol implementations.
/// Query threads only read the tracker's volatile result without locking.
@BatchMode(enabled = false)
@PreFetch(enabled = false)
public class KWayMergeSupportPredicate
    implements InstanceConfigChangeListener, LiveInstanceChangeListener, BooleanSupplier {
  private final ClusterVersionTracker _versions;
  private final boolean _releaseVersion;

  public KWayMergeSupportPredicate(HelixManager manager) {
    this(manager, PinotVersion.VERSION);
  }

  KWayMergeSupportPredicate(HelixManager manager, String version) {
    _versions = new ClusterVersionTracker(manager, version);
    _releaseVersion = version != null && !version.equals(PinotVersion.UNKNOWN) && !version.contains("SNAPSHOT");
  }

  public boolean needWatchForVersionChanges() {
    return _releaseVersion;
  }

  public void stop() {
    _versions.stop();
  }

  @Override
  public boolean getAsBoolean() {
    return _releaseVersion && _versions.isSameVersionWithBrokerAndServer();
  }

  @Override
  public void onInstanceConfigChange(List<InstanceConfig> configs, NotificationContext context) {
    if (_releaseVersion) {
      _versions.onInstanceConfigChange(configs, context);
    }
  }

  @Override
  @PreFetch(enabled = false)
  public void onLiveInstanceChange(List<LiveInstance> liveInstances, NotificationContext context) {
    if (_releaseVersion) {
      _versions.onLiveInstanceChange(liveInstances, context);
    }
  }
}
