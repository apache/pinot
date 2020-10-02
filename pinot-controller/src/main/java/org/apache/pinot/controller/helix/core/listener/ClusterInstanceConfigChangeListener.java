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
package org.apache.pinot.controller.helix.core.listener;

import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.model.InstanceConfig;

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.utils.helix.HelixHelper;


public class ClusterInstanceConfigChangeListener implements InstanceConfigChangeListener {
  private final HelixManager _helixManager;
  private List<InstanceConfig> _instanceConfigs = new ArrayList<>();
  private Long _lastEventTimestamp = null;
  private boolean _listenerInitiated = false;

  public ClusterInstanceConfigChangeListener(HelixManager helixManager) {
    _helixManager = helixManager;
  }

  @Override
  public synchronized void onInstanceConfigChange(List<InstanceConfig> instanceConfigs, NotificationContext context) {
    if (context.getType() == NotificationContext.Type.INIT) {
      _listenerInitiated = true;
    }

    if (_lastEventTimestamp == null || _lastEventTimestamp <= context.getCreationTime()) {
      _instanceConfigs = instanceConfigs;
      _lastEventTimestamp = context.getCreationTime();
    }
  }

  public List<InstanceConfig> getInstanceConfigs() {
    if (_instanceConfigs.isEmpty() || !_listenerInitiated) {
       return HelixHelper.getInstanceConfigs(_helixManager);
    }
    return _instanceConfigs;
  }
}
