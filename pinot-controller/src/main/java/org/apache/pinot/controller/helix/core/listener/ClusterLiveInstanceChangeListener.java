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

import java.util.ArrayList;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.listeners.LiveInstanceChangeListener;
import org.apache.helix.model.LiveInstance;

import java.util.List;
import org.apache.helix.PropertyKey.Builder;


public class ClusterLiveInstanceChangeListener implements LiveInstanceChangeListener {
  private final HelixDataAccessor _helixDataAccessor;
  private Builder _keyBuilder;
  private List<LiveInstance> _liveInstances = new ArrayList<>();
  private Long _lastEventTimestamp = null;
  private boolean _listenerInitiated = false;

  public ClusterLiveInstanceChangeListener(HelixDataAccessor helixDataAccessor, Builder keyBuilder) {
    _helixDataAccessor = helixDataAccessor;
    _keyBuilder = keyBuilder;
  }

  @Override
  public synchronized void onLiveInstanceChange(List<LiveInstance> liveInstances, NotificationContext changeContext) {
    if (changeContext.getType() == NotificationContext.Type.INIT) {
      _listenerInitiated = true;
    }

    if (_lastEventTimestamp == null || _lastEventTimestamp <= changeContext.getCreationTime()) {
      _liveInstances = liveInstances;
      _lastEventTimestamp = changeContext.getCreationTime();
    }
  }

  public List<LiveInstance> getLiveInstances() {
    if (_liveInstances.isEmpty() || !_listenerInitiated) {
       return _helixDataAccessor.getChildValues(_keyBuilder.liveInstances());
    }
    return _liveInstances;
  }
}
