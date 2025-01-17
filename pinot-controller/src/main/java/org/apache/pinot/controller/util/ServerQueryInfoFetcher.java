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
package org.apache.pinot.controller.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.InstanceTypeUtils;


/**
 * This is a helper class that fetch server information from Helix/ZK. It caches the server information to avoid
 * repeated ZK access. This class is NOT thread-safe.
 */
public class ServerQueryInfoFetcher {
  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final Map<String, ServerQueryInfo> _cache;

  public ServerQueryInfoFetcher(PinotHelixResourceManager pinotHelixResourceManager) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _cache = new HashMap<>();
  }

  @Nullable
  public ServerQueryInfo getServerQueryInfo(String instanceId) {
    return _cache.computeIfAbsent(instanceId, this::getServerQueryInfoOndemand);
  }

  @Nullable
  private ServerQueryInfo getServerQueryInfoOndemand(String instanceId) {
    InstanceConfig instanceConfig = _pinotHelixResourceManager.getHelixInstanceConfig(instanceId);
    if (instanceConfig == null || !InstanceTypeUtils.isServer(instanceId)) {
      return null;
    }
    List<String> tags = instanceConfig.getTags();
    ZNRecord record = instanceConfig.getRecord();
    boolean helixEnabled = instanceConfig.getInstanceEnabled();
    boolean queriesDisabled = record.getBooleanField(CommonConstants.Helix.QUERIES_DISABLED, false);
    boolean shutdownInProgress = record.getBooleanField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, false);

    return new ServerQueryInfo(instanceId, tags, null, helixEnabled, queriesDisabled, shutdownInProgress);
  }

  public static class ServerQueryInfo {
    private final String _instanceName;
    private final List<String> _tags;
    private final List<String> _tables;
    private final boolean _helixEnabled;
    private final boolean _queriesDisabled;
    private final boolean _shutdownInProgress;

    private ServerQueryInfo(String instanceName, List<String> tags, List<String> tables, boolean helixEnabled,
        boolean queriesDisabled, boolean shutdownInProgress) {
      _instanceName = instanceName;
      _tags = tags;
      _tables = tables;
      _helixEnabled = helixEnabled;
      _queriesDisabled = queriesDisabled;
      _shutdownInProgress = shutdownInProgress;
    }

    public boolean isHelixEnabled() {
      return _helixEnabled;
    }

    public boolean isQueriesDisabled() {
      return _queriesDisabled;
    }

    public boolean isShutdownInProgress() {
      return _shutdownInProgress;
    }
  }
}
