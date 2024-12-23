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
  private PinotHelixResourceManager _pinotHelixResourceManager;
  private Map<String, ServerQueryInfo> _cache;

  public ServerQueryInfoFetcher(PinotHelixResourceManager pinotHelixResourceManager) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _cache = new HashMap<>();
  }

  public ServerQueryInfo getServerQueryInfo(String instanceId) {
    return _cache.computeIfAbsent(instanceId, this::getServerQueryInfoOndemand);
  }

  private ServerQueryInfo getServerQueryInfoOndemand(String instanceId) {
    InstanceConfig instanceConfig = _pinotHelixResourceManager.getHelixInstanceConfig(instanceId);
    if (instanceConfig == null || !InstanceTypeUtils.isServer(instanceId)) {
      return null;
    }
    List<String> tags = instanceConfig.getTags();
    ZNRecord record = instanceConfig.getRecord();
    boolean helixEnabled = record.getBooleanField(
        InstanceConfig.InstanceConfigProperty.HELIX_ENABLED.name(), false);
    boolean queriesDisabled = record.getBooleanField(CommonConstants.Helix.QUERIES_DISABLED, false);
    boolean shutdownInProgress = record.getBooleanField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, false);
    return new ServerQueryInfo(instanceId, tags, null, helixEnabled, queriesDisabled, shutdownInProgress);
  }

  public static class ServerQueryInfo {
    private String _instanceName;
    private List<String> _tags;
    private List<String> _tables;
    private boolean _helixEnabled;
    private boolean _queriesDisabled;
    private boolean _shutdownInProgress;
    private ServerQueryInfo(String instanceName,
        List<String> tags,
        List<String> tables,
        boolean helixEnabled,
        boolean queriesDisabled,
        boolean shutdownInProgress) {
      _instanceName = instanceName;
      _tags = tags;
      _tables = tables;
      _helixEnabled = helixEnabled;
      _queriesDisabled = queriesDisabled;
      _shutdownInProgress = shutdownInProgress;
    }

    public String getInstanceName() {
      return _instanceName;
    }

    public void setInstanceName(String instanceName) {
      _instanceName = instanceName;
    }

    public List<String> getTags() {
      return _tags;
    }

    public void setTags(List<String> tags) {
      _tags = tags;
    }

    public List<String> getTables() {
      return _tables;
    }

    public void setTables(List<String> tables) {
      _tables = tables;
    }

    public boolean isHelixEnabled() {
      return _helixEnabled;
    }

    public void setHelixEnabled(boolean helixEnabled) {
      _helixEnabled = helixEnabled;
    }

    public boolean isQueriesDisabled() {
      return _queriesDisabled;
    }

    public void setQueriesDisabled(boolean queriesDisabled) {
      _queriesDisabled = queriesDisabled;
    }

    public boolean isShutdownInProgress() {
      return _shutdownInProgress;
    }

    public void setShutdownInProgress(boolean shutdownInProgress) {
      _shutdownInProgress = shutdownInProgress;
    }
  }
}
