/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.config;

import org.apache.helix.HelixManager;


/**
 * Abstract base class for tag configs
 */
public class TagConfig {

  TableConfig _tableConfig;

  String _serverTenant;

  public TagConfig(TableConfig tableConfig, HelixManager helixManager) {

    _tableConfig = tableConfig;

    // TODO: we will introduce TENANTS config in property store, which should return the server tags
    // once we have that, below code will change to fetching TENANT from property store and returning the consuming/completed values
    _serverTenant = tableConfig.getTenantConfig().getServer();
  }

  public TableConfig getTableConfig() {
    return _tableConfig;
  }

  public String getServerTenantName() {
    return _serverTenant;
  }

}

