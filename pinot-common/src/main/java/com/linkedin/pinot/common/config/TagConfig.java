/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

/**
 * Abstract base class for tag configs
 */
public class TagConfig {

  TableConfig _tableConfig;
  String _serverTenant;

  public TagConfig(TableConfig tableConfig) {

    _tableConfig = tableConfig;
    _serverTenant = tableConfig.getTenantConfig().getServer();
  }

  public TableConfig getTableConfig() {
    return _tableConfig;
  }

  public String getServerTenantName() {
    return _serverTenant;
  }

}

