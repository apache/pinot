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
package org.apache.pinot.spi.utils.builder;

import java.util.Map;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.PhysicalTableConfig;


public class LogicalTableConfigBuilder {
  private String _tableName;
  private Map<String, PhysicalTableConfig> _physicalTableConfigMap;
  private String _brokerTenant;

  public LogicalTableConfigBuilder setTableName(String tableName) {
    _tableName = tableName;
    return this;
  }

  public LogicalTableConfigBuilder setPhysicalTableConfigMap(Map<String, PhysicalTableConfig> physicalTableConfigMap) {
    _physicalTableConfigMap = physicalTableConfigMap;
    return this;
  }

  public LogicalTableConfigBuilder setBrokerTenant(String brokerTenant) {
    _brokerTenant = brokerTenant;
    return this;
  }

  public LogicalTableConfig build() {
    LogicalTableConfig logicalTableConfig = new LogicalTableConfig();
    logicalTableConfig.setTableName(_tableName);
    logicalTableConfig.setPhysicalTableConfigMap(_physicalTableConfigMap);
    logicalTableConfig.setBrokerTenant(_brokerTenant);
    return logicalTableConfig;
  }
}
