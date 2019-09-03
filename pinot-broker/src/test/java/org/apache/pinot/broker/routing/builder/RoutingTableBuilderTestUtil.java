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
package org.apache.pinot.broker.routing.builder;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.config.RoutingConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.utils.CommonConstants.Helix.TableType;


/**
 * Util class for routing table builder tests
 */
public class RoutingTableBuilderTestUtil {
  private RoutingTableBuilderTestUtil() {
  }

  public static TableConfig getDynamicComputingTableConfig(String tableName) {
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    RoutingConfig routingConfig = new RoutingConfig();
    Map<String, String> routingTableBuilderOptions = new HashMap<>();
    routingTableBuilderOptions.put(RoutingConfig.ENABLE_DYNAMIC_COMPUTING_KEY, "true");
    routingConfig.setRoutingTableBuilderOptions(routingTableBuilderOptions);
    return new TableConfig.Builder(tableType).setTableName(tableName).setRoutingConfig(routingConfig).build();
  }
}
