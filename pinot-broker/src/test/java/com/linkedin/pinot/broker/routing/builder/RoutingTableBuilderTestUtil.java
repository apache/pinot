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
package com.linkedin.pinot.broker.routing.builder;

import com.linkedin.pinot.common.config.RoutingConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.utils.CommonConstants;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


/**
 * Util class for routing table builder tests
 */
public class RoutingTableBuilderTestUtil {

  private RoutingTableBuilderTestUtil() {
  }

  public static TableConfig getDynamicComputingTableConfig(String tableName) throws IOException {
    CommonConstants.Helix.TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    TableConfig tableConfig = new TableConfig.Builder(tableType).setTableName(tableName).build();
    Map<String, String> option = new HashMap<>();
    option.put(RoutingConfig.ENABLE_DYNAMIC_COMPUTING_KEY, "true");
    tableConfig.getRoutingConfig().setRoutingTableBuilderOptions(option);
    return tableConfig;
  }
}
