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
package org.apache.pinot.client.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.pinot.client.ExternalViewReader;


public class BrokerSelectorUtils {

  private BrokerSelectorUtils() {
  }

  /**
   *
   * @param tableNames: List of table names.
   * @param brokerData: map holding data for table hosting on brokers.
   * @return list of common brokers hosting all the tables.
   */
  public static List<String> getTablesCommonBrokers(List<String> tableNames, Map<String, List<String>> brokerData) {
    List<List<String>> tablesBrokersList = new ArrayList<>();
    for (String name: tableNames) {
      String tableName = getTableNameWithoutSuffix(name);
      int idx = tableName.indexOf('.');

      if (brokerData.containsKey(tableName)) {
        tablesBrokersList.add(brokerData.get(tableName));
      } else if (idx > 0) {
        // In case tableName is formatted as <db>.<table>
        tableName = tableName.substring(idx + 1);
        tablesBrokersList.add(brokerData.get(tableName));
      }
    }

    // return null if tablesBrokersList is empty or contains null
    if (tablesBrokersList.isEmpty()
        || tablesBrokersList.stream().anyMatch(Objects::isNull)) {
      return null;
    }

    List<String> commonBrokers = tablesBrokersList.get(0);
    for (int i = 1; i < tablesBrokersList.size(); i++) {
      commonBrokers.retainAll(tablesBrokersList.get(i));
    }
    return commonBrokers;
  }

  private static String getTableNameWithoutSuffix(String tableName) {
    return
        tableName.replace(ExternalViewReader.OFFLINE_SUFFIX, "").
            replace(ExternalViewReader.REALTIME_SUFFIX, "");
  }
}
