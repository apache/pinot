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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nullable;
import org.apache.pinot.client.ExternalViewReader;


public class BrokerSelectorUtils {

  private BrokerSelectorUtils() {
  }

  /**
   *
   * @param tableNames: List of table names.
   * @param brokerData: map holding data for table hosting on brokers.
   * @return list of common brokers hosting all the tables or null if no common brokers found.
   * @deprecated Use {@link #getTablesCommonBrokersSet(List, Map)} instead. It is more efficient and its semantics are
   * clearer (ie it returns an empty set instead of null if no common brokers are found).
   */
  @Nullable
  @Deprecated
  public static List<String> getTablesCommonBrokers(@Nullable List<String> tableNames,
      Map<String, List<String>> brokerData) {
    Set<String> tablesCommonBrokersSet = getTablesCommonBrokersSet(tableNames, brokerData);
    if (tablesCommonBrokersSet == null || tablesCommonBrokersSet.isEmpty()) {
      return null;
    }
    return new ArrayList<>(tablesCommonBrokersSet);
  }

  /**
   * Returns a random broker from the common brokers hosting all the tables.
   */
  @Nullable
  public static String getRandomBroker(@Nullable List<String> tableNames, Map<String, List<String>> brokerData) {
    Set<String> tablesCommonBrokersSet = getTablesCommonBrokersSet(tableNames, brokerData);
    if (tablesCommonBrokersSet.isEmpty()) {
      return null;
    }
    return tablesCommonBrokersSet.stream()
        .skip(ThreadLocalRandom.current().nextInt(tablesCommonBrokersSet.size()))
        .findFirst()
        .orElseThrow(() -> new IllegalStateException("No broker found"));
  }

  /**
   *
   * @param tableNames: List of table names.
   * @param brokerData: map holding data for table hosting on brokers.
   * @return set of common brokers hosting all the tables
   */
  public static Set<String> getTablesCommonBrokersSet(
      @Nullable List<String> tableNames, Map<String, List<String>> brokerData) {
    if (tableNames == null || tableNames.isEmpty()) {
      return Collections.emptySet();
    }
    HashSet<String> commonBrokers = getBrokers(tableNames.get(0), brokerData);
    for (int i = 1; i < tableNames.size() && !commonBrokers.isEmpty(); i++) {
      commonBrokers.retainAll(getBrokers(tableNames.get(i), brokerData));
    }

    return commonBrokers;
  }

  private static String getTableNameWithoutSuffix(String tableName) {
    return
        tableName.replace(ExternalViewReader.OFFLINE_SUFFIX, "").
            replace(ExternalViewReader.REALTIME_SUFFIX, "");
  }

  /**
   * Returns the brokers for the given table name.
   *
   * This means that an empty set is returned if there are no brokers for the given table name.
   */
  private static HashSet<String> getBrokers(String tableName, Map<String, List<String>> brokerData) {
    String tableNameWithoutSuffix = getTableNameWithoutSuffix(tableName);
    int idx = tableNameWithoutSuffix.indexOf('.');

    List<String> brokers = brokerData.get(tableNameWithoutSuffix);
    if (brokers != null) {
      return new HashSet<>(brokers);
    } else if (idx > 0) {
      // TODO: This is probably unnecessary and even wrong. `brokerData` should include the fully qualified name.
      // In case tableNameWithoutSuffix is formatted as <db>.<table> and not found in the fully qualified name
      tableNameWithoutSuffix = tableNameWithoutSuffix.substring(idx + 1);
      List<String> brokersWithoutDb = brokerData.get(tableNameWithoutSuffix);
      if (brokersWithoutDb != null) {
        return new HashSet<>(brokersWithoutDb);
      }
    }
    return new HashSet<>();
  }
}
