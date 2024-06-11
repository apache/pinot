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
package org.apache.pinot.common.broker;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;


/**
 * CommonTenantBrokerSelector selects brokers using the following rules:
 * - if no tables are specified, return a random broker.
 * - if ONE or more table are specified, then a broker serving that table is selected. If none found, null is returned.
 */
public class CommonTenantBrokerSelector extends DynamicBrokerSelector {

  public CommonTenantBrokerSelector(String zkServers, boolean preferTlsPort) {
    super(zkServers, preferTlsPort);
  }

  public CommonTenantBrokerSelector(String zkServers) {
    super(zkServers);
  }

  @Nullable
  @Override
  public BrokerInfo selectBrokerInfo(String... tableNames) {
    if (!(tableNames == null || tableNames.length == 0 || tableNames[0] == null)) {
      // getting list of brokers hosting all the tables.
      List<BrokerInfo> commonBrokers = BrokerSelectorUtils.getTablesCommonBrokers(Arrays.asList(tableNames),
          _tableToBrokerListMapRef.get());
      if (commonBrokers != null && !commonBrokers.isEmpty()) {
        // Return a broker randomly if table is null or no broker is found for the specified table.
        List<BrokerInfo> list = new ArrayList<>(commonBrokers);
        if (!list.isEmpty()) {
          return list.get(RANDOM.nextInt(list.size()));
        }
      }
    } else {
      // If no tables were specified, return a random broker.
      List<BrokerInfo> list = new ArrayList<>(_allBrokerListRef.get());
      if (!list.isEmpty()) {
        return list.get(RANDOM.nextInt(list.size()));
      }
    }

    // Return null if tables were specified and no brokers were found. Do not return a random broker unlike other
    // selectors.
    return null;
  }
}
