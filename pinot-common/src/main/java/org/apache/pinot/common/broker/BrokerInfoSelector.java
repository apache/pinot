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

import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;


public abstract class BrokerInfoSelector implements BrokerSelector {

  protected final boolean _preferTlsPort;

  public BrokerInfoSelector(boolean preferTlsPort) {
    _preferTlsPort = preferTlsPort;
  }

  @Nullable
  @Override
  public String selectBroker(String... tableNames) {
    BrokerInfo brokerInfo = selectBrokerInfo(tableNames);
    if (brokerInfo != null) {
      return brokerInfo.getHostPort(_preferTlsPort);
    }
    return null;
  }


  @Override
  public List<String> getBrokers() {
    return getBrokerInfoList().stream().map(b -> b.getHostPort(_preferTlsPort)).collect(Collectors.toList());
  }

  /**
   * Returns a BrokerInfo object with network information about the broker.
   * @param tableNames List of tables that the broker has to process.
   * @return A BrokerInfo object or null if none found.
   */
  public abstract BrokerInfo selectBrokerInfo(String... tableNames);

  /**
   * Get a list of BrokerInfo objects.
   * @return A list of BrokerInfo objects
   */
  public abstract List<BrokerInfo> getBrokerInfoList();
}
