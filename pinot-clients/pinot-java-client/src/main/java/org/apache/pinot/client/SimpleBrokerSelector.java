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
package org.apache.pinot.client;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Random;


/**
 * Picks a broker randomly from list of brokers provided. This assumes that all the provided brokers
 * are healthy. There is no health check done on the brokers
 */
public class SimpleBrokerSelector implements BrokerSelector {

  private final List<String> _brokerList;
  private final Random _random = new Random();

  public SimpleBrokerSelector(List<String> brokerList) {
    _brokerList = ImmutableList.copyOf(brokerList);
  }

  @Override
  public String selectBroker(String... tableNames) {
    return _brokerList.get(_random.nextInt(_brokerList.size()));
  }

  @Override
  public List<String> getBrokers() {
    return ImmutableList.copyOf(_brokerList);
  }

  @Override
  public void close() {
  }
}
