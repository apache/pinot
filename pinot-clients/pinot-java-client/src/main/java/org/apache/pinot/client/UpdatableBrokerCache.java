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

import java.util.List;


/**
 * A updatable cache of table name to list of eligible brokers.
 * Implementations should implement manual refreshing mechanism
 */
public interface UpdatableBrokerCache {
  /**
   * Initializes the cache
   * @throws Exception
   */
  void init()
      throws Exception;

  /**
   * Method to get one random broker for a given table
   * @param tableName
   * @return Broker address corresponding to the table
   */
  String getBroker(String... tableName);

  /**
   * Returns all the brokers currently in the cache
   * @return List of all avaliable brokers
   */
  List<String> getBrokers();

  /**
   * Manually trigger a cache refresh
   * @throws Exception
   */
  void triggerBrokerCacheUpdate()
      throws Exception;

  /**
   * Closes the cache and release any resources
   */
  void close();
}
