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

import com.google.common.annotations.VisibleForTesting;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;


/**
 * Creates connections to Pinot, given various initialization methods.
 */
public class ConnectionFactory {
  private static PinotClientTransport _defaultTransport;

  private ConnectionFactory() {
  }

  /**
   * Creates a connection to a Pinot cluster, given its Zookeeper URL
   *
   * @param zkUrl The URL to the Zookeeper cluster, must include the cluster name e.g host:port/chroot/pinot-cluster
   * @return A connection that connects to the brokers in the given Helix cluster
   */
  public static Connection fromZookeeper(String zkUrl) {
    return fromZookeeper(zkUrl, getDefault());
  }

  /**
   * Creates a connection to a Pinot cluster, given its Zookeeper URL
   *
   * @param zkUrl The URL to the Zookeeper cluster, must include the cluster name e.g host:port/chroot/pinot-cluster
   * @param transport pinot transport
   * @return A connection that connects to the brokers in the given Helix cluster
   */
  public static Connection fromZookeeper(String zkUrl, PinotClientTransport transport) {
    return fromZookeeper(new DynamicBrokerSelector(zkUrl), transport);
  }

  @VisibleForTesting
  static Connection fromZookeeper(DynamicBrokerSelector dynamicBrokerSelector, PinotClientTransport transport) {
    try {
      return new Connection(dynamicBrokerSelector, transport);
    } catch (Exception e) {
      throw new PinotClientException(e);
    }
  }

  /**
   * Creates a connection from properties containing the connection parameters.
   *
   * @param properties The properties to use for the connection
   * @return A connection that connects to the brokers specified in the properties
   */
  public static Connection fromProperties(Properties properties) {
    return fromProperties(properties, getDefault());
  }

  /**
   * Creates a connection from properties containing the connection parameters.
   *
   * @param properties The properties to use for the connection
   * @param transport pinot transport
   * @return A connection that connects to the brokers specified in the properties
   */
  public static Connection fromProperties(Properties properties, PinotClientTransport transport) {
    return new Connection(Arrays.asList(properties.getProperty("brokerList").split(",")), transport);
  }

  /**
   * Creates a connection which sends queries randomly between the specified brokers.
   *
   * @param brokers The list of brokers to send queries to
   * @return A connection to the set of brokers specified
   */
  public static Connection fromHostList(String... brokers) {
    return fromHostList(Arrays.asList(brokers), getDefault());
  }

  /**
   * Creates a connection which sends queries randomly between the specified brokers.
   *
   * @param brokers The list of brokers to send queries to
   * @param transport pinot transport
   * @return A connection to the set of brokers specified
   */
  public static Connection fromHostList(List<String> brokers, PinotClientTransport transport) {
    return new Connection(brokers, transport);
  }

  private static PinotClientTransport getDefault() {
    if (_defaultTransport == null) {
      _defaultTransport = new JsonAsyncHttpPinotClientTransportFactory().buildTransport();
    }
    return _defaultTransport;
  }
}
