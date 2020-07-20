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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * Creates connections to Pinot, given various initialization methods.
 */
public class ConnectionFactory {
  static PinotClientTransportFactory _transportFactory = new JsonAsyncHttpPinotClientTransportFactory();

  private ConnectionFactory() {
  }

  /**
   * Creates a connection to a Pinot cluster, given its Zookeeper URL
   *
   * @param zkUrl The URL to the Zookeeper cluster, must include the cluster name e.g host:port/chroot/pinot-cluster
   * @return A connection that connects to the brokers in the given Helix cluster
   */
  public static Connection fromZookeeper(String zkUrl) {
    try {
      DynamicBrokerSelector dynamicBrokerSelector = new DynamicBrokerSelector(zkUrl);
      return new Connection(dynamicBrokerSelector, _transportFactory.buildTransport(null));
    } catch (Exception e) {
      throw new PinotClientException(e);
    }
  }

  /**
   * Creates a connection to a Pinot cluster, given its Controller URL
   * Please note that this client requires Pinot Controller supports getBroker APIs,
   * which is supported from Pinot 0.5.0.
   *
   * @param controllerUrls The comma separated URLs to Pinot Controller, suggest to use Vip hostname or k8s service.
   *                      E.g. http://pinot-controller:9000
   *                      http://pinot-controller-0:9000,http://pinot-controller-1:9000,http://pinot-controller-2:9000
   * @return A connection that connects to the brokers in the given Pinot cluster
   */
  public static Connection fromController(String controllerUrls) {
    try {
      BrokerSelector brokerSelector = new ControllerBasedBrokerSelector(controllerUrls);
      return new Connection(brokerSelector, _transportFactory.buildTransport(null));
    } catch (Exception e) {
      throw new PinotClientException(e);
    }
  }

  /**
   * Creates a connection to a Pinot cluster, given its Controller URL
   * Please note that this client requires Pinot Controller supports getBroker APIs,
   * which is supported from Pinot 0.5.0.
   *
   * @param controllerUrls The comma separated URLs to Pinot Controller, suggest to use Vip hostname or k8s service.
   *                      E.g. http://pinot-controller:9000
   *                      http://pinot-controller-0:9000,http://pinot-controller-1:9000,http://pinot-controller-2:9000
   * @param controllerFetchRetries Retries before fail a broker info calls to controller.
   * @param controllerFetchRetriesIntervalMills Retry interval of broker info calls to controller.
   * @param controllerFetchScheduleIntervalMills Scheduler interval of broker info calls.
   * @return A connection that connects to the brokers in the given Pinot cluster
   */
  public static Connection fromController(String controllerUrls, int controllerFetchRetries,
      long controllerFetchRetriesIntervalMills, long controllerFetchScheduleIntervalMills) {
    try {
      BrokerSelector brokerSelector =
          new ControllerBasedBrokerSelector(controllerUrls, controllerFetchRetries, controllerFetchRetriesIntervalMills,
              controllerFetchScheduleIntervalMills);
      return new Connection(brokerSelector, _transportFactory.buildTransport(null));
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
    return new Connection(Arrays.asList(properties.getProperty("brokerList").split(",")),
        _transportFactory.buildTransport(null));
  }

  /**
   * Creates a connection which sends queries randomly between the specified brokers.
   *
   * @param brokers The list of brokers to send queries to
   * @return A connection to the set of brokers specified
   */
  public static Connection fromHostList(String... brokers) {
    return new Connection(Arrays.asList(brokers), _transportFactory.buildTransport(null));
  }

  /**
   * Creates a connection which sends queries randomly between the specified brokers.
   *
   * @param brokers The list of brokers to send queries to
   * @param headers Map of key and values of header which need to be used during http call
   * @return A connection to the set of brokers specified
   */
  public static Connection fromHostList(List<String> brokers, Map<String, String> headers) {
    return new Connection(brokers, _transportFactory.buildTransport(headers));
  }
}
