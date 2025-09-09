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
import org.apache.pinot.client.cursor.CursorConnection;
import org.apache.pinot.client.cursor.CursorConnectionImpl;
import org.apache.pinot.client.grpc.GrpcConnection;


/**
 * Creates connections to Pinot, given various initialization methods.
 */
public class ConnectionFactory {
  private static volatile PinotClientTransport _defaultTransport;

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
    try {
      return fromZookeeper(new DynamicBrokerSelector(zkUrl), transport);
    } catch (Exception e) {
      throw new PinotClientException(e);
    }
  }

  /**
   * Creates a connection to a Pinot cluster, given its Zookeeper URL and properties.
   *
   * @param properties connection properties
   * @param zkUrl zookeeper URL.
   * @return A connection that connects to the brokers in the given Helix cluster
   */
  public static Connection fromZookeeper(Properties properties, String zkUrl) {
    try {
      return fromZookeeper(properties, new DynamicBrokerSelector(zkUrl), getDefault(properties));
    } catch (Exception e) {
      throw new PinotClientException(e);
    }
  }

  /**
   *
   * @param scheme controller URL scheme
   * @param controllerHost controller host
   * @param controllerPort controller port
   * @return A connection that connects to brokers as per the given controller
   */
  @Deprecated
  public static Connection fromController(String scheme, String controllerHost, int controllerPort) {
    return fromController(new Properties(), scheme, controllerHost, controllerPort);
  }

  /**
   *
   * @param scheme controller URL scheme
   * @param controllerHost controller host
   * @param controllerPort controller port
   * @return A connection that connects to brokers as per the given controller
   */
  @Deprecated
  public static Connection fromController(Properties properties, String scheme,
      String controllerHost, int controllerPort) {
    try {
      return new Connection(properties,
          new ControllerBasedBrokerSelector(scheme, controllerHost, controllerPort, properties),
          getDefault());
    } catch (Exception e) {
      throw new PinotClientException(e);
    }
  }

  /**
   * @param controllerUrl url host:port of the controller
   * @return A connection that connects to brokers as per the given controller
   */
  public static Connection fromController(String controllerUrl) {
    return fromController(new Properties(), controllerUrl);
  }

  /**
   * @param properties
   * @param controllerUrl url host:port of the controller
   * @return A connection that connects to brokers as per the given controller
   */
  public static Connection fromController(Properties properties, String controllerUrl) {
    try {
      return new Connection(properties,
          new ControllerBasedBrokerSelector(properties, controllerUrl),
          getDefault(properties));
    } catch (Exception e) {
      throw new PinotClientException(e);
    }
  }

  /**
   * @param properties
   * @param controllerUrl url host:port of the controller
   * @param transport pinot transport
   * @return A connection that connects to brokers as per the given controller
   */
  public static Connection fromController(Properties properties, String controllerUrl, PinotClientTransport transport) {
    try {
      return new Connection(properties,
          new ControllerBasedBrokerSelector(properties, controllerUrl),
          transport);
    } catch (Exception e) {
      throw new PinotClientException(e);
    }
  }

  /**
   * @param properties
   * @param controllerUrl url host:port of the controller
   * @return A connection that connects to brokers as per the given controller
   */
  public static GrpcConnection fromControllerGrpc(Properties properties, String controllerUrl) {
    try {
      properties.setProperty("useGrpcPort", "true");
      return new GrpcConnection(properties, new ControllerBasedBrokerSelector(properties, controllerUrl));
    } catch (Exception e) {
      throw new PinotClientException(e);
    }
  }

  /**
   * Creates a connection to a Pinot cluster, given its Zookeeper URL
   *
   * @param properties The Pinot connection properties
   * @param zkUrl The URL to the Zookeeper cluster, must include the cluster name e.g host:port/chroot/pinot-cluster
   * @param transport pinot transport
   * @return A connection that connects to the brokers in the given Helix cluster
   */
  public static Connection fromZookeeper(Properties properties, String zkUrl, PinotClientTransport transport) {
    try {
      boolean preferTls = Boolean.parseBoolean(properties.getProperty("preferTLS", "false"));
      boolean useGrpcPort = Boolean.parseBoolean(properties.getProperty("useGrpcPort", "false"));
      return fromZookeeper(properties, new DynamicBrokerSelector(zkUrl, preferTls, useGrpcPort), transport);
    } catch (Exception e) {
      throw new PinotClientException(e);
    }
  }

  @VisibleForTesting
  static Connection fromZookeeper(DynamicBrokerSelector dynamicBrokerSelector, PinotClientTransport transport) {
    return fromZookeeper(new Properties(), dynamicBrokerSelector, transport);
  }

  @VisibleForTesting
  static Connection fromZookeeper(Properties properties, DynamicBrokerSelector dynamicBrokerSelector,
      PinotClientTransport transport) {
    return new Connection(properties, dynamicBrokerSelector, transport);
  }

  /**
   * Creates a connection to a Pinot cluster, given its Zookeeper URL
   *
   * @param properties The Pinot connection properties
   * @param zkUrl The URL to the Zookeeper cluster, must include the cluster name e.g host:port/chroot/pinot-cluster
   * @return A connection that connects to the brokers in the given Helix cluster
   */
  public static GrpcConnection fromZookeeperGrpc(Properties properties, String zkUrl) {
    try {
      return new GrpcConnection(properties, new DynamicBrokerSelector(zkUrl, false, true));
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
    return fromProperties(properties, getDefault(properties));
  }

  /**
   * Creates a connection from properties containing the connection parameters.
   *
   * @param properties The properties to use for the connection
   * @param transport pinot transport
   * @return A connection that connects to the brokers specified in the properties
   */
  public static Connection fromProperties(Properties properties, PinotClientTransport transport) {
    return new Connection(properties, Arrays.asList(properties.getProperty("brokerList").split(",")), transport);
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

  /**
   * Creates a connection which sends queries randomly between the specified brokers.
   *
   * @param properties The Pinot connection properties
   * @param brokers The list of brokers to send queries to
   * @param transport pinot transport
   * @return A connection to the set of brokers specified
   */
  public static Connection fromHostList(Properties properties, List<String> brokers,
      PinotClientTransport transport) {
    return new Connection(properties, brokers, transport);
  }

  public static GrpcConnection fromHostListGrpc(Properties properties, List<String> brokers) {
    return new GrpcConnection(properties, brokers);
  }

  private static PinotClientTransport getDefault(Properties connectionProperties) {
    // TODO: This code incorrectly assumes that connection properties are always the same
    if (_defaultTransport == null) {
      synchronized (ConnectionFactory.class) {
        if (_defaultTransport == null) {
          _defaultTransport = new JsonAsyncHttpPinotClientTransportFactory()
              .withConnectionProperties(connectionProperties)
              .buildTransport();
        }
      }
    }
    return _defaultTransport;
  }

  private static PinotClientTransport getDefault() {
    return getDefault(new Properties());
  }

  /**
   * Creates a cursor connection to a Pinot cluster, given its Zookeeper URL.
   *
   * @param zkUrl The URL to the Zookeeper cluster, must include the cluster name e.g host:port/chroot/pinot-cluster
   * @return A cursor connection that connects to the brokers in the given Helix cluster
   */
  public static CursorConnection createCursorConnection(String zkUrl) {
    Connection connection = fromZookeeper(zkUrl);
    return new CursorConnectionImpl(connection);
  }

  /**
   * Creates a cursor connection to a Pinot cluster, given its Zookeeper URL and properties.
   *
   * @param properties The Pinot connection properties
   * @param zkUrl The URL to the Zookeeper cluster, must include the cluster name e.g host:port/chroot/pinot-cluster
   * @return A cursor connection that connects to the brokers in the given Helix cluster
   */
  public static CursorConnection createCursorConnection(Properties properties, String zkUrl) {
    Connection connection = fromZookeeper(properties, zkUrl);
    return new CursorConnectionImpl(connection);
  }

  /**
   * Creates a cursor connection to a Pinot cluster, given its Zookeeper URL and transport.
   *
   * @param zkUrl The URL to the Zookeeper cluster, must include the cluster name e.g host:port/chroot/pinot-cluster
   * @param helixClusterName The name of the Helix cluster
   * @param transport pinot transport
   * @return A cursor connection that connects to the brokers in the given Helix cluster
   * @throws IllegalArgumentException If transport is not JsonAsyncHttpPinotClientTransport
   */
  public static CursorConnection createCursorConnection(String zkUrl, String helixClusterName,
      PinotClientTransport transport) {
    Connection connection = fromZookeeper(zkUrl, transport);
    return new CursorConnectionImpl(connection);
  }

  /**
   * Creates a cursor connection to a Pinot cluster, given its Zookeeper URL, properties and transport.
   *
   * @param properties The Pinot connection properties
   * @param zkUrl The URL to the Zookeeper cluster, must include the cluster name e.g host:port/chroot/pinot-cluster
   * @param helixClusterName The name of the Helix cluster
   * @param transport pinot transport
   * @return A cursor connection that connects to the brokers in the given Helix cluster
   * @throws IllegalArgumentException If transport is not JsonAsyncHttpPinotClientTransport
   */
  public static CursorConnection createCursorConnection(Properties properties, String zkUrl,
      String helixClusterName, PinotClientTransport transport) {
    Connection connection = fromZookeeper(properties, zkUrl, transport);
    return new CursorConnectionImpl(connection);
  }

  /**
   * Creates a cursor connection to a Pinot cluster, given its Zookeeper URL.
   *
   * @param zkServers The list of Zookeeper servers
   * @param zkPath The Zookeeper path
   * @param tag
   * @return A cursor connection that connects to the brokers in the given Helix cluster
   */
  public static CursorConnection createCursorConnection(List<String> zkServers, String zkPath,
      String tag) {
    String zkUrl = String.join(",", zkServers) + zkPath;
    Connection connection = fromZookeeper(new Properties(), zkUrl, getDefault());
    return new CursorConnectionImpl(connection);
  }

  /**
   * Creates a cursor connection to a Pinot cluster, given its controller URL.
   *
   * @param controllerUrl url host:port of the controller
   * @param tag
   * @return A cursor connection that connects to brokers as per the given controller
   */
  public static CursorConnection createCursorConnectionFromController(String controllerUrl, String tag) {
    return createCursorConnectionFromController(new Properties(), controllerUrl, tag, getDefault());
  }

  /**
   * Creates a cursor connection to a Pinot cluster, given its controller URL and properties.
   *
   * @param controllerUrl url host:port of the controller
   * @param tag
   * @param properties The Pinot connection properties
   * @return A cursor connection that connects to brokers as per the given controller
   */
  public static CursorConnection createCursorConnectionFromController(String controllerUrl, String tag,
      Properties properties) {
    return createCursorConnectionFromController(properties, controllerUrl, tag, getDefault(properties));
  }

  /**
   * Creates a cursor connection to a Pinot cluster, given its controller URL and properties.
   *
   * @param properties The Pinot connection properties
   * @param controllerUrl url host:port of the controller
   * @param tag
   * @return A cursor connection that connects to brokers as per the given controller
   */
  public static CursorConnection createCursorConnectionFromController(Properties properties, String controllerUrl,
      String tag) {
    Connection connection = fromController(properties, controllerUrl);
    return new CursorConnectionImpl(connection);
  }

  /**
   * Creates a cursor connection to a Pinot cluster, given its controller URL, properties and transport.
   *
   * @param controllerUrl url host:port of the controller
   * @param tag
   * @param properties The Pinot connection properties
   * @param transportFactory pinot transport factory
   * @return A cursor connection that connects to brokers as per the given controller
   * @throws IllegalArgumentException If transport is not JsonAsyncHttpPinotClientTransport
   */
  public static CursorConnection createCursorConnectionFromController(String controllerUrl, String tag,
      Properties properties, PinotClientTransportFactory transportFactory) {
    PinotClientTransport transport = transportFactory != null ? transportFactory.buildTransport()
        : getDefault(properties);
    Connection connection = fromController(properties, controllerUrl, transport);
    return new CursorConnectionImpl(connection);
  }

  /**
   * Creates a cursor connection to a Pinot cluster, given its controller URL, properties and transport.
   *
   * @param properties The Pinot connection properties
   * @param controllerUrl url host:port of the controller
   * @param tag
   * @param transport pinot transport
   * @return A cursor connection that connects to brokers as per the given controller
   * @throws IllegalArgumentException If transport is not JsonAsyncHttpPinotClientTransport
   */
  public static CursorConnection createCursorConnectionFromController(Properties properties,
      String controllerUrl, String tag, PinotClientTransport transport) {
    Connection connection = fromController(properties, controllerUrl, transport);
    return new CursorConnectionImpl(connection);
  }

  /**
   * Creates a cursor connection from properties containing the connection parameters.
   *
   * @param properties The properties to use for the connection
   * @return A cursor connection that connects to the brokers specified in the properties
   */
  public static CursorConnection createCursorConnectionFromProperties(Properties properties) {
    Connection connection = fromProperties(properties);
    return new CursorConnectionImpl(connection);
  }

  /**
   * Creates a cursor connection from properties containing the connection parameters.
   *
   * @param properties The properties to use for the connection
   * @param transport pinot transport
   * @return A cursor connection that connects to the brokers specified in the properties
   */
  public static CursorConnection createCursorConnectionFromProperties(Properties properties,
      PinotClientTransport transport) {
    Connection connection = fromProperties(properties, transport);
    return new CursorConnectionImpl(connection);
  }

  /**
   * Creates a cursor connection which sends queries randomly between the specified brokers.
   *
   * @param brokers The list of brokers to send queries to
   * @return A cursor connection to the set of brokers specified
   */
  public static CursorConnection createCursorConnectionFromHostList(String... brokers) {
    Connection connection = fromHostList(brokers);
    return new CursorConnectionImpl(connection);
  }

  /**
   * Creates a cursor connection which sends queries randomly between the specified brokers.
   *
   * @param brokers The list of brokers to send queries to
   * @return A cursor connection to the set of brokers specified
   */
  public static CursorConnection createCursorConnectionFromHostList(List<String> brokers) {
    Connection connection = fromHostList(brokers, getDefault());
    return new CursorConnectionImpl(connection);
  }

  /**
   * Creates a cursor connection which sends queries randomly between the specified brokers.
   *
   * @param brokers The list of brokers to send queries to
   * @param transport pinot transport
   * @return A cursor connection to the set of brokers specified
   */
  public static CursorConnection createCursorConnectionFromHostList(List<String> brokers,
      PinotClientTransport transport) {
    Connection connection = fromHostList(brokers, transport);
    return new CursorConnectionImpl(connection);
  }

  /**
   * Creates a cursor connection which sends queries randomly between the specified brokers.
   *
   * @param properties The Pinot connection properties
   * @param brokers The list of brokers to send queries to
   * @param transport pinot transport
   * @return A cursor connection to the set of brokers specified
   */
  public static CursorConnection createCursorConnectionFromHostList(Properties properties,
      List<String> brokers, PinotClientTransport transport) {
    Connection connection = fromHostList(properties, brokers, transport);
    return new CursorConnectionImpl(connection);
  }

  /**
   * Creates a cursor connection wrapping the provided connection.
   *
   * @param connection the connection to wrap
   * @return A cursor connection wrapping the provided connection
   * @throws IllegalArgumentException If the connection's transport is not JsonAsyncHttpPinotClientTransport
   */
  public static CursorConnection createCursorConnection(Connection connection) {
    return new CursorConnectionImpl(connection);
  }
}
