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

package com.linkedin.thirdeye.datasource.pinot;

import com.linkedin.pinot.client.Connection;
import com.linkedin.pinot.client.ConnectionFactory;
import com.linkedin.pinot.client.PinotClientException;
import com.linkedin.pinot.client.ResultSet;
import com.linkedin.pinot.client.ResultSetGroup;
import com.linkedin.thirdeye.datasource.pinot.resultset.ThirdEyeResultSetGroup;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.InstanceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PinotControllerResponseCacheLoader extends PinotResponseCacheLoader {
  private static final Logger LOG = LoggerFactory.getLogger(PinotControllerResponseCacheLoader.class);

  private static final long CONNECTION_TIMEOUT = 60000;

  private static int MAX_CONNECTIONS;
  static {
    try {
      MAX_CONNECTIONS = Integer.parseInt(System.getProperty("max_pinot_connections", "25"));
    } catch (Exception e) {
      MAX_CONNECTIONS = 25;
    }
  }
  private Connection[] connections;

  private static final String BROKER_PREFIX = "Broker_";

  private final AtomicInteger activeConnections = new AtomicInteger();

  /**
   * Constructs a empty {@link PinotControllerResponseCacheLoader}. Please use init() to setup the connection of the
   * constructed cache loader.
   */
  public PinotControllerResponseCacheLoader() { }

  /**
   * Constructs a {@link PinotControllerResponseCacheLoader} using the given data source config.
   *
   * @param pinotThirdEyeDataSourceConfig the data source config that provides controller's information.
   *
   * @throws Exception when an error occurs connecting to the Pinot controller.
   */
  public PinotControllerResponseCacheLoader(PinotThirdEyeDataSourceConfig pinotThirdEyeDataSourceConfig)
      throws Exception {
    this.init(pinotThirdEyeDataSourceConfig);
  }

  /**
   * Initializes the cache loader using the given property map.
   *
   * @param properties the property map that provides controller's information.
   *
   * @throws Exception when an error occurs connecting to the Pinot controller.
   */
  public void init(Map<String, Object> properties) throws Exception {
    PinotThirdEyeDataSourceConfig dataSourceConfig = PinotThirdEyeDataSourceConfig.createFromProperties(properties);
    this.init(dataSourceConfig);
  }

  /**
   * Initializes the cache loader using the given data source config.
   *
   * @param pinotThirdEyeDataSourceConfig the data source config that provides controller's information.
   *
   * @throws Exception when an error occurs connecting to the Pinot controller.
   */
  private void init(PinotThirdEyeDataSourceConfig pinotThirdEyeDataSourceConfig) throws Exception {
    if (pinotThirdEyeDataSourceConfig.getBrokerUrl() != null
        && pinotThirdEyeDataSourceConfig.getBrokerUrl().trim().length() > 0) {
      ZkClient zkClient = new ZkClient(pinotThirdEyeDataSourceConfig.getZookeeperUrl());
      zkClient.setZkSerializer(new ZNRecordSerializer());
      zkClient.waitUntilConnected(CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS);
      ZKHelixAdmin helixAdmin = new ZKHelixAdmin(zkClient);
      List<String> thirdeyeBrokerList = helixAdmin.getInstancesInClusterWithTag(
          pinotThirdEyeDataSourceConfig.getClusterName(), pinotThirdEyeDataSourceConfig.getTag());

      String[] thirdeyeBrokers = new String[thirdeyeBrokerList.size()];
      for (int i = 0; i < thirdeyeBrokerList.size(); i++) {
        String instanceName = thirdeyeBrokerList.get(i);
        InstanceConfig instanceConfig =
            helixAdmin.getInstanceConfig(pinotThirdEyeDataSourceConfig.getClusterName(), instanceName);
        thirdeyeBrokers[i] = instanceConfig.getHostName().replaceAll(BROKER_PREFIX, "") + ":"
            + instanceConfig.getPort();
      }
      this.connections = fromHostList(thirdeyeBrokers);
      LOG.info("Created PinotControllerResponseCacheLoader with brokers {}", thirdeyeBrokers);
    } else {
      this.connections = fromZookeeper(pinotThirdEyeDataSourceConfig);
      LOG.info("Created PinotControllerResponseCacheLoader with controller {}:{}",
          pinotThirdEyeDataSourceConfig.getControllerHost(), pinotThirdEyeDataSourceConfig.getControllerPort());
    }
  }

  @Override
  public ThirdEyeResultSetGroup load(PinotQuery pinotQuery) throws Exception {
    try {
      Connection connection = getConnection();
      try {
        synchronized (connection) {
          int activeConnections = this.activeConnections.incrementAndGet();

          long start = System.currentTimeMillis();
          ResultSetGroup resultSetGroup = connection.execute(pinotQuery.getTableName(), pinotQuery.getPql());
          if (LOG.isDebugEnabled()) {
            LOG.debug("Query:{}  response:{}", pinotQuery.getPql(), format(resultSetGroup));
          }
          long end = System.currentTimeMillis();
          LOG.info("Query:{}  took:{} ms  connections:{}", pinotQuery.getPql(), (end - start), activeConnections);

          return ThirdEyeResultSetGroup.fromPinotResultSetGroup(resultSetGroup);
        }
      } finally {
        this.activeConnections.decrementAndGet();
      }
    } catch (PinotClientException cause) {
      LOG.error("Error when running pql:" + pinotQuery.getPql(), cause);
      throw new PinotClientException("Error when running pql:" + pinotQuery.getPql(), cause);
    }
  }

  private Connection getConnection() {
    return connections[(int) (Thread.currentThread().getId() % MAX_CONNECTIONS)];
  }

  private static String format(ResultSetGroup result) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < result.getResultSetCount(); i++) {
      ResultSet resultSet = result.getResultSet(i);
      for (int c = 0; c < resultSet.getColumnCount(); c++) {
        sb.append(resultSet.getColumnName(c)).append("=").append(resultSet.getDouble(c));
      }
    }
    return sb.toString();
  }

  private static Connection[] fromHostList(final String[] thirdeyeBrokers) throws Exception {
    Callable<Connection> callable = new Callable<Connection>() {
        @Override
        public Connection call() throws Exception {
          return ConnectionFactory.fromHostList(thirdeyeBrokers);
        }
      };
    return fromFutures(executeReplicated(callable, MAX_CONNECTIONS));
  }

  private static Connection[] fromZookeeper(final PinotThirdEyeDataSourceConfig pinotThirdEyeDataSourceConfig) throws Exception {
    Callable<Connection> callable = new Callable<Connection>() {
        @Override
        public Connection call() throws Exception {
          return ConnectionFactory.fromZookeeper(
              pinotThirdEyeDataSourceConfig.getZookeeperUrl()
                  + "/" + pinotThirdEyeDataSourceConfig.getClusterName());
        }
      };
    return fromFutures(executeReplicated(callable, MAX_CONNECTIONS));
  }

  private static <T> Collection<Future<T>> executeReplicated(Callable<T> callable, int n) {
    ExecutorService executor = Executors.newCachedThreadPool();
    Collection<Future<T>> futures = new ArrayList<>();
    for(int i=0; i<n; i++) {
      futures.add(executor.submit(callable));
    }
    executor.shutdown();
    return futures;
  }

  private static Connection[] fromFutures(Collection<Future<Connection>> futures) throws Exception {
    Connection[] connections = new Connection[futures.size()];
    int i = 0;
    for(Future<Connection> f : futures) {
      connections[i++] = f.get(CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS);
    }
    return connections;
  }
}
