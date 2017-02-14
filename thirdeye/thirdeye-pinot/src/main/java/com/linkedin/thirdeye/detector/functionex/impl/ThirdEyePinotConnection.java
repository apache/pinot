package com.linkedin.thirdeye.detector.functionex.impl;

import com.linkedin.pinot.client.Connection;
import com.linkedin.pinot.client.ConnectionFactory;
import com.linkedin.pinot.client.ResultSetGroup;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClientConfig;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.InstanceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * duplicates com.linkedin.thirdeye.client.cache.ResultSetGroupCacheLoader
 *
 * Allows testing pinot components without standing up all of ThirdEye
 */
public class ThirdEyePinotConnection {
  static final Logger LOG = LoggerFactory.getLogger(ThirdEyePinotConnection.class);

  static final String BROKER_PREFIX = "Broker_";

  static final int CONNECTION_TIMEOUT = 1000;
  static final int MAX_CONNECTIONS;
  static {
    int MAX_CONNECTIONS_DEFAULT = 25;
    try {
      MAX_CONNECTIONS_DEFAULT = Integer.parseInt(System.getProperty("max_pinot_connections", "25"));
    } catch (Exception e) {
      // left blank
    }
    MAX_CONNECTIONS = MAX_CONNECTIONS_DEFAULT;
  }

  PinotThirdEyeClientConfig config;
  BlockingQueue<Connection> connections;

  // TODO add async query execution

  public ThirdEyePinotConnection(PinotThirdEyeClientConfig config) {
    this(config, MAX_CONNECTIONS);
  }

  public ThirdEyePinotConnection(PinotThirdEyeClientConfig config, int maxConnections) {
    this.config = config;
    this.connections = new ArrayBlockingQueue<>(maxConnections);

    if (config.getBrokerUrl() != null
        && config.getBrokerUrl().trim().length() > 0) {
      ZkClient zkClient = new ZkClient(config.getZookeeperUrl());
      zkClient.setZkSerializer(new ZNRecordSerializer());
      zkClient.waitUntilConnected();
      ZKHelixAdmin helixAdmin = new ZKHelixAdmin(zkClient);
      List<String> thirdeyeBrokerList = helixAdmin.getInstancesInClusterWithTag(
          config.getClusterName(), config.getTag());

      String[] thirdeyeBrokers = new String[thirdeyeBrokerList.size()];
      for (int i = 0; i < thirdeyeBrokerList.size(); i++) {
        String instanceName = thirdeyeBrokerList.get(i);
        InstanceConfig instanceConfig =
            helixAdmin.getInstanceConfig(config.getClusterName(), instanceName);
        thirdeyeBrokers[i] = instanceConfig.getHostName().replaceAll(BROKER_PREFIX, "") + ":"
            + instanceConfig.getPort();
      }
      for (int i = 0; i < maxConnections; i++) {
        connections.add(ConnectionFactory.fromHostList(thirdeyeBrokers));
      }
    } else {
      for (int i = 0; i < maxConnections; i++) {
        connections.add(ConnectionFactory.fromZookeeper(config.getZookeeperUrl()
            + "/" + config.getClusterName()));
      }
    }
  }

  public ResultSetGroup execute(String table, String query) throws Exception {
    Connection c = null;
    try {
      c = this.connections.poll(CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS);
      LOG.debug("Executing pinot query on table '{}': '{}'", table, query);
      long t = System.currentTimeMillis();
      ResultSetGroup res = c.execute(table, query);
      LOG.debug("Query took {} ms", System.currentTimeMillis() - t);
      return res;
    } catch(Exception e) {
      throw e;
    } finally {
      if(c != null)
        this.connections.add(c);
    }
  }
}
