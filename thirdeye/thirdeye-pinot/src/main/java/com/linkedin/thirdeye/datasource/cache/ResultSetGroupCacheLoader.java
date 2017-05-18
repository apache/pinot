package com.linkedin.thirdeye.datasource.cache;

import java.util.List;

import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.InstanceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheLoader;
import com.linkedin.pinot.client.Connection;
import com.linkedin.pinot.client.ConnectionFactory;
import com.linkedin.pinot.client.PinotClientException;
import com.linkedin.pinot.client.ResultSet;
import com.linkedin.pinot.client.ResultSetGroup;
import com.linkedin.thirdeye.datasource.pinot.PinotQuery;
import com.linkedin.thirdeye.datasource.pinot.PinotThirdEyeDataSourceConfig;

public class ResultSetGroupCacheLoader extends CacheLoader<PinotQuery, ResultSetGroup> {
  private static final Logger LOG = LoggerFactory.getLogger(ResultSetGroupCacheLoader.class);
  
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

  public ResultSetGroupCacheLoader(PinotThirdEyeDataSourceConfig pinotThirdEyeDataSourceConfig) {

    if (pinotThirdEyeDataSourceConfig.getBrokerUrl() != null
        && pinotThirdEyeDataSourceConfig.getBrokerUrl().trim().length() > 0) {
      ZkClient zkClient = new ZkClient(pinotThirdEyeDataSourceConfig.getZookeeperUrl());
      zkClient.setZkSerializer(new ZNRecordSerializer());
      zkClient.waitUntilConnected();
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
      this.connections = new Connection[MAX_CONNECTIONS];
      for (int i = 0; i < MAX_CONNECTIONS; i++) {
        connections[i] = ConnectionFactory.fromHostList(thirdeyeBrokers);
      }
    } else {
      this.connections = new Connection[MAX_CONNECTIONS];
      for (int i = 0; i < MAX_CONNECTIONS; i++) {
        connections[i] = ConnectionFactory.fromZookeeper(pinotThirdEyeDataSourceConfig.getZookeeperUrl()
            + "/" + pinotThirdEyeDataSourceConfig.getClusterName());
      }
    }
  }

  @Override
  public ResultSetGroup load(PinotQuery pinotQuery) throws Exception {
    try {
      Connection connection = getConnection();
      synchronized (connection) {
        long start = System.currentTimeMillis();

        ResultSetGroup resultSetGroup =
            connection.execute(pinotQuery.getTableName(), pinotQuery.getPql());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Query:{}  response:{}", pinotQuery.getPql(), format(resultSetGroup));
        }
        long end = System.currentTimeMillis();
        LOG.info("Query:{}  took:{} ms", pinotQuery.getPql(), (end - start));

        return resultSetGroup;
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
}
