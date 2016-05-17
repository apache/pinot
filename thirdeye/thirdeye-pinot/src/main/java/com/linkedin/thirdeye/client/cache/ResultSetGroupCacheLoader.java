package com.linkedin.thirdeye.client.cache;

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
import com.linkedin.thirdeye.client.PinotQuery;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClient;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClientConfig;

public class ResultSetGroupCacheLoader extends CacheLoader<PinotQuery, ResultSetGroup> {
  private static final Logger LOG = LoggerFactory.getLogger(ResultSetGroupCacheLoader.class);

  private Connection connection;

  private static final String BROKER_PREFIX = "Broker_";

  public ResultSetGroupCacheLoader(Connection connection) {
    this.connection = connection;
  }

  public ResultSetGroupCacheLoader(PinotThirdEyeClientConfig pinotThirdEyeClientConfig) {

    if (pinotThirdEyeClientConfig.getBrokerUrl() != null
        && pinotThirdEyeClientConfig.getBrokerUrl().trim().length() > 0) {
      ZkClient zkClient = new ZkClient(pinotThirdEyeClientConfig.getZookeeperUrl());
      zkClient.setZkSerializer(new ZNRecordSerializer());
      zkClient.waitUntilConnected();
      ZKHelixAdmin helixAdmin = new ZKHelixAdmin(zkClient);
      List<String> thirdeyeBrokerList = helixAdmin.getInstancesInClusterWithTag(
          pinotThirdEyeClientConfig.getClusterName(), pinotThirdEyeClientConfig.getTag());

      String[] thirdeyeBrokers = new String[thirdeyeBrokerList.size()];
      for (int i = 0; i < thirdeyeBrokerList.size(); i++) {
        String instanceName = thirdeyeBrokerList.get(i);
        InstanceConfig instanceConfig =
            helixAdmin.getInstanceConfig(pinotThirdEyeClientConfig.getClusterName(), instanceName);
        thirdeyeBrokers[i] = instanceConfig.getHostName().replaceAll(BROKER_PREFIX, "") + ":"
            + instanceConfig.getPort();
      }
      this.connection = ConnectionFactory.fromHostList(thirdeyeBrokers);
    } else {
      this.connection = ConnectionFactory.fromZookeeper(pinotThirdEyeClientConfig.getZookeeperUrl()
          + "/" + pinotThirdEyeClientConfig.getClusterName());
    }
  }

  @Override
  public ResultSetGroup load(PinotQuery pinotQuery) throws Exception {
    try {
      synchronized (this) {
        ResultSetGroup resultSetGroup =
            connection.execute(pinotQuery.getTableName(), pinotQuery.getPql());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Query:{}  response:{}", pinotQuery.getPql(), format(resultSetGroup));
        }
        return resultSetGroup;
      }
    } catch (PinotClientException cause) {
      LOG.error("Error when running pql:" + pinotQuery.getPql(), cause);
      throw new PinotClientException("Error when running pql:" + pinotQuery.getPql(), cause);
    }
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
