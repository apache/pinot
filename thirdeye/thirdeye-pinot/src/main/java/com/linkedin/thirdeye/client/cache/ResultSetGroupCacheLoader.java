package com.linkedin.thirdeye.client.cache;

import java.util.List;

import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.InstanceConfig;

import com.google.common.cache.CacheLoader;
import com.linkedin.pinot.client.Connection;
import com.linkedin.pinot.client.ConnectionFactory;
import com.linkedin.pinot.client.PinotClientException;
import com.linkedin.pinot.client.ResultSetGroup;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClientConfig;

public class ResultSetGroupCacheLoader extends CacheLoader<String, ResultSetGroup> {

  private Connection connection;

  private static final String BROKER_PREFIX = "Broker_";

  public ResultSetGroupCacheLoader(PinotThirdEyeClientConfig pinotThirdEyeClientConfig) {

    if(pinotThirdEyeClientConfig.getBrokerUrl() != null || pinotThirdEyeClientConfig.getBrokerUrl().trim().length() > 0 ){
      ZkClient zkClient = new ZkClient(pinotThirdEyeClientConfig.getZookeeperUrl());
      zkClient.setZkSerializer(new ZNRecordSerializer());
      zkClient.waitUntilConnected();
      ZKHelixAdmin helixAdmin = new ZKHelixAdmin(zkClient);
      List<String> thirdeyeBrokerList = helixAdmin.getInstancesInClusterWithTag(pinotThirdEyeClientConfig.getClusterName(), pinotThirdEyeClientConfig.getTag());

      String[] thirdeyeBrokers = new String[thirdeyeBrokerList.size()];
      for (int i = 0; i < thirdeyeBrokerList.size(); i++) {
        String instanceName = thirdeyeBrokerList.get(i);
        InstanceConfig instanceConfig = helixAdmin.getInstanceConfig(pinotThirdEyeClientConfig.getClusterName(), instanceName);
        thirdeyeBrokers[i] = instanceConfig.getHostName().replaceAll(BROKER_PREFIX, "") + ":"
            + instanceConfig.getPort();
      }
      this.connection = ConnectionFactory.fromHostList(thirdeyeBrokers);
    } else {
      this.connection = ConnectionFactory.fromZookeeper(pinotThirdEyeClientConfig.getZookeeperUrl());
    }
  }

  @Override
  public ResultSetGroup load(String sql) throws Exception {
    try {
      ResultSetGroup resultSetGroup = connection.execute(sql);
      return resultSetGroup;
    } catch (PinotClientException cause) {
      throw new PinotClientException("Error when running sql:" + sql, cause);
    }
  }
}