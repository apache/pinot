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
package org.apache.pinot.integration.tests.rocketmq;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.admin.TopicStatsTable;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.testng.Assert;


public class MiniRocketMQCluster implements Closeable {
  private NamesrvController _namesrvController;
  private BrokerController _brokerController;

  private static Random _random = new Random();
  private static final int TOPIC_CREATE_TIME = 30 * 1000;
  private static final int INDEX_NUM = 1000;
  private static final int COMMIT_LOG_SIZE = 1024 * 1024 * 100;
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), "MiniRocketMQCluster-" + UUID.randomUUID());
  private static AtomicInteger _port = new AtomicInteger(40000);
  private final String _clusterName;
  private final String _brokerName;
  private String _namesrvAddress;

  public MiniRocketMQCluster(String clusterName, String brokerName) {
    _clusterName = clusterName;
    _brokerName = brokerName;
  }

  public void start() {
    File namesrvDir = new File(TEMP_DIR.getAbsolutePath(), "namesrv");
    File brokerDir = new File(TEMP_DIR.getAbsolutePath(), "broker");
    namesrvDir.mkdirs();
    brokerDir.mkdirs();
    _namesrvController = createAndStartNamesrv(namesrvDir.getAbsolutePath());
    _namesrvAddress = "127.0.0.1:" + _namesrvController.getNettyServerConfig().getListenPort();
    _brokerController = createAndStartBroker(brokerDir.getAbsolutePath(), _namesrvAddress);
  }

  @Override
  public void close()
      throws IOException {
    _brokerController.shutdown();
    _namesrvController.shutdown();
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  public String getNamesrvAddress() {
    return _namesrvAddress;
  }

  public static synchronized int nextPort() {
    return _port.addAndGet(_random.nextInt(10) + 10);
  }

  public static NamesrvController createAndStartNamesrv(String baseDir) {
    NamesrvConfig namesrvConfig = new NamesrvConfig();
    NettyServerConfig nameServerNettyServerConfig = new NettyServerConfig();
    namesrvConfig.setKvConfigPath(baseDir + File.separator + "namesrv" + File.separator + "kvConfig.json");
    namesrvConfig.setConfigStorePath(baseDir + File.separator + "namesrv" + File.separator + "namesrv.properties");

    nameServerNettyServerConfig.setListenPort(nextPort());
    NamesrvController namesrvController = new NamesrvController(namesrvConfig, nameServerNettyServerConfig);
    try {
      Assert.assertTrue(namesrvController.initialize());
      namesrvController.start();
    } catch (Exception e) {
      Assert.fail();
    }
    return namesrvController;
  }

  public BrokerController createAndStartBroker(String baseDir, String nsAddr) {
    File commitLog = new File(baseDir, "commitlog");
    File consumeQueue = new File(baseDir, "consumequeue");
    commitLog.mkdirs();
    consumeQueue.mkdirs();

    BrokerConfig brokerConfig = new BrokerConfig();
    MessageStoreConfig storeConfig = new MessageStoreConfig();
    brokerConfig.setBrokerName(_brokerName);
    brokerConfig.setBrokerIP1("127.0.0.1");
    brokerConfig.setNamesrvAddr(nsAddr);
    brokerConfig.setEnablePropertyFilter(true);
    brokerConfig.setBrokerClusterName(_clusterName);
    storeConfig.setStorePathRootDir(baseDir);
    storeConfig.setStorePathCommitLog(commitLog.getAbsolutePath());
    storeConfig.setMappedFileSizeCommitLog(COMMIT_LOG_SIZE);
    storeConfig.setMaxIndexNum(INDEX_NUM);
    storeConfig.setMaxHashSlotNum(INDEX_NUM * 4);

    NettyServerConfig nettyServerConfig = new NettyServerConfig();
    NettyClientConfig nettyClientConfig = new NettyClientConfig();
    nettyServerConfig.setListenPort(nextPort());
    storeConfig.setHaListenPort(nextPort());
    BrokerController brokerController =
        new BrokerController(brokerConfig, nettyServerConfig, nettyClientConfig, storeConfig);
    try {
      Assert.assertTrue(brokerController.initialize());
      brokerController.start();
    } catch (Throwable t) {
      Assert.fail();
    }
    return brokerController;
  }

  public boolean initTopic(String topic, int queueNumbers) {
    long startTime = System.currentTimeMillis();
    boolean createResult;

    while (true) {
      createResult = createTopic(_namesrvAddress, _clusterName, topic, queueNumbers, TOPIC_CREATE_TIME);
      if (createResult) {
        break;
      } else if (System.currentTimeMillis() - startTime > TOPIC_CREATE_TIME) {
        Assert.fail(String.format("topic[%s] is created failed after:%d ms", topic,
            System.currentTimeMillis() - startTime));
        break;
      } else {
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        continue;
      }
    }

    return createResult;
  }

  public static boolean createTopic(String nameSrvAddr, String clusterName, String topic,
      int queueNum, int waitTimeSec) {
    boolean createResult = false;
    DefaultMQAdminExt mqAdminExt = new DefaultMQAdminExt();
    mqAdminExt.setInstanceName(UUID.randomUUID().toString());
    mqAdminExt.setNamesrvAddr(nameSrvAddr);
    try {
      mqAdminExt.start();
      mqAdminExt.createTopic(clusterName, topic, queueNum);
    } catch (Exception e) {
      return false;
    }

    long startTime = System.currentTimeMillis();
    while (!createResult) {
      createResult = checkTopicExist(mqAdminExt, topic);
      if (System.currentTimeMillis() - startTime < waitTimeSec * 1000) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      } else {
        break;
      }
    }

    mqAdminExt.shutdown();
    return createResult;
  }

  private static boolean checkTopicExist(DefaultMQAdminExt mqAdminExt, String topic) {
    boolean createResult = false;
    try {
      TopicStatsTable topicInfo = mqAdminExt.examineTopicStats(topic);
      createResult = !topicInfo.getOffsetTable().isEmpty();
    } catch (Exception e) {
      return false;
    }

    return createResult;
  }
}
