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
package org.apache.pinot.plugin.stream.kafka20.server;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.pinot.spi.stream.StreamDataServerStartable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaDataServerStartable implements StreamDataServerStartable {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDataServerStartable.class);

  private static final String ZOOKEEPER_CONNECT = "zookeeper.connect";
  private static final String LOG_DIRS = "log.dirs";
  private static final String PORT = "port";

  private KafkaServerStartable serverStartable;
  private int port;
  private String zkStr;
  private String logDirPath;
  private AdminClient adminClient;

  public void init(Properties props) {
    port = (int) props.get(PORT);
    zkStr = props.getProperty(ZOOKEEPER_CONNECT);
    logDirPath = props.getProperty(LOG_DIRS);

    // Create the ZK nodes for Kafka, if needed
    int indexOfFirstSlash = zkStr.indexOf('/');
    if (indexOfFirstSlash != -1) {
      String bareZkUrl = zkStr.substring(0, indexOfFirstSlash);
      String zkNodePath = zkStr.substring(indexOfFirstSlash);
      ZkClient client = new ZkClient(bareZkUrl);
      client.createPersistent(zkNodePath, true);
      client.close();
    }

    File logDir = new File(logDirPath);
    logDir.mkdirs();

    props.put("zookeeper.session.timeout.ms", "60000");
    serverStartable = new KafkaServerStartable(new KafkaConfig(props));
    final Map<String, Object> config = new HashMap<>();
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + port);
    config.put(AdminClientConfig.CLIENT_ID_CONFIG, "Kafka2AdminClient-" + UUID.randomUUID().toString());
    config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 15000);
    adminClient = KafkaAdminClient.create(config);
  }

  @Override
  public void start() {
    serverStartable.startup();
  }

  @Override
  public void stop() {
    serverStartable.shutdown();
    FileUtils.deleteQuietly(new File(serverStartable.staticServerConfig().logDirs().apply(0)));
  }

  @Override
  public void createTopic(String topic, Properties props) {
    int partition = (Integer) props.get("partition");
    Collection<NewTopic> topicList = Arrays.asList(new NewTopic(topic, partition, (short) 1));
    adminClient.createTopics(topicList);
  }

  @Override
  public int getPort() {
    return port;
  }
}
