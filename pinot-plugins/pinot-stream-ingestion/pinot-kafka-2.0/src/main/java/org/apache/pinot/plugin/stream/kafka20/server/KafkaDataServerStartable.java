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

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.utils.Time;
import org.apache.pinot.spi.stream.StreamDataServerStartable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;


public class KafkaDataServerStartable implements StreamDataServerStartable {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDataServerStartable.class);

  private static final String ZOOKEEPER_CONNECT = "zookeeper.connect";
  private static final String LOG_DIRS = "log.dirs";
  private static final String PORT = "port";

  private KafkaServer _serverStartable;
  private int _port;
  private String _zkStr;
  private String _logDirPath;
  private AdminClient _adminClient;

  public void init(Properties props) {
    _port = (int) props.get(PORT);
    _zkStr = props.getProperty(ZOOKEEPER_CONNECT);
    _logDirPath = props.getProperty(LOG_DIRS);

    // Create the ZK nodes for Kafka, if needed
    int indexOfFirstSlash = _zkStr.indexOf('/');
    if (indexOfFirstSlash != -1) {
      String bareZkUrl = _zkStr.substring(0, indexOfFirstSlash);
      String zkNodePath = _zkStr.substring(indexOfFirstSlash);
      ZkClient client = new ZkClient(bareZkUrl);
      client.createPersistent(zkNodePath, true);
      client.close();
    }

    File logDir = new File(_logDirPath);
    logDir.mkdirs();

    props.put("zookeeper.session.timeout.ms", "60000");
    _serverStartable = new KafkaServer(new KafkaConfig(props), Time.SYSTEM, Option.empty(), false);
    final Map<String, Object> config = new HashMap<>();
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + _port);
    config.put(AdminClientConfig.CLIENT_ID_CONFIG, "Kafka2AdminClient-" + UUID.randomUUID().toString());
    config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 15000);
    _adminClient = KafkaAdminClient.create(config);
  }

  @Override
  public void start() {
    _serverStartable.startup();
  }

  @Override
  public void stop() {
    _serverStartable.shutdown();
    FileUtils.deleteQuietly(new File(_serverStartable.config().logDirs().apply(0)));
  }

  @Override
  public void createTopic(String topic, Properties props) {
    Map<String, String> map = new HashMap<>();
    for (String key : props.stringPropertyNames()) {
      map.put(key, props.getProperty(key));
    }
    int partition = (Integer) props.get("partition");
    Collection<NewTopic> topicList = Arrays.asList(new NewTopic(topic, partition, (short) 1).configs(map));
    _adminClient.createTopics(topicList);
    waitForCondition(new Function<Void, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable Void aVoid) {
        try {
          return _adminClient.listTopics().names().get().contains(topic);
        } catch (Exception e) {
          LOGGER.warn("Could not fetch Kafka topics", e);
          return null;
        }
      }
    }, 1000L, 30000, "Kafka topic " + topic + " is not created yet");
  }

  @Override
  public int getPort() {
    return _port;
  }

  private static void waitForCondition(Function<Void, Boolean> condition, long checkIntervalMs, long timeoutMs,
      @Nullable String errorMessage) {
    long endTime = System.currentTimeMillis() + timeoutMs;
    String errorMessageSuffix = errorMessage != null ? ", error message: " + errorMessage : "";
    while (System.currentTimeMillis() < endTime) {
      try {
        if (Boolean.TRUE.equals(condition.apply(null))) {
          return;
        }
        Thread.sleep(checkIntervalMs);
      } catch (Exception e) {
        LOGGER.error("Caught exception while checking the condition" + errorMessageSuffix, e);
      }
    }
    LOGGER.error("Failed to meet condition in " + timeoutMs + "ms" + errorMessageSuffix);
  }
}
