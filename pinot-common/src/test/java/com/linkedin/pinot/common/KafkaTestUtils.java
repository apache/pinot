/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.common;

import java.util.Properties;
import kafka.admin.TopicCommand;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.I0Itec.zkclient.ZkClient;


/**
 * Utilities to start Kafka during unit tests.
 *
 * @author jfim
 */
public class KafkaTestUtils {
  public static final int DEFAULT_KAFKA_PORT = 19092;
  public static final int DEFAULT_BROKER_ID = 0;
  public static final String DEFAULT_ZK_STR = ZkTestUtils.DEFAULT_ZK_STR + "/kafka";
  public static final String DEFAULT_KAFKA_BROKER = "localhost:" + DEFAULT_KAFKA_PORT;

  public static Properties getDefaultKafkaConfiguration() {
    return new Properties();
  }

  public static KafkaServerStartable startServer(final int port, final int brokerId, final String zkStr, final Properties configuration) {
    // Create the ZK nodes for Kafka, if needed
    int indexOfFirstSlash = zkStr.indexOf('/');
    if (indexOfFirstSlash != -1) {
      String bareZkUrl = zkStr.substring(0, indexOfFirstSlash);
      String zkNodePath = zkStr.substring(indexOfFirstSlash);
      ZkClient client = new ZkClient(bareZkUrl);
      client.createPersistent(zkNodePath, true);
      client.createPersistent(zkNodePath + "-tracking", true);
      client.close();
    }

    configuration.put("port", Integer.toString(port));
    configuration.put("zookeeper.connect", zkStr);
    configuration.put("broker.id", Integer.toString(brokerId));
    KafkaConfig config = new KafkaConfig(configuration);

    KafkaServerStartable serverStartable = new KafkaServerStartable(config);
    serverStartable.startup();

    return serverStartable;
  }

  public static void stopServer(KafkaServerStartable serverStartable) {
    serverStartable.shutdown();
  }

  public static void createTopic(String kafkaTopic, String zkStr) {
    TopicCommand.main(new String[]{"--create", "--zookeeper", zkStr, "--replication-factor", "1",
        "--partitions", "10", "--topic", kafkaTopic});
  }
}
