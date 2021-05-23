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
package org.apache.pinot.plugin.stream.kafka09.server;

import java.io.File;
import java.security.Permission;
import java.util.Properties;
import kafka.admin.TopicCommand;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.stream.StreamDataServerStartable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaDataServerStartable implements StreamDataServerStartable {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDataServerStartable.class);

  private static final String ZOOKEEPER_CONNECT = "zookeeper.connect";
  private static final String LOG_DIRS = "log.dirs";

  private KafkaServerStartable _serverStartable;
  private String _zkStr;
  private String _logDirPath;
  private int _port;

  private static void invokeTopicCommand(String[] args) {
    // jfim: Use Java security to trap System.exit in Kafka 0.9's TopicCommand
    System.setSecurityManager(new SecurityManager() {
      @Override
      public void checkPermission(Permission perm) {
        if (perm.getName().startsWith("exitVM")) {
          throw new SecurityException("System.exit is disabled");
        }
      }

      @Override
      public void checkPermission(Permission perm, Object context) {
        checkPermission(perm);
      }
    });

    try {
      TopicCommand.main(args);
    } catch (SecurityException ex) {
      // Do nothing, this is caused by our security manager that disables System.exit
    }

    System.setSecurityManager(null);
  }

  public void init(Properties props) {
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
    KafkaConfig config = new KafkaConfig(props);

    _port = config.port();
    _serverStartable = new KafkaServerStartable(config);
  }

  @Override
  public void start() {
    _serverStartable.startup();
  }

  @Override
  public void stop() {
    _serverStartable.shutdown();
    FileUtils.deleteQuietly(new File(_serverStartable.serverConfig().logDirs().apply(0)));
  }

  @Override
  public void createTopic(String topic, Properties props) {
    invokeTopicCommand(
        new String[]{"--create", "--zookeeper", this._zkStr, "--replication-factor", "1", "--partitions", Integer.toString(
            (Integer) props.get("partition")), "--topic", topic});
  }

  @Override
  public int getPort() {
    return _port;
  }
}
