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
package com.linkedin.pinot.opal.distributed.keyCoordinator.starter;

import com.linkedin.pinot.opal.common.messages.KeyCoordinatorQueueMsg;
import com.linkedin.pinot.opal.common.updateStrategy.MessageResolveStrategy;
import com.linkedin.pinot.opal.common.updateStrategy.MessageTimeResolveStrategy;
import com.linkedin.pinot.opal.distributed.keyCoordinator.internal.ConfigStore;
import com.linkedin.pinot.opal.common.RpcQueue.KafkaQueueConsumer;
import com.linkedin.pinot.opal.common.RpcQueue.KafkaQueueProducer;
import com.linkedin.pinot.opal.common.messages.LogCoordinatorMessage;
import com.linkedin.pinot.opal.common.utils.State;
import com.linkedin.pinot.opal.distributed.keyCoordinator.internal.DistributedKeyCoordinatorCore;
import com.linkedin.pinot.opal.distributed.keyCoordinator.api.KeyCoordinatorApiApplication;
import com.linkedin.pinot.opal.distributed.keyCoordinator.internal.KeyCoordinatorQueueConsumer;
import com.linkedin.pinot.opal.distributed.keyCoordinator.internal.LogCoordinatorQueueProducer;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Iterator;

public class KeyCoordinatorStarter {
  private static final Logger LOGGER = LoggerFactory.getLogger(KeyCoordinatorStarter.class);

  private ConfigStore configStore;
  private KeyCoordinatorConf _keyCoordinatorConf;
  private KafkaQueueConsumer<KeyCoordinatorQueueMsg> _consumer;
  private KafkaQueueProducer<String, LogCoordinatorMessage> _producer;
  private MessageResolveStrategy _messageResolveStrategy;
  private DistributedKeyCoordinatorCore _keyCoordinatorCore;
  private KeyCoordinatorApiApplication _application;

  public KeyCoordinatorStarter(KeyCoordinatorConf conf) {
    _keyCoordinatorConf = conf;
    configStore = ConfigStore.getConfigStore(conf.getConfigStorePathFile());
    _consumer = new KeyCoordinatorQueueConsumer(
        _keyCoordinatorConf.subset(KeyCoordinatorConf.KEY_COORDINATOR_CONSUMER_CONF));
    _producer = new LogCoordinatorQueueProducer(
        _keyCoordinatorConf.subset(KeyCoordinatorConf.KEY_COORDINATOR_PRODUCER_CONF));
    _messageResolveStrategy = new MessageTimeResolveStrategy();
    _keyCoordinatorCore = new DistributedKeyCoordinatorCore();
    _application = new KeyCoordinatorApiApplication(this);
  }

  public void start() {
    LOGGER.info("starting key coordinator instance");
    _keyCoordinatorCore.init(_keyCoordinatorConf, _producer, _consumer, _messageResolveStrategy);
    LOGGER.info("finished init key coordinator instance, starting loop");
    _keyCoordinatorCore.start();
    LOGGER.info("starting web service");
    Configuration serverConfig = _keyCoordinatorConf.subset(KeyCoordinatorConf.SERVER_CONFIG);
    _application.start(serverConfig.getInt(KeyCoordinatorConf.PORT, KeyCoordinatorConf.PORT_DEFAULT));
  }

  public void shutdown() {
    LOGGER.info("shutting down key coordinator instance");
    _keyCoordinatorCore.stop();
    LOGGER.info("finished shutdown key coordinator instance");
    _producer.close();
    LOGGER.info("finished shutdown producer");
    _consumer.close();
    LOGGER.info("finished shutdown consumer");
  }

  public boolean isRunning() {
    return _keyCoordinatorCore != null && _keyCoordinatorCore.getState() == State.RUNNING;
  }

  public static KeyCoordinatorStarter startDefault(KeyCoordinatorConf conf) {
    KeyCoordinatorStarter starter = new KeyCoordinatorStarter(conf);
    starter.start();
    return starter;
  }

  public static void main(String[] args) throws ConfigurationException {
    if (args.length == 0) {
      System.out.println("need path to file in props");
    }
    File confFile = new File(args[0]);
    if (!confFile.exists()) {
      System.out.println("conf file does not exist");
    }
    KeyCoordinatorConf properties = new KeyCoordinatorConf(confFile);
    LOGGER.info(properties.toString());
    Iterator<String> iterators = properties.getKeys();
    while (iterators.hasNext()) {
      String key = iterators.next();
      LOGGER.info("opal kc Prop: key= " + key + ", value= " + properties.getString(key));
    }
    KeyCoordinatorStarter starter = startDefault(properties);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        try {
          LOGGER.info("received shutdown event from shutdown hook");
          starter.shutdown();
        } catch (Exception e) {
          e.printStackTrace();
          LOGGER.error("error shutting down key coordinator: ", e);
        }
      }
    });

  }
}
