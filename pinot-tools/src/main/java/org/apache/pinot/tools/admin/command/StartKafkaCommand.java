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
package org.apache.pinot.tools.admin.command;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import org.apache.pinot.spi.stream.StreamDataProvider;
import org.apache.pinot.spi.stream.StreamDataServerStartable;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.utils.KafkaStarterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


/**
 * Class for command to start Kafka.
 */
@CommandLine.Command(name = "StartKafka", description = "Start Kafka at the specified port.",
    mixinStandardHelpOptions = true)
public class StartKafkaCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(StartKafkaCommand.class);

  @CommandLine.Option(names = {"-port"}, required = false, description = "Port to start Kafka server on.")
  private int _port = KafkaStarterUtils.DEFAULT_KAFKA_PORT;

  @CommandLine.Option(names = {"-brokerId"}, required = false, description = "Kafka broker ID.")
  private int _brokerId = KafkaStarterUtils.DEFAULT_BROKER_ID;

  @CommandLine.Option(names = {"-zkAddress"}, required = false, description = "Address of Zookeeper.")
  private String _zkAddress = KafkaStarterUtils.getDefaultKafkaZKAddress();
  private StreamDataServerStartable _kafkaStarter;

  @Override
  public String getName() {
    return "StartKafka";
  }

  @Override
  public String toString() {
    return "StartKafka -port " + _port + " -brokerId " + _brokerId + " -zkAddress " + _zkAddress;
  }

  @Override
  public boolean execute()
      throws IOException {
    Properties kafkaConfiguration = KafkaStarterUtils.getDefaultKafkaConfiguration();
    kafkaConfiguration.put(KafkaStarterUtils.BROKER_ID, _brokerId);
    kafkaConfiguration.put(KafkaStarterUtils.PORT, _port);
    kafkaConfiguration.put(KafkaStarterUtils.ZOOKEEPER_CONNECT, _zkAddress);
    try {
      _kafkaStarter = StreamDataProvider
          .getServerDataStartable(KafkaStarterUtils.KAFKA_SERVER_STARTABLE_CLASS_NAME, kafkaConfiguration);
    } catch (Exception e) {
      throw new RuntimeException("Failed to start " + KafkaStarterUtils.KAFKA_SERVER_STARTABLE_CLASS_NAME, e);
    }
    _kafkaStarter.start();
    LOGGER.info("Start kafka at localhost:" + _port + " in thread " + Thread.currentThread().getName());
    savePID(System.getProperty("java.io.tmpdir") + File.separator + ".kafka.pid");
    return true;
  }
}
