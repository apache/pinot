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
@CommandLine.Command(name = "StartKafka", mixinStandardHelpOptions = true)
public class StartKafkaCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(StartKafkaCommand.class);

  @CommandLine.Option(names = {"-port"}, required = false, description = "Port to start Kafka server on.")
  private int _port = KafkaStarterUtils.DEFAULT_KAFKA_PORT;

  @CommandLine.Option(names = {"-brokerId"}, required = false, description = "Kafka broker ID.")
  private int _brokerId = KafkaStarterUtils.DEFAULT_BROKER_ID;

  @Override
  public String getName() {
    return "StartKafka";
  }

  @Override
  public String toString() {
    return "StartKafka -port " + _port + " -brokerId " + _brokerId;
  }

  @Override
  public String description() {
    return "Start Kafka at the specified port.";
  }

  @Override
  public boolean execute()
      throws Exception {
    if (_port <= 0 || _port > 65535) {
      throw new IllegalArgumentException("Invalid Kafka port: " + _port);
    }
    if (_brokerId < 0) {
      throw new IllegalArgumentException("Invalid Kafka brokerId: " + _brokerId);
    }

    Properties props = new Properties();
    props.put(KafkaStarterUtils.KAFKA_SERVER_OWNER_NAME, getName());
    props.put(KafkaStarterUtils.KAFKA_SERVER_BOOTSTRAP_SERVERS, "localhost:" + _port);
    props.put(KafkaStarterUtils.KAFKA_SERVER_PORT, String.valueOf(_port));
    props.put(KafkaStarterUtils.KAFKA_SERVER_BROKER_ID, String.valueOf(_brokerId));
    props.put(KafkaStarterUtils.KAFKA_SERVER_ALLOW_MANAGED_FOR_CONFIGURED_BROKER, "true");

    StreamDataServerStartable kafkaStarter =
        StreamDataProvider.getServerDataStartable(KafkaStarterUtils.KAFKA_SERVER_STARTABLE_CLASS_NAME, props);
    kafkaStarter.start();
    LOGGER.info("Kafka server started at localhost:{}", _port);
    return true;
  }
}
