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
import org.apache.pinot.core.realtime.impl.kafka.KafkaStarterUtils;
import org.apache.pinot.core.realtime.stream.StreamDataProvider;
import org.apache.pinot.core.realtime.stream.StreamDataServerStartable;
import org.apache.pinot.tools.Command;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class for command to start Kafka.
 */
public class StartKafkaCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(StartKafkaCommand.class);

  @Option(name = "-port", required = false, metaVar = "<int>", usage = "Port to start Kafka server on.")
  private int _port = KafkaStarterUtils.DEFAULT_KAFKA_PORT;

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help = false;

  @Option(name = "-brokerId", required = false, metaVar = "<int>", usage = "Kafka broker ID.")
  private int _brokerId = KafkaStarterUtils.DEFAULT_BROKER_ID;

  @Option(name = "-zkAddress", required = false, metaVar = "<string>", usage = "Address of Zookeeper.")
  private String _zkAddress = "localhost:2181";
  private StreamDataServerStartable _kafkaStarter;

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public String getName() {
    return "StartKafka";
  }

  @Override
  public String toString() {
    return "StartKafka -port " + _port + " -brokerId " + _brokerId + " -zkAddress " + _zkAddress;
  }

  @Override
  public String description() {
    return "Start Kafka at the specified port.";
  }

  @Override
  public boolean execute()
      throws IOException {
    try {
      _kafkaStarter = StreamDataProvider.getServerDataStartable(KafkaStarterUtils.KAFKA_SERVER_STARTABLE_CLASS_NAME, KafkaStarterUtils.getDefaultKafkaConfiguration());
    } catch (Exception e) {
      throw new RuntimeException("Failed to start " + KafkaStarterUtils.KAFKA_SERVER_STARTABLE_CLASS_NAME, e);
    }
    _kafkaStarter.start();

    LOGGER.info("Start kafka at localhost:" + _port + " in thread " + Thread.currentThread().getName());

    savePID(System.getProperty("java.io.tmpdir") + File.separator + ".kafka.pid");
    return true;
  }
}
