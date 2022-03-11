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
package org.apache.pinot.tools.utils;

import java.util.Properties;
import org.apache.pinot.spi.stream.StreamDataProvider;
import org.apache.pinot.spi.stream.StreamDataServerStartable;


public class KinesisStarterUtils {
  private KinesisStarterUtils() {
  }

  public static final String DEFAULT_KINESIS_PORT = "4566";
  public static final String DEFAULT_KINESIS_ENDPOINT = "http://localhost:" + DEFAULT_KINESIS_PORT;

  public static final String KINESIS_SERVER_STARTABLE_CLASS_NAME =
      getKinesisConnectorPackageName() + ".server.KinesisDataServerStartable";
  public static final String KINESIS_PRODUCER_CLASS_NAME =
      getKinesisConnectorPackageName() + ".server.KinesisDataProducer";
  public static final String KINESIS_STREAM_CONSUMER_FACTORY_CLASS_NAME =
      getKinesisConnectorPackageName() + ".KinesisConsumerFactory";

  public static final String PORT = "port";
  public static final String NUM_SHARDS = "numShards";

  private static String getKinesisConnectorPackageName() {
    return "org.apache.pinot.plugin.stream.kinesis";
  }

  public static Properties getTopicCreationProps(int numKinesisShards) {
    Properties topicProps = new Properties();
    topicProps.put(NUM_SHARDS, numKinesisShards);
    return topicProps;
  }

  public static StreamDataServerStartable startServer(final int port, final Properties baseConf) {
    StreamDataServerStartable kinesisStarter;
    Properties configuration = new Properties(baseConf);
    int kinesisPort = port;
    try {
      configuration.put(KinesisStarterUtils.PORT, kinesisPort);
      kinesisStarter = StreamDataProvider.getServerDataStartable(KINESIS_SERVER_STARTABLE_CLASS_NAME, configuration);
    } catch (Exception e) {
      throw new RuntimeException("Failed to start " + KINESIS_SERVER_STARTABLE_CLASS_NAME, e);
    }
    kinesisStarter.start();
    return kinesisStarter;
  }
}
