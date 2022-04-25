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
package org.apache.pinot.tools.streams.githubevents;

import com.google.common.base.Preconditions;
import java.io.File;
import java.net.URL;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.stream.StreamDataProducer;
import org.apache.pinot.spi.stream.StreamDataProvider;
import org.apache.pinot.tools.Quickstart;
import org.apache.pinot.tools.streams.PinotRealtimeSource;
import org.apache.pinot.tools.streams.PinotSourceDataGenerator;
import org.apache.pinot.tools.utils.KafkaStarterUtils;
import org.apache.pinot.tools.utils.KinesisStarterUtils;
import org.apache.pinot.tools.utils.StreamSourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.tools.Quickstart.printStatus;


/**
 * Creates a Kafka producer, for given kafka broker list
 * Continuously fetches github events data.
 * Creates a PullRequestMergedEvent for each valid PR event.
 * Publishes the PullRequestMergedEvent to the given kafka topic
 */
public class PullRequestMergedEventsStream {
  private static final Logger LOGGER = LoggerFactory.getLogger(PullRequestMergedEventsStream.class);

  private PinotRealtimeSource _pinotStream;

  public PullRequestMergedEventsStream(File schemaFile, String topicName, String personalAccessToken,
      StreamDataProducer producer)
      throws Exception {
    PinotSourceDataGenerator generator = new GithubPullRequestSourceGenerator(schemaFile, personalAccessToken);
    _pinotStream =
        PinotRealtimeSource.builder().setProducer(producer).setGenerator(generator).setTopic(topicName).build();
  }

  public PullRequestMergedEventsStream(String schemaFilePath, String topicName, String personalAccessToken,
      StreamDataProducer producer)
      throws Exception {
    this(getSchemaFile(schemaFilePath), topicName, personalAccessToken, producer);
  }

  public static File getSchemaFile(String schemaFilePath) {
    File pinotSchema;
    try {
      if (schemaFilePath == null) {
        ClassLoader classLoader = PullRequestMergedEventsStream.class.getClassLoader();
        URL resource = classLoader.getResource("examples/stream/githubEvents/pullRequestMergedEvents_schema.json");
        Preconditions.checkNotNull(resource);
        pinotSchema = new File(resource.getFile());
      } else {
        pinotSchema = new File(schemaFilePath);
      }
    } catch (Exception e) {
      LOGGER.error("Got exception while reading Pinot schema from file: [" + schemaFilePath + "]");
      throw e;
    }
    return pinotSchema;
  }

  public static StreamDataProducer getKafkaStreamDataProducer()
      throws Exception {
    return getKafkaStreamDataProducer(KafkaStarterUtils.DEFAULT_KAFKA_BROKER);
  }

  public static StreamDataProducer getKafkaStreamDataProducer(String kafkaBrokerList)
      throws Exception {
    Properties properties = new Properties();
    properties.put("metadata.broker.list", kafkaBrokerList);
    properties.put("serializer.class", "kafka.serializer.DefaultEncoder");
    properties.put("request.required.acks", "1");
    return StreamDataProvider.getStreamDataProducer(KafkaStarterUtils.KAFKA_PRODUCER_CLASS_NAME, properties);
  }

  public static StreamDataProducer getKinesisStreamDataProducer(String endpoint, String region, String access,
      String secret)
      throws Exception {
    Properties properties = new Properties();

    if (StringUtils.isNotEmpty(access) && StringUtils.isNotEmpty(secret)) {
      properties.put("access", access);
      properties.put("secret", secret);
    }

    if (StringUtils.isNotEmpty(endpoint)) {
      properties.put("endpoint", endpoint);
    }
    properties.put("region", region);
    return StreamDataProvider.getStreamDataProducer(KinesisStarterUtils.KINESIS_PRODUCER_CLASS_NAME, properties);
  }

  public static StreamDataProducer getKinesisStreamDataProducer()
      throws Exception {
    return getKinesisStreamDataProducer("http://localhost:4566", "us-east-1", "access", "secret");
  }

  public static StreamDataProducer getStreamDataProducer(StreamSourceType streamSourceType)
      throws Exception {
    switch (streamSourceType) {
      case KAFKA:
        return getKafkaStreamDataProducer();
      case KINESIS:
        return getKinesisStreamDataProducer();
      default:
        throw new RuntimeException("Invalid streamSourceType specified: " + streamSourceType);
    }
  }

  public static void main(String[] args)
      throws Exception {
    String personalAccessToken = args[0];
    String schemaFile = args[1];
    String topic = "pullRequestMergedEvent";
    PullRequestMergedEventsStream stream =
        new PullRequestMergedEventsStream(schemaFile, topic, personalAccessToken, getKafkaStreamDataProducer());
    stream.execute();
  }

  /**
   * Starts the stream.
   * Adds shutdown hook.
   */
  public void execute() {
    start();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        shutdown();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }));
  }

  /**
   * Shuts down the stream.
   */
  public void shutdown()
      throws Exception {
    printStatus(Quickstart.Color.GREEN, "***** Shutting down pullRequestMergedEvents Stream *****");
    _pinotStream.close();
  }

  public void start() {
    printStatus(Quickstart.Color.CYAN, "***** Starting pullRequestMergedEvents Stream *****");
    _pinotStream.run();
  }
}
