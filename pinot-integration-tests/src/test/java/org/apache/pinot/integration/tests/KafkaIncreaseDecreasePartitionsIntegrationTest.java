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
package org.apache.pinot.integration.tests;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pinot.controller.api.resources.PauseStatusDetails;
import org.apache.pinot.controller.api.resources.TableViews;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.tools.utils.KafkaStarterUtils;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;


public class KafkaIncreaseDecreasePartitionsIntegrationTest extends BaseRealtimeClusterIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaIncreaseDecreasePartitionsIntegrationTest.class);

  private static final String KAFKA_TOPIC = "meetup";
  private static final int NUM_PARTITIONS = 3;

  public void publish(int numMessages)
      throws Exception {
    LOGGER.info("Publishing {} messages to Kafka topic: {}", numMessages, KAFKA_TOPIC);
    String bootstrapServers = "localhost:19092";

    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
      Random random = new Random();
      for (int i = 0; i < numMessages; i++) {
        long randomNumber = 17389458960L + random.nextInt(1000000); // Get a positive random number
        String randomString = generateRandomString(10); // Generate a random string of length 10
        String key = "message-" + i;
        String value = "{\"event_id\":\"" + randomString + "\",\"mtime\":" + randomNumber + "}";
        ProducerRecord<String, String> record = new ProducerRecord<>(KAFKA_TOPIC, 0, key, value);
        producer.send(record);
      }

      producer.flush();
      LOGGER.info("{} messages published successfully!", numMessages);
    }
  }

  private String generateRandomString(int length) {
    String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    Random random = new Random();
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < length; i++) {
      int index = random.nextInt(characters.length());
      sb.append(characters.charAt(index));
    }
    return sb.toString();
  }

  String getIdealState(String tableName)
      throws IOException {
    return sendGetRequest(getControllerRequestURLBuilder().forIdealState(tableName));
  }

  String getExternalView(String tableName)
      throws IOException {
    return sendGetRequest(getControllerRequestURLBuilder().forExternalView(tableName));
  }

  void pauseTable(String tableName)
      throws IOException {
    sendPostRequest(getControllerRequestURLBuilder().forPauseConsumption(tableName));
    TestUtils.waitForCondition((aVoid) -> {
      try {
        PauseStatusDetails pauseStatusDetails =
            JsonUtils.stringToObject(sendGetRequest(getControllerRequestURLBuilder().forPauseStatus(tableName)),
                PauseStatusDetails.class);
        return pauseStatusDetails.getConsumingSegments().isEmpty();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }, 60_000L, "Failed to pause table: " + tableName);
  }

  void resumeTable(String tableName)
      throws IOException {
    sendPostRequest(getControllerRequestURLBuilder().forResumeConsumption(tableName));
    TestUtils.waitForCondition((aVoid) -> {
      try {
        PauseStatusDetails pauseStatusDetails =
            JsonUtils.stringToObject(sendGetRequest(getControllerRequestURLBuilder().forPauseStatus(tableName)),
                PauseStatusDetails.class);
        return !pauseStatusDetails.getConsumingSegments().isEmpty();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }, 60_000L, "Failed to pause table: " + tableName);
  }

  void pauseResumeTable(String tableName)
      throws IOException {
    pauseTable(tableName);
    resumeTable(tableName);
  }

  private boolean isTableLoaded(String tableName) {
    try {
      return getCurrentCountStarResult(tableName) > 0;
    } catch (Exception e) {
      return false;
    }
  }

  String createTable()
      throws IOException {
    Schema schema =  createSchema("simpleMeetup_schema.json");
    addSchema(schema);
    TableConfig tableConfig = JsonUtils.inputStreamToObject(getClass().getClassLoader().getResourceAsStream("simpleMeetup_realtime_table_config.json"), TableConfig.class);
    addTableConfig(tableConfig);
    String tableName = tableConfig.getTableName();
    return tableName;
  }

  String createTableAndConsumeData()
      throws Exception {
    String tableName = createTable();
    LOGGER.info("Waiting for table {} to load some data", tableName);
    for (int i = 0; i < 60; i++) {
      if (!isTableLoaded(tableName)) {
        Thread.sleep(1000L);
      } else {
        break;
      }
    }
    LOGGER.info("Table {} has loaded some data", tableName);
    return tableName;
  }

  void waitForNumConsumingSegmentsInIS(String tableName, int desiredNumConsumingSegments) {
    TestUtils.waitForCondition((aVoid) -> {
      try {
        AtomicInteger numConsumingSegments = new AtomicInteger(0);
        String state = getIdealState(tableName);
        TableViews.TableView tableView = JsonUtils.stringToObject(state, TableViews.TableView.class);
        tableView._realtime.values().forEach((v) -> {
          numConsumingSegments.addAndGet((int) v.values().stream().filter((v1) -> v1.equals("CONSUMING")).count());
        });
        return numConsumingSegments.get() == desiredNumConsumingSegments;
      } catch (IOException e) {
        LOGGER.error("Exception in waitForNumConsumingSegmentsInIS: {}", e.getMessage());
        return false;
      }
    }, 5000, 600_000L, "Failed to wait for " + desiredNumConsumingSegments + " consuming segments for table: " + tableName);
  }

  void waitForNumConsumingSegmentsInEV(String tableName, int desiredNumConsumingSegments) {
    TestUtils.waitForCondition((aVoid) -> {
      try {
        AtomicInteger numConsumingSegments = new AtomicInteger(0);
        String state = getExternalView(tableName);
        TableViews.TableView tableView = JsonUtils.stringToObject(state, TableViews.TableView.class);
        tableView._realtime.values().forEach((v) -> {
          numConsumingSegments.addAndGet((int) v.values().stream().filter((v1) -> v1.equals("CONSUMING")).count());
        });
        return numConsumingSegments.get() == desiredNumConsumingSegments;
      } catch (IOException e) {
        LOGGER.error("Exception in waitForNumConsumingSegments: {}", e.getMessage());
        return false;
      }
    }, 5000, 600_000L, "Failed to wait for " + desiredNumConsumingSegments + " consuming segments for table: " + tableName);
  }

  @Test
  public void testDecreasePartitions()
      throws Exception {
    LOGGER.info("Starting testDecreasePartitions");
    LOGGER.info("Creating Kafka topic with {} partitions", NUM_PARTITIONS + 2);
    _kafkaStarters.get(0).createTopic(KAFKA_TOPIC, KafkaStarterUtils.getTopicCreationProps(NUM_PARTITIONS + 2));
    String tableName = createTable();
    waitForNumConsumingSegmentsInEV(tableName, NUM_PARTITIONS + 2);

    pauseTable(tableName);

    LOGGER.info("Deleting Kafka topic");
    _kafkaStarters.get(0).deleteTopic(KAFKA_TOPIC);
    LOGGER.info("Sleeping for 5 seconds before creating the topic again");
    Thread.sleep(5000);
    LOGGER.info("Creating Kafka topic with {} partitions", NUM_PARTITIONS);
    _kafkaStarters.get(0).createTopic(KAFKA_TOPIC, KafkaStarterUtils.getTopicCreationProps(NUM_PARTITIONS));

    resumeTable(tableName);
    waitForNumConsumingSegmentsInEV(tableName, NUM_PARTITIONS);
  }

  // org.apache.kafka.common.errors.InvalidPartitionsException: Topic currently has 5 partitions, which is higher than the requested 3.
  // @Test
  public void testIncreaseDecreasePartitions()
      throws Exception {
    LOGGER.info("Starting testIncreaseDecreasePartitions");
    LOGGER.info("Creating Kafka topic");
    _kafkaStarters.get(0).createTopic(KAFKA_TOPIC, KafkaStarterUtils.getTopicCreationProps(NUM_PARTITIONS));
    publish(100);

    String tableName = createTableAndConsumeData();

    publish(100);
    pauseResumeTable(tableName);

    LOGGER.info("Ideal state for table before increasing partition count: {}", getIdealState(tableName));

    // Increase the number of partitions
    LOGGER.info("Increasing the number of partitions to: {}", NUM_PARTITIONS + 2);
    _kafkaStarters.get(0).createPartitions(KAFKA_TOPIC,NUM_PARTITIONS + 2);
    LOGGER.info("Increased the number of partitions to: {}", NUM_PARTITIONS + 2);

    publish(100);
    pauseResumeTable(tableName);

    LOGGER.info("deal state for table after increasing partition count: {}", getIdealState(tableName));

    // Decrease the number of partitions
    LOGGER.info("Decreasing the number of partitions to: {}", NUM_PARTITIONS);
    // org.apache.kafka.common.errors.InvalidPartitionsException: Topic currently has 5 partitions, which is higher than the requested 3.
    _kafkaStarters.get(0).createPartitions(KAFKA_TOPIC, NUM_PARTITIONS);
    LOGGER.info("Decreased the number of partitions to: {}", NUM_PARTITIONS);

    publish(100);
    pauseResumeTable(tableName);
  }

  @Test(enabled = false)
  public void testDictionaryBasedQueries(boolean useMultiStageQueryEngine) {
    // Do nothing
  }

  @Test(enabled = false)
  public void testGeneratedQueries(boolean useMultiStageQueryEngine) {
    // Do nothing
  }

  @Test(enabled = false)
  public void testHardcodedQueries(boolean useMultiStageQueryEngine) {
    // Do nothing
  }

  @Test(enabled = false)
  public void testInstanceShutdown() {
    // Do nothing
  }

  @Test(enabled = false)
  public void testQueriesFromQueryFile(boolean useMultiStageQueryEngine) {
    // Do nothing
  }

  @Test(enabled = false)
  public void testQueryExceptions(boolean useMultiStageQueryEngine) {
    // Do nothing
  }

  @Test(enabled = false)
  public void testHardcodedServerPartitionedSqlQueries() {
    // Do nothing
  }
}
