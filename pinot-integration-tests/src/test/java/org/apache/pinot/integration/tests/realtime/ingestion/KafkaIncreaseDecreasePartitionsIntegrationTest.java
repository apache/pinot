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
package org.apache.pinot.integration.tests.realtime.ingestion;

import java.io.IOException;
import org.apache.pinot.integration.tests.BaseRealtimeClusterIntegrationTest;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.tools.utils.KafkaStarterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;


public class KafkaIncreaseDecreasePartitionsIntegrationTest extends BaseRealtimeClusterIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaIncreaseDecreasePartitionsIntegrationTest.class);

  private static final String KAFKA_TOPIC = "meetup";
  private static final int NUM_PARTITIONS = 1;

  String createTable()
      throws IOException {
    Schema schema = createSchema("simpleMeetup_schema.json");
    addSchema(schema);
    TableConfig tableConfig = JsonUtils.inputStreamToObject(
        getClass().getClassLoader().getResourceAsStream("simpleMeetup_realtime_table_config.json"), TableConfig.class);
    addTableConfig(tableConfig);
    return tableConfig.getTableName();
  }

  @Test
  public void testDecreasePartitions()
      throws Exception {
    LOGGER.info("Starting testDecreasePartitions");
    LOGGER.info("Creating Kafka topic with {} partitions", NUM_PARTITIONS + 2);
    _kafkaStarters.get(0).createTopic(KAFKA_TOPIC, KafkaStarterUtils.getTopicCreationProps(NUM_PARTITIONS + 2));
    String tableName = createTable();
    waitForNumSegmentsInDesiredStateInEV(tableName, "CONSUMING", NUM_PARTITIONS + 2);

    pauseTable(tableName);

    LOGGER.info("Deleting Kafka topic");
    _kafkaStarters.get(0).deleteTopic(KAFKA_TOPIC);
    LOGGER.info("Creating Kafka topic with {} partitions", NUM_PARTITIONS);
    _kafkaStarters.get(0).createTopic(KAFKA_TOPIC, KafkaStarterUtils.getTopicCreationProps(NUM_PARTITIONS));

    resumeTable(tableName);
    waitForNumSegmentsInDesiredStateInEV(tableName, "CONSUMING", NUM_PARTITIONS);
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
