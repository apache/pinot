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
package org.apache.pinot.integration.tests.custom;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.MultiTopicLLCSegmentName;
import org.apache.pinot.integration.tests.ClusterIntegrationTestUtils;
import org.apache.pinot.plugin.inputformat.csv.CSVMessageDecoder;
import org.apache.pinot.plugin.stream.kafka.KafkaStreamConfigProperties;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Integration test for a single realtime table that ingests from N Kafka topics using
 * the {@link StreamIngestionConfig#getStreamConfigMaps()} multi-stream configuration.
 *
 * <p>The number of topics is controlled by {@link #getNumTopics()}. For each topic index {@code i},
 * a Kafka topic named {@code multiTopicTest_topic{i}} is created and populated with
 * {@link #RECORDS_PER_TOPIC} CSV records whose {@code source} column is set to {@code TOPIC_{i}}
 * and whose {@code value} column falls in the exclusive range {@code [i*100 + 1, i*100 + 100]}.
 *
 * <p>Queries then verify that every topic's data is present and correctly isolated.
 * Subclasses can override {@link #getNumTopics()} to test with a different number of topics.
 */
public class MultiTopicRealtimeClusterIntegrationTest extends CustomDataQueryClusterIntegrationTest {
  private static final String TABLE_NAME = "multiTopicTable";
  private static final String TOPIC_PREFIX = "multiTopicTest_topic";
  private static final int NUM_PARTITIONS_PER_TOPIC = 2;
  private static final int RECORDS_PER_TOPIC = 500;
  private static final int VALUE_RANGE_PER_TOPIC = 100;
  private static final String CSV_DELIMITER = ",";
  private static final String CSV_HEADER = "id,name,source,value,ts";

  /**
   * Returns the number of Kafka topics to create. Override in subclasses to test
   * with a different topic count.
   */
  protected int getNumTopics() {
    return 3;
  }

  private String topicName(int topicIndex) {
    return TOPIC_PREFIX + topicIndex;
  }

  private String sourceName(int topicIndex) {
    int maxIndex = getNumTopics() - 1;
    int width = String.valueOf(maxIndex).length();
    return String.format("TOPIC_%0" + width + "d", topicIndex);
  }

  private int valueRangeStart(int topicIndex) {
    return topicIndex * VALUE_RANGE_PER_TOPIC + 1;
  }

  private int valueRangeEnd(int topicIndex) {
    return (topicIndex + 1) * VALUE_RANGE_PER_TOPIC;
  }

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  @Override
  public String getKafkaTopic() {
    return topicName(0);
  }

  @Override
  protected int getNumKafkaPartitions() {
    return NUM_PARTITIONS_PER_TOPIC;
  }

  @Override
  protected long getCountStarResult() {
    return (long) RECORDS_PER_TOPIC * getNumTopics();
  }

  @Override
  public boolean isRealtimeTable() {
    return true;
  }

  @Override
  protected String getSortedColumn() {
    return null;
  }

  @Override
  protected List<String> getInvertedIndexColumns() {
    return List.of("source");
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension("id", FieldSpec.DataType.INT)
        .addSingleValueDimension("name", FieldSpec.DataType.STRING)
        .addSingleValueDimension("source", FieldSpec.DataType.STRING)
        .addMetric("value", FieldSpec.DataType.INT)
        .addDateTime("ts", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
  }

  @Override
  public List<File> createAvroFiles() {
    return List.of();
  }

  protected IngestionConfig getIngestionConfig() {
    int numTopics = getNumTopics();
    List<Map<String, String>> streamConfigMaps = new ArrayList<>(numTopics);
    for (int i = 0; i < numTopics; i++) {
      streamConfigMaps.add(buildStreamConfigMap(topicName(i)));
    }
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setStreamIngestionConfig(new StreamIngestionConfig(streamConfigMaps));
    return ingestionConfig;
  }

  private Map<String, String> buildStreamConfigMap(String topicName) {
    String streamType = "kafka";
    Map<String, String> map = new HashMap<>();
    map.put(StreamConfigProperties.STREAM_TYPE, streamType);
    map.put(KafkaStreamConfigProperties.constructStreamProperty(
            KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_BROKER_LIST),
        getSharedKafkaBrokerList());
    map.put(StreamConfigProperties.constructStreamProperty(streamType,
        StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS), getStreamConsumerFactoryClassName());
    map.put(StreamConfigProperties.constructStreamProperty(streamType,
        StreamConfigProperties.STREAM_TOPIC_NAME), topicName);
    map.put(StreamConfigProperties.constructStreamProperty(streamType,
        StreamConfigProperties.STREAM_DECODER_CLASS), CSVMessageDecoder.class.getName());
    map.put(StreamConfigProperties.constructStreamProperty(streamType, "decoder.prop.delimiter"), CSV_DELIMITER);
    map.put(StreamConfigProperties.constructStreamProperty(streamType, "decoder.prop.header"), CSV_HEADER);
    map.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS,
        Integer.toString(getRealtimeSegmentFlushSize()));
    map.put(StreamConfigProperties.constructStreamProperty(streamType,
        StreamConfigProperties.STREAM_CONSUMER_OFFSET_CRITERIA), "smallest");
    return map;
  }

  private List<String> generateRecords(int topicIndex) {
    String source = sourceName(topicIndex);
    int valStart = valueRangeStart(topicIndex);
    List<String> records = new ArrayList<>(RECORDS_PER_TOPIC);
    long baseTs = 1700000000000L;
    for (int i = 0; i < RECORDS_PER_TOPIC; i++) {
      int id = topicIndex * RECORDS_PER_TOPIC + i;
      String name = "name_" + topicIndex + "_" + i;
      int value = valStart + (i % VALUE_RANGE_PER_TOPIC);
      long ts = baseTs + ((long) topicIndex * RECORDS_PER_TOPIC + i) * 1000L;
      records.add(String.join(CSV_DELIMITER,
          String.valueOf(id), name, source, String.valueOf(value), String.valueOf(ts)));
    }
    return records;
  }

  @Override
  @BeforeClass
  public void setUp()
      throws Exception {
    initControllerRequestURLBuilder();
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);

    int numTopics = getNumTopics();
    for (int i = 0; i < numTopics; i++) {
      createSharedKafkaTopic(topicName(i), NUM_PARTITIONS_PER_TOPIC);
    }

    Schema schema = createSchema();
    addSchema(schema);

    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(TABLE_NAME)
        .setTimeColumnName(getTimeColumnName())
        .setInvertedIndexColumns(getInvertedIndexColumns())
        .setNumReplicas(getNumReplicas())
        .setBrokerTenant(getBrokerTenant())
        .setServerTenant(getServerTenant())
        .setIngestionConfig(getIngestionConfig())
        .setNullHandlingEnabled(getNullHandlingEnabled())
        .build();
    addTableConfig(tableConfig);

    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME);
    int totalExpectedPartitions = NUM_PARTITIONS_PER_TOPIC * numTopics;
    TestUtils.waitForCondition(
        () -> getNumConsumingPartitions(realtimeTableName) == totalExpectedPartitions,
        200L, 120_000L,
        "Failed to get CONSUMING segments for all " + totalExpectedPartitions + " partitions",
        Duration.ofSeconds(12));

    for (int i = 0; i < numTopics; i++) {
      ClusterIntegrationTestUtils.pushCsvIntoKafka(generateRecords(i),
          getSharedKafkaBrokerList(), topicName(i), null, injectTombstones());
    }

    waitForAllDocsLoaded(600_000L);
  }

  private int getNumConsumingPartitions(String tableNameWithType) {
    IdealState idealState = getSharedHelixResourceManager().getTableIdealState(tableNameWithType);
    if (idealState == null) {
      return 0;
    }
    Set<Integer> consumingPartitions = new HashSet<>();
    for (String segmentName : idealState.getPartitionSet()) {
      if (!LLCSegmentName.isLLCSegment(segmentName)) {
        continue;
      }
      Map<String, String> stateMap = idealState.getInstanceStateMap(segmentName);
      if (stateMap != null
          && stateMap.containsValue(CommonConstants.Helix.StateModel.SegmentStateModel.CONSUMING)) {
        consumingPartitions.add(new LLCSegmentName(segmentName).getPartitionGroupId());
      }
    }
    return consumingPartitions.size();
  }

  @Test
  public void testTotalDocCount()
      throws Exception {
    JsonNode response = postQuery("SELECT COUNT(*) FROM " + TABLE_NAME);
    assertEquals(response.get("resultTable").get("rows").get(0).get(0).asLong(),
        (long) RECORDS_PER_TOPIC * getNumTopics(),
        "Total doc count should equal RECORDS_PER_TOPIC * numTopics");
  }

  @Test
  public void testGroupBySource()
      throws Exception {
    int numTopics = getNumTopics();
    JsonNode response = postQuery(
        "SELECT source, COUNT(*) AS cnt FROM " + TABLE_NAME + " GROUP BY source ORDER BY source");
    JsonNode rows = response.get("resultTable").get("rows");
    assertEquals(rows.size(), numTopics, "Should have exactly " + numTopics + " groups");

    for (int i = 0; i < numTopics; i++) {
      assertEquals(rows.get(i).get(0).asText(), sourceName(i),
          "Group " + i + " source name mismatch");
      assertEquals(rows.get(i).get(1).asLong(), RECORDS_PER_TOPIC,
          "Group " + i + " count mismatch");
    }
  }

  @Test
  public void testFilterBySource()
      throws Exception {
    for (int i = 0; i < getNumTopics(); i++) {
      String source = sourceName(i);
      JsonNode response = postQuery(
          "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE source = '" + source + "'");
      assertEquals(response.get("resultTable").get("rows").get(0).get(0).asLong(),
          (long) RECORDS_PER_TOPIC,
          "Filter on " + source + " should return exactly RECORDS_PER_TOPIC rows");
    }
  }

  @Test
  public void testValueRangesBySource()
      throws Exception {
    for (int i = 0; i < getNumTopics(); i++) {
      String source = sourceName(i);
      int expectedMin = valueRangeStart(i);
      int expectedMax = valueRangeEnd(i);
      JsonNode response = postQuery(
          "SELECT MIN(value), MAX(value) FROM " + TABLE_NAME + " WHERE source = '" + source + "'");
      JsonNode row = response.get("resultTable").get("rows").get(0);
      assertEquals(row.get(0).asInt(), expectedMin,
          source + " min value should be " + expectedMin + " but was " + row.get(0).asInt());
      assertEquals(row.get(1).asInt(), expectedMax,
          source + " max value should be " + expectedMax + " but was " + row.get(1).asInt());
    }
  }

  @Test
  public void testAggregationBySource()
      throws Exception {
    int numTopics = getNumTopics();
    JsonNode response = postQuery(
        "SELECT source, SUM(value) AS total FROM " + TABLE_NAME + " GROUP BY source ORDER BY source");
    JsonNode rows = response.get("resultTable").get("rows");
    assertEquals(rows.size(), numTopics);

    long previousSum = Long.MIN_VALUE;
    for (int i = 0; i < numTopics; i++) {
      long sum = rows.get(i).get(1).asLong();
      assertTrue(sum > 0, sourceName(i) + " sum should be positive");
      assertTrue(sum > previousSum,
          sourceName(i) + " sum (" + sum + ") should be larger than previous topic sum (" + previousSum + ")");
      previousSum = sum;
    }
  }

  @Test
  public void testCrossTopicQuery()
      throws Exception {
    int numTopics = getNumTopics();
    String orClauses = java.util.stream.IntStream.range(0, numTopics)
        .mapToObj(i -> "source = '" + sourceName(i) + "'")
        .collect(Collectors.joining(" OR "));
    JsonNode response = postQuery(
        "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE " + orClauses);
    assertEquals(response.get("resultTable").get("rows").get(0).get(0).asLong(),
        (long) RECORDS_PER_TOPIC * numTopics,
        "OR filter across all sources should return all records");
  }

  @Test
  public void testNoDataLeakBetweenTopics()
      throws Exception {
    for (int i = 0; i < getNumTopics(); i++) {
      String source = sourceName(i);
      int maxAllowed = valueRangeEnd(i);
      int minAllowed = valueRangeStart(i);

      JsonNode aboveRange = postQuery(
          "SELECT COUNT(*) FROM " + TABLE_NAME
              + " WHERE source = '" + source + "' AND value > " + maxAllowed);
      assertEquals(aboveRange.get("resultTable").get("rows").get(0).get(0).asLong(), 0L,
          source + " should have no values > " + maxAllowed);

      JsonNode belowRange = postQuery(
          "SELECT COUNT(*) FROM " + TABLE_NAME
              + " WHERE source = '" + source + "' AND value < " + minAllowed);
      assertEquals(belowRange.get("resultTable").get("rows").get(0).get(0).asLong(), 0L,
          source + " should have no values < " + minAllowed);
    }
  }

  @Test
  public void testSegmentsFromAllTopics() {
    int numTopics = getNumTopics();
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME);
    IdealState idealState = getSharedHelixResourceManager().getTableIdealState(realtimeTableName);
    assertNotNull(idealState);

    Set<Integer> topicIndicesSeen = new HashSet<>();
    for (String segmentName : idealState.getPartitionSet()) {
      if (LLCSegmentName.isLLCSegment(segmentName)) {
        int pgId = new LLCSegmentName(segmentName).getPartitionGroupId();
        topicIndicesSeen.add(IngestionConfigUtils.getStreamConfigIndexFromPinotPartitionId(pgId));
      }
    }
    for (int i = 0; i < numTopics; i++) {
      assertTrue(topicIndicesSeen.contains(i),
          "Should have segments from topic " + i + " (stream config index " + i + ")");
    }
  }

  @Test
  public void testDistinctSources()
      throws Exception {
    int numTopics = getNumTopics();
    JsonNode response = postQuery(
        "SELECT DISTINCT source FROM " + TABLE_NAME + " ORDER BY source LIMIT " + (numTopics + 5));
    JsonNode rows = response.get("resultTable").get("rows");
    assertEquals(rows.size(), numTopics, "Should have exactly " + numTopics + " distinct source values");

    for (int i = 0; i < numTopics; i++) {
      assertEquals(rows.get(i).get(0).asText(), sourceName(i));
    }
  }

  @Test
  public void testStreamConfigIdsAssigned() {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME);
    TableConfig tableConfig = getSharedHelixResourceManager().getTableConfig(realtimeTableName);
    assertNotNull(tableConfig, "Table config should exist");

    StreamIngestionConfig streamIngestionConfig =
        tableConfig.getIngestionConfig().getStreamIngestionConfig();
    assertNotNull(streamIngestionConfig, "Stream ingestion config should exist");

    List<Map<String, String>> streamConfigMaps = streamIngestionConfig.getStreamConfigMaps();
    int numTopics = getNumTopics();
    assertEquals(streamConfigMaps.size(), numTopics, "Should have " + numTopics + " stream configs");

    Set<Integer> configIds = new HashSet<>();
    for (int i = 0; i < numTopics; i++) {
      Map<String, String> configMap = streamConfigMaps.get(i);
      String configIdStr = configMap.get(StreamConfigProperties.STREAM_CONFIG_ID);
      assertNotNull(configIdStr, "stream.config.id should be set for stream config at index " + i);

      int configId = Integer.parseInt(configIdStr);
      assertTrue(configId >= 0, "Config ID should be non-negative, got: " + configId);
      assertTrue(configIds.add(configId), "Config IDs should be unique, duplicate: " + configId);
    }

    assertEquals(streamIngestionConfig.getNextStreamConfigId(), numTopics,
        "nextStreamConfigId should equal the number of topics");
  }

  @Test
  public void testMultiTopicSegmentNameFormat() {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME);
    IdealState idealState = getSharedHelixResourceManager().getTableIdealState(realtimeTableName);
    assertNotNull(idealState);

    int numTopics = getNumTopics();
    if (numTopics <= 1) {
      return;
    }

    int multiTopicSegmentCount = 0;
    Set<Integer> configIdsSeen = new HashSet<>();

    for (String segmentName : idealState.getPartitionSet()) {
      MultiTopicLLCSegmentName multiTopicName = MultiTopicLLCSegmentName.of(segmentName);
      if (multiTopicName != null) {
        multiTopicSegmentCount++;
        configIdsSeen.add(multiTopicName.getConfigId());

        assertTrue(multiTopicName.getConfigId() >= 0,
            "Config ID should be non-negative: " + segmentName);
        assertTrue(multiTopicName.getStreamPartitionId() >= 0,
            "Stream partition ID should be non-negative: " + segmentName);
        assertTrue(multiTopicName.getStreamPartitionId() < NUM_PARTITIONS_PER_TOPIC,
            "Stream partition ID should be < " + NUM_PARTITIONS_PER_TOPIC + ": " + segmentName);

        int expectedPartitionGroupId =
            IngestionConfigUtils.getPinotPartitionIdFromConfigId(
                multiTopicName.getStreamPartitionId(), multiTopicName.getConfigId());
        assertEquals(multiTopicName.getPartitionGroupId(), expectedPartitionGroupId,
            "Partition group ID encoding mismatch for segment: " + segmentName);

        LLCSegmentName llcParsed = new LLCSegmentName(segmentName);
        assertEquals(llcParsed.getPartitionGroupId(), expectedPartitionGroupId,
            "LLCSegmentName should parse 5-part format and produce same partition group ID");
      }
    }

    assertTrue(multiTopicSegmentCount > 0,
        "Should have at least one segment in 5-part multi-topic format");

    for (int i = 0; i < numTopics; i++) {
      assertTrue(configIdsSeen.contains(i),
          "Should have segments with config ID " + i);
    }
  }

  @Test
  public void testConfigIdLookupMethods() {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME);
    TableConfig tableConfig = getSharedHelixResourceManager().getTableConfig(realtimeTableName);
    assertNotNull(tableConfig);

    StreamIngestionConfig streamIngestionConfig =
        tableConfig.getIngestionConfig().getStreamIngestionConfig();
    int numTopics = getNumTopics();

    for (int i = 0; i < numTopics; i++) {
      int configId = streamIngestionConfig.getConfigId(i);
      assertEquals(configId, i, "Config ID at index " + i + " should be " + i);

      Map<String, String> configMap = streamIngestionConfig.getStreamConfigMapByConfigId(configId);
      assertNotNull(configMap, "Should find stream config for config ID " + configId);

      String topicName = configMap.get(
          StreamConfigProperties.constructStreamProperty("kafka", StreamConfigProperties.STREAM_TOPIC_NAME));
      assertEquals(topicName, topicName(i),
          "Topic name for config ID " + configId + " should match");
    }

    Map<Integer, Map<String, String>> configIdMap = streamIngestionConfig.getConfigIdToStreamConfigMap();
    assertEquals(configIdMap.size(), numTopics, "Config ID map should have " + numTopics + " entries");
    for (int i = 0; i < numTopics; i++) {
      assertTrue(configIdMap.containsKey(i), "Config ID map should contain key " + i);
    }
  }

  @Test
  public void testSegmentNameBackwardCompatibility() {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME);
    IdealState idealState = getSharedHelixResourceManager().getTableIdealState(realtimeTableName);
    assertNotNull(idealState);

    for (String segmentName : idealState.getPartitionSet()) {
      if (!LLCSegmentName.isLLCSegment(segmentName)) {
        continue;
      }

      LLCSegmentName llcName = LLCSegmentName.of(segmentName);
      assertNotNull(llcName, "LLCSegmentName.of() should parse segment: " + segmentName);

      MultiTopicLLCSegmentName multiTopicName = MultiTopicLLCSegmentName.of(segmentName);
      if (multiTopicName != null) {
        assertEquals(llcName.getPartitionGroupId(), multiTopicName.getPartitionGroupId(),
            "Partition group ID should match between LLCSegmentName and MultiTopicLLCSegmentName "
                + "for segment: " + segmentName);
        assertEquals(llcName.getTableName(), multiTopicName.getTableName());
        assertEquals(llcName.getSequenceNumber(), multiTopicName.getSequenceNumber());
      }
    }
  }
}
