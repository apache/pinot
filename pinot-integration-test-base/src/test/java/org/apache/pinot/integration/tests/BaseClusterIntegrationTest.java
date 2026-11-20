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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskState;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.pinot.client.ConnectionFactory;
import org.apache.pinot.client.JsonAsyncHttpPinotClientTransportFactory;
import org.apache.pinot.client.ResultSetGroup;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.restlet.resources.TableMetadataInfo;
import org.apache.pinot.common.restlet.resources.ValidDocIdsMetadataInfo;
import org.apache.pinot.common.restlet.resources.ValidDocIdsType;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.util.ConsumingSegmentInfoReader.ConsumingSegmentInfo;
import org.apache.pinot.controller.util.ConsumingSegmentInfoReader.ConsumingSegmentsInfoMap;
import org.apache.pinot.controller.util.ConsumingSegmentInfoReader.PartitionOffsetInfo;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.plugin.inputformat.csv.CSVMessageDecoder;
import org.apache.pinot.plugin.stream.kafka.KafkaStreamConfigProperties;
import org.apache.pinot.plugin.stream.kafka40.server.EmbeddedKafkaCluster;
import org.apache.pinot.server.starter.helix.BaseServerStarter;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.QueryConfig;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.PhysicalTableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeBoundaryConfig;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.stream.StreamDataServerStartable;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.Enablement;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.LogicalTableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.apache.pinot.tools.utils.KafkaStarterUtils;
import org.apache.pinot.util.TestUtils;
import org.intellij.lang.annotations.Language;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Shared implementation details of the cluster integration tests.
public abstract class BaseClusterIntegrationTest extends ClusterTest {
  /// Queries [#assertRowsFromEveryReplica] sends at most; the broker alternates replicas per request.
  private static final int MAX_QUERIES_TO_REACH_EVERY_REPLICA = 20;
  // Default settings
  protected static final String DEFAULT_TABLE_NAME = "mytable";
  protected static final String DEFAULT_LOGICAL_TABLE_NAME = "mytable_logical";
  protected static final String DEFAULT_SCHEMA_NAME = "mytable";
  protected static final String DEFAULT_SCHEMA_FILE_NAME =
      "On_Time_On_Time_Performance_2014_100k_subset_nonulls.schema";
  protected static final String DEFAULT_TIME_COLUMN_NAME = "DaysSinceEpoch";
  protected static final String DEFAULT_AVRO_TAR_FILE_NAME =
      "On_Time_On_Time_Performance_2014_100k_subset_nonulls.tar.gz";
  protected static final long DEFAULT_COUNT_STAR_RESULT = 115545L;
  protected static final int DEFAULT_LLC_SEGMENT_FLUSH_SIZE = 5000;
  protected static final int DEFAULT_TRANSACTION_NUM_KAFKA_BROKERS = 3;
  protected static final int DEFAULT_LLC_NUM_KAFKA_BROKERS = 2;
  protected static final int DEFAULT_LLC_NUM_KAFKA_PARTITIONS = 2;
  protected static final int DEFAULT_MAX_NUM_KAFKA_MESSAGES_PER_BATCH = 10000;
  private static final long KAFKA_CLUSTER_READY_TIMEOUT_MS = 180_000L;
  private static final long KAFKA_TOPIC_READY_TIMEOUT_MS = 180_000L;
  protected static final List<String> DEFAULT_NO_DICTIONARY_COLUMNS =
      Arrays.asList("ActualElapsedTime", "ArrDelay", "DepDelay", "CRSDepTime");
  protected static final String DEFAULT_SORTED_COLUMN = "Carrier";
  protected static final List<String> DEFAULT_INVERTED_INDEX_COLUMNS = Arrays.asList("FlightNum", "Origin", "Quarter");
  private static final List<String> DEFAULT_BLOOM_FILTER_COLUMNS = Arrays.asList("FlightNum", "Origin");
  private static final List<String> DEFAULT_RANGE_INDEX_COLUMNS = List.of("Origin");
  protected static final int DEFAULT_NUM_REPLICAS = 1;
  protected static final boolean DEFAULT_NULL_HANDLING_ENABLED = false;

  protected final File _tempDir = new File(FileUtils.getTempDirectory(), getClass().getSimpleName());
  protected final File _segmentDir = new File(_tempDir, "segmentDir");
  protected final File _tarDir = new File(_tempDir, "tarDir");
  protected List<StreamDataServerStartable> _kafkaStarters;

  protected org.apache.pinot.client.Connection _pinotConnection;
  protected org.apache.pinot.client.Connection _pinotConnectionV2;
  protected Connection _h2Connection;
  protected QueryGenerator _queryGenerator;

  /// The following getters can be overridden to change default settings.

  protected String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  protected String getLogicalTableName() {
    return DEFAULT_LOGICAL_TABLE_NAME;
  }

  protected String getSchemaFileName() {
    return DEFAULT_SCHEMA_FILE_NAME;
  }

  @Nullable
  protected String getTimeColumnName() {
    return DEFAULT_TIME_COLUMN_NAME;
  }

  protected String getAvroTarFileName() {
    return DEFAULT_AVRO_TAR_FILE_NAME;
  }

  protected long getCountStarResult() {
    return DEFAULT_COUNT_STAR_RESULT;
  }

  protected boolean useKafkaTransaction() {
    return false;
  }

  protected String getStreamConsumerFactoryClassName() {
    return KafkaStarterUtils.KAFKA_STREAM_CONSUMER_FACTORY_CLASS_NAME;
  }

  protected int getRealtimeSegmentFlushSize() {
    return DEFAULT_LLC_SEGMENT_FLUSH_SIZE;
  }

  protected int getNumKafkaBrokers() {
    return useKafkaTransaction() ? DEFAULT_TRANSACTION_NUM_KAFKA_BROKERS : DEFAULT_LLC_NUM_KAFKA_BROKERS;
  }

  /// Override to pass extra Kafka broker config properties when starting the embedded Kafka cluster.
  protected Properties getKafkaExtraProperties() {
    return new Properties();
  }

  protected int getKafkaPort() {
    int idx = RANDOM.nextInt(_kafkaStarters.size());
    return _kafkaStarters.get(idx).getPort();
  }

  protected int getNumKafkaPartitions() {
    return DEFAULT_LLC_NUM_KAFKA_PARTITIONS;
  }

  protected String getKafkaTopic() {
    return getClass().getSimpleName();
  }

  protected int getMaxNumKafkaMessagesPerBatch() {
    return DEFAULT_MAX_NUM_KAFKA_MESSAGES_PER_BATCH;
  }

  @Nullable
  protected byte[] getKafkaMessageHeader() {
    return null;
  }

  @Nullable
  protected String getPartitionColumn() {
    return null;
  }

  @Nullable
  protected String getSortedColumn() {
    return DEFAULT_SORTED_COLUMN;
  }

  @Nullable
  protected List<String> getInvertedIndexColumns() {
    return new ArrayList<>(DEFAULT_INVERTED_INDEX_COLUMNS);
  }

  @Nullable
  protected List<String> getNoDictionaryColumns() {
    return new ArrayList<>(DEFAULT_NO_DICTIONARY_COLUMNS);
  }

  @Nullable
  protected List<String> getRangeIndexColumns() {
    return new ArrayList<>(DEFAULT_RANGE_INDEX_COLUMNS);
  }

  @Nullable
  protected RoutingConfig getRoutingConfig() {
    // Default routing config is handled by broker
    return null;
  }

  @Nullable
  protected UpsertConfig getUpsertConfig() {
    return null;
  }

  @Nullable
  protected List<String> getBloomFilterColumns() {
    return new ArrayList<>(DEFAULT_BLOOM_FILTER_COLUMNS);
  }

  @Nullable
  protected List<FieldConfig> getFieldConfigs() {
    return null;
  }

  protected int getNumReplicas() {
    return DEFAULT_NUM_REPLICAS;
  }

  @Nullable
  protected String getSegmentVersion() {
    return null;
  }

  @Nullable
  protected String getLoadMode() {
    return null;
  }

  @Nullable
  protected TableTaskConfig getTaskConfig() {
    return null;
  }

  @Nullable
  protected String getBrokerTenant() {
    return TagNameUtils.DEFAULT_TENANT_NAME;
  }

  @Nullable
  protected String getServerTenant() {
    return TagNameUtils.DEFAULT_TENANT_NAME;
  }

  @Nullable
  protected IngestionConfig getIngestionConfig() {
    return null;
  }

  protected QueryConfig getQueryConfig() {
    // Enable groovy for tables used in the tests
    return new QueryConfig(null, false, null, null, null, null);
  }

  protected boolean getNullHandlingEnabled() {
    return DEFAULT_NULL_HANDLING_ENABLED;
  }

  @Nullable
  protected SegmentPartitionConfig getSegmentPartitionConfig() {
    return null;
  }

  /// Creates a new schema.
  protected Schema createSchema()
      throws IOException {
    Schema schema = createSchema(getSchemaFileName());
    schema.setSchemaName(getTableName());
    return schema;
  }

  protected Schema createSchema(String schemaFileName)
      throws IOException {
    InputStream inputStream = getClass().getClassLoader().getResourceAsStream(schemaFileName);
    assertNotNull(inputStream);
    return Schema.fromInputStream(inputStream);
  }

  protected Schema createSchema(File schemaFile)
      throws IOException {
    return Schema.fromInputStream(new FileInputStream(schemaFile));
  }

  protected TableConfig createTableConfig(String tableConfigFileName)
      throws IOException {
    URL configPathUrl = getClass().getClassLoader().getResource(tableConfigFileName);
    assertNotNull(configPathUrl);
    return createTableConfig(new File(configPathUrl.getFile()));
  }

  protected TableConfig createTableConfig(File tableConfigFile)
      throws IOException {
    InputStream inputStream = new FileInputStream(tableConfigFile);
    assertNotNull(inputStream);
    return JsonUtils.inputStreamToObject(inputStream, TableConfig.class);
  }

  /// Creates a new OFFLINE table config.
  protected TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(getTableName())
        .setTimeColumnName(getTimeColumnName())
        .setSortedColumn(getSortedColumn())
        .setInvertedIndexColumns(getInvertedIndexColumns())
        .setNoDictionaryColumns(getNoDictionaryColumns())
        .setRangeIndexColumns(getRangeIndexColumns())
        .setBloomFilterColumns(getBloomFilterColumns())
        .setFieldConfigList(getFieldConfigs())
        .setNumReplicas(getNumReplicas())
        .setSegmentVersion(getSegmentVersion())
        .setLoadMode(getLoadMode())
        .setTaskConfig(getTaskConfig())
        .setBrokerTenant(getBrokerTenant())
        .setServerTenant(getServerTenant())
        .setIngestionConfig(getIngestionConfig())
        .setQueryConfig(getQueryConfig())
        .setNullHandlingEnabled(getNullHandlingEnabled())
        .setSegmentPartitionConfig(getSegmentPartitionConfig())
        .setOptimizeNoDictStatsCollection(true)
        .build();
  }

  /// Returns the OFFLINE table config in the cluster.
  protected TableConfig getOfflineTableConfig() {
    return getOfflineTableConfig(getTableName());
  }

  protected Map<String, String> getStreamConfigs() {
    return getStreamConfigMap();
  }

  protected Map<String, String> getStreamConfigMap() {
    Map<String, String> streamConfigMap = new HashMap<>();
    String streamType = "kafka";
    streamConfigMap.put(StreamConfigProperties.STREAM_TYPE, streamType);
    streamConfigMap.put(KafkaStreamConfigProperties.constructStreamProperty(
        KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_BROKER_LIST), getKafkaBrokerList());
    if (useKafkaTransaction()) {
      streamConfigMap.put(KafkaStreamConfigProperties.constructStreamProperty(
              KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_ISOLATION_LEVEL),
          KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_ISOLATION_LEVEL_READ_COMMITTED);
      // Ensure the consumer can fetch complete transactional batches plus commit markers.
      streamConfigMap.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, Integer.toString(10 * 1024 * 1024));
    }
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(streamType,
        StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS), getStreamConsumerFactoryClassName());
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_TOPIC_NAME),
        getKafkaTopic());
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_DECODER_CLASS),
        AvroFileSchemaKafkaAvroMessageDecoder.class.getName());
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS,
        Integer.toString(getRealtimeSegmentFlushSize()));
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(streamType,
        StreamConfigProperties.STREAM_CONSUMER_OFFSET_CRITERIA), "smallest");
    return streamConfigMap;
  }

  protected String getKafkaBrokerList() {
    StreamDataServerStartable firstStarter = _kafkaStarters.get(0);
    if (firstStarter instanceof EmbeddedKafkaCluster) {
      return ((EmbeddedKafkaCluster) firstStarter).bootstrapServers();
    }
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < _kafkaStarters.size(); i++) {
      if (i > 0) {
        builder.append(',');
      }
      builder.append("localhost:").append(_kafkaStarters.get(i).getPort());
    }
    return builder.toString();
  }

  /// Creates a new REALTIME table config.
  protected TableConfig createRealtimeTableConfig(File sampleAvroFile) {
    AvroFileSchemaKafkaAvroMessageDecoder._avroFile = sampleAvroFile;
    return getTableConfigBuilder(TableType.REALTIME).build();
  }

  /// Creates a LogicalTableConfig backed by the OFFLINE and REALTIME physical tables.
  protected LogicalTableConfig createLogicalTableConfig() {
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(getTableName());

    Map<String, PhysicalTableConfig> physicalTableConfigMap = new HashMap<>();
    physicalTableConfigMap.put(offlineTableName, new PhysicalTableConfig());
    physicalTableConfigMap.put(realtimeTableName, new PhysicalTableConfig());

    return new LogicalTableConfigBuilder().setTableName(getLogicalTableName())
        .setBrokerTenant(getBrokerTenant())
        .setRefOfflineTableName(offlineTableName)
        .setRefRealtimeTableName(realtimeTableName)
        .setPhysicalTableConfigMap(physicalTableConfigMap)
        .setTimeBoundaryConfig(new TimeBoundaryConfig("min", Map.of("includedTables", physicalTableConfigMap.keySet())))
        .build();
  }

  /// Registers a logical table backed by the OFFLINE and REALTIME physical tables.
  /// The schema for the logical table should be created separately before calling this method.
  protected void createLogicalTable()
      throws Exception {
    LogicalTableConfig logicalTableConfig = createLogicalTableConfig();
    getOrCreateAdminClient().getLogicalTableClient().createLogicalTable(logicalTableConfig.toSingleLineJsonString());
  }

  // TODO - Use this method to create table config for all table types to avoid redundant code
  protected TableConfigBuilder getTableConfigBuilder(TableType tableType) {
    return new TableConfigBuilder(tableType).setTableName(getTableName())
        .setTimeColumnName(getTimeColumnName())
        .setSortedColumn(getSortedColumn())
        .setInvertedIndexColumns(getInvertedIndexColumns())
        .setNoDictionaryColumns(getNoDictionaryColumns())
        .setRangeIndexColumns(getRangeIndexColumns())
        .setRoutingConfig(getRoutingConfig())
        .setUpsertConfig(getUpsertConfig())
        .setBloomFilterColumns(getBloomFilterColumns())
        .setFieldConfigList(getFieldConfigs())
        .setNumReplicas(getNumReplicas())
        .setSegmentVersion(getSegmentVersion())
        .setLoadMode(getLoadMode())
        .setTaskConfig(getTaskConfig())
        .setBrokerTenant(getBrokerTenant())
        .setServerTenant(getServerTenant())
        .setIngestionConfig(getIngestionConfig())
        .setQueryConfig(getQueryConfig())
        .setStreamConfigs(getStreamConfigs())
        .setNullHandlingEnabled(getNullHandlingEnabled())
        .setSegmentPartitionConfig(getSegmentPartitionConfig())
        .setOptimizeNoDictStatsCollection(true);
  }

  /// Creates a new Upsert enabled table config.
  protected TableConfig createUpsertTableConfig(File sampleAvroFile, String primaryKeyColumn, String deleteColumn,
      int numPartitions) {
    AvroFileSchemaKafkaAvroMessageDecoder._avroFile = sampleAvroFile;
    Map<String, ColumnPartitionConfig> columnPartitionConfigMap = new HashMap<>();
    columnPartitionConfigMap.put(primaryKeyColumn, new ColumnPartitionConfig("Murmur", numPartitions));

    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setDeleteRecordColumn(deleteColumn);

    return new TableConfigBuilder(TableType.REALTIME).setTableName(getTableName())
        .setTimeColumnName(getTimeColumnName())
        .setFieldConfigList(getFieldConfigs())
        .setNumReplicas(getNumReplicas())
        .setSegmentVersion(getSegmentVersion())
        .setLoadMode(getLoadMode())
        .setTaskConfig(getTaskConfig())
        .setBrokerTenant(getBrokerTenant())
        .setServerTenant(getServerTenant())
        .setIngestionConfig(getIngestionConfig())
        .setStreamConfigs(getStreamConfigs())
        .setNullHandlingEnabled(getNullHandlingEnabled())
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .setSegmentPartitionConfig(new SegmentPartitionConfig(columnPartitionConfigMap))
        .setOptimizeNoDictStatsCollection(true)
        .setUpsertConfig(upsertConfig)
        .build();
  }

  protected Map<String, String> getCSVDecoderProperties(@Nullable String delimiter,
      @Nullable String csvHeaderProperty) {
    String streamType = "kafka";
    Map<String, String> csvDecoderProperties = new HashMap<>();
    csvDecoderProperties.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_DECODER_CLASS),
        CSVMessageDecoder.class.getName());
    if (delimiter != null) {
      csvDecoderProperties.put(StreamConfigProperties.constructStreamProperty(streamType, "decoder.prop.delimiter"),
          delimiter);
    }
    if (csvHeaderProperty != null) {
      csvDecoderProperties.put(StreamConfigProperties.constructStreamProperty(streamType, "decoder.prop.header"),
          csvHeaderProperty);
    }
    return csvDecoderProperties;
  }

  /// Creates a new Upsert enabled table config.
  protected TableConfig createCSVUpsertTableConfig(String tableName, @Nullable String kafkaTopicName, int numPartitions,
      Map<String, String> streamDecoderProperties, UpsertConfig upsertConfig, String primaryKeyColumn) {
    Map<String, ColumnPartitionConfig> columnPartitionConfigMap = new HashMap<>();
    columnPartitionConfigMap.put(primaryKeyColumn, new ColumnPartitionConfig("Murmur", numPartitions));

    if (upsertConfig == null) {
      upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
      upsertConfig.setSnapshot(Enablement.ENABLE);
    }
    if (kafkaTopicName == null) {
      kafkaTopicName = getKafkaTopic();
    }

    Map<String, String> streamConfigsMap = getStreamConfigMap();
    streamConfigsMap.put(
        StreamConfigProperties.constructStreamProperty("kafka", StreamConfigProperties.STREAM_TOPIC_NAME),
        kafkaTopicName);
    streamConfigsMap.putAll(streamDecoderProperties);

    return new TableConfigBuilder(TableType.REALTIME).setTableName(tableName)
        .setTimeColumnName(getTimeColumnName())
        .setFieldConfigList(getFieldConfigs())
        .setNumReplicas(getNumReplicas())
        .setSegmentVersion(getSegmentVersion())
        .setLoadMode(getLoadMode())
        .setTaskConfig(getTaskConfig())
        .setBrokerTenant(getBrokerTenant())
        .setServerTenant(getServerTenant())
        .setIngestionConfig(getIngestionConfig())
        .setStreamConfigs(streamConfigsMap)
        .setNullHandlingEnabled(UpsertConfig.Mode.PARTIAL.equals(upsertConfig.getMode()) || getNullHandlingEnabled())
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .setSegmentPartitionConfig(new SegmentPartitionConfig(columnPartitionConfigMap))
        .setOptimizeNoDictStatsCollection(true)
        .setUpsertConfig(upsertConfig)
        .build();
  }

  /// Creates a new Dedup enabled table config
  protected TableConfig createDedupTableConfig(File sampleAvroFile, String primaryKeyColumn, int numPartitions) {
    AvroFileSchemaKafkaAvroMessageDecoder._avroFile = sampleAvroFile;
    Map<String, ColumnPartitionConfig> columnPartitionConfigMap = new HashMap<>();
    columnPartitionConfigMap.put(primaryKeyColumn, new ColumnPartitionConfig("Murmur", numPartitions));

    return new TableConfigBuilder(TableType.REALTIME).setTableName(getTableName())
        .setTimeColumnName(getTimeColumnName())
        .setFieldConfigList(getFieldConfigs())
        .setNumReplicas(getNumReplicas())
        .setSegmentVersion(getSegmentVersion())
        .setLoadMode(getLoadMode())
        .setTaskConfig(getTaskConfig())
        .setBrokerTenant(getBrokerTenant())
        .setServerTenant(getServerTenant())
        .setIngestionConfig(getIngestionConfig())
        .setNullHandlingEnabled(getNullHandlingEnabled())
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .setSegmentPartitionConfig(new SegmentPartitionConfig(columnPartitionConfigMap))
        .setDedupConfig(new DedupConfig())
        .setOptimizeNoDictStatsCollection(true)
        .build();
  }

  /// Returns the REALTIME table config in the cluster.
  protected TableConfig getRealtimeTableConfig() {
    return getRealtimeTableConfig(getTableName());
  }

  /// Returns the headers to be used for the connection to Pinot cluster.
  /// [org.apache.pinot.client.PinotClientTransportFactory]
  protected Map<String, String> getPinotClientTransportHeaders() {
    return Map.of();
  }

  /// Get the Pinot connection.
  ///
  /// @return Pinot connection
  protected org.apache.pinot.client.Connection getPinotConnection() {
    // TODO: This code is assuming getPinotConnectionProperties() will always return the same values
    if (useMultiStageQueryEngine()) {
      if (_pinotConnectionV2 == null) {
        Properties properties = getPinotConnectionProperties();
        properties.put("useMultistageEngine", "true");
        _pinotConnectionV2 = ConnectionFactory.fromZookeeper(getZkUrl() + "/" + getHelixClusterName(),
            new JsonAsyncHttpPinotClientTransportFactory().withConnectionProperties(properties).buildTransport());
      }
      return _pinotConnectionV2;
    }
    if (_pinotConnection == null) {
      JsonAsyncHttpPinotClientTransportFactory factory =
          new JsonAsyncHttpPinotClientTransportFactory().withConnectionProperties(getPinotConnectionProperties());
      factory.setHeaders(getPinotClientTransportHeaders());
      _pinotConnection =
          ConnectionFactory.fromZookeeper(getZkUrl() + "/" + getHelixClusterName(), factory.buildTransport());
    }
    return _pinotConnection;
  }

  protected Properties getPinotConnectionProperties() {
    Properties properties = new Properties();
    properties.putAll(getExtraQueryProperties());
    return properties;
  }

  /// Get the H2 connection. H2 connection must be set up before calling this method.
  ///
  /// @return H2 connection
  protected Connection getH2Connection() {
    assertNotNull(_h2Connection, "H2 Connection has not been initialized");
    return _h2Connection;
  }

  /// Get the query generator. Query generator must be set up before calling this method.
  ///
  /// @return Query generator.
  protected QueryGenerator getQueryGenerator() {
    assertNotNull(_queryGenerator, "Query Generator has not been initialized");
    return _queryGenerator;
  }

  /// Sets up the H2 connection
  protected void setUpH2Connection()
      throws Exception {
    assertNull(_h2Connection);
    Class.forName("org.h2.Driver");
    _h2Connection = DriverManager.getConnection("jdbc:h2:mem:");
  }

  /// Sets up the H2 connection to a table with pre-loaded data.
  protected void setUpH2Connection(List<File> avroFiles)
      throws Exception {
    setUpH2Connection();
    ClusterIntegrationTestUtils.setUpH2TableWithAvro(avroFiles, getTableName(), _h2Connection);
  }

  /// Sets up the query generator using the given Avro files.
  protected void setUpQueryGenerator(List<File> avroFiles) {
    assertNull(_queryGenerator);
    String tableName = getTableName();
    _queryGenerator = new QueryGenerator(avroFiles, tableName, tableName);
  }

  protected List<File> unpackAvroData(File outputDir)
      throws Exception {
    return unpackTarData(getAvroTarFileName(), outputDir);
  }

  /// Unpack the tarred data into the given directory.
  ///
  /// @param tarFileName Input tar filename
  /// @param outputDir Output directory
  /// @return List of files unpacked.
  /// @throws Exception
  protected List<File> unpackTarData(String tarFileName, File outputDir)
      throws Exception {
    InputStream inputStream = getClass().getClassLoader().getResourceAsStream(tarFileName);
    assertNotNull(inputStream);
    return TarCompressionUtils.untar(inputStream, outputDir);
  }

  protected void pushAvroIntoKafka(List<File> avroFiles)
      throws Exception {
    ClusterIntegrationTestUtils.pushAvroIntoKafka(avroFiles, "localhost:" + getKafkaPort(), getKafkaTopic(),
        getMaxNumKafkaMessagesPerBatch(), getKafkaMessageHeader(), getPartitionColumn(), injectTombstones());
  }

  protected void pushCsvIntoKafka(File csvFile, String kafkaTopic, @Nullable Integer partitionColumnIndex)
      throws Exception {
    ClusterIntegrationTestUtils.pushCsvIntoKafka(csvFile, "localhost:" + getKafkaPort(), kafkaTopic,
        partitionColumnIndex, injectTombstones());
  }

  protected void pushCsvIntoKafka(List<String> csvRecords, String kafkaTopic, @Nullable Integer partitionColumnIndex)
      throws Exception {
    ClusterIntegrationTestUtils.pushCsvIntoKafka(csvRecords, "localhost:" + getKafkaPort(), kafkaTopic,
        partitionColumnIndex, injectTombstones());
  }

  protected boolean injectTombstones() {
    return false;
  }

  protected void createAndUploadSegmentFromClasspath(TableConfig tableConfig, Schema schema, String dataFilePath,
      FileFormat fileFormat, long expectedNoOfDocs, long timeoutMs)
      throws Exception {
    URL dataPathUrl = getClass().getClassLoader().getResource(dataFilePath);
    assert dataPathUrl != null;
    File file = new File(dataPathUrl.getFile());

    createAndUploadSegmentFromFile(tableConfig, schema, file, fileFormat, expectedNoOfDocs, timeoutMs);
  }

  /// @deprecated use createAndUploadSegmentFromClasspath instead, given what this class does is to look for
  /// dataFilePath on the classpath
  @Deprecated
  protected void createAndUploadSegmentFromFile(TableConfig tableConfig, Schema schema, String dataFilePath,
      FileFormat fileFormat, long expectedNoOfDocs, long timeoutMs)
      throws Exception {
    createAndUploadSegmentFromClasspath(tableConfig, schema, dataFilePath, fileFormat, expectedNoOfDocs, timeoutMs);
  }

  protected void createAndUploadSegmentFromFile(TableConfig tableConfig, Schema schema, File file,
      FileFormat fileFormat, long expectedNoOfDocs, long timeoutMs)
      throws Exception {

    TestUtils.ensureDirectoriesExistAndEmpty(_segmentDir, _tarDir);
    ClusterIntegrationTestUtils.buildSegmentFromFile(file, tableConfig, schema, "%", _segmentDir, _tarDir, fileFormat);
    uploadSegments(tableConfig.getTableName(), _tarDir);

    TestUtils.waitForCondition(() -> getCurrentCountStarResult(tableConfig.getTableName()) == expectedNoOfDocs, 100L,
        timeoutMs, "Failed to load " + expectedNoOfDocs + " documents in table " + tableConfig.getTableName(),
        Duration.ofMillis(timeoutMs / 10));
  }

  protected List<File> getAllAvroFiles()
      throws Exception {
    // Unpack the Avro files
    int numSegments = unpackAvroData(_tempDir).size();

    // Avro files has to be ordered as time series data
    List<File> avroFiles = new ArrayList<>(numSegments);
    for (int i = 1; i <= numSegments; i++) {
      avroFiles.add(new File(_tempDir, "On_Time_On_Time_Performance_2014_" + i + ".avro"));
    }

    return avroFiles;
  }

  protected List<File> getOfflineAvroFiles(List<File> avroFiles, int numOfflineSegments) {
    List<File> offlineAvroFiles = new ArrayList<>(numOfflineSegments);
    for (int i = 0; i < numOfflineSegments; i++) {
      offlineAvroFiles.add(avroFiles.get(i));
    }
    return offlineAvroFiles;
  }

  protected List<File> getRealtimeAvroFiles(List<File> avroFiles, int numRealtimeSegments) {
    int numSegments = avroFiles.size();
    List<File> realtimeAvroFiles = new ArrayList<>(numRealtimeSegments);
    for (int i = numSegments - numRealtimeSegments; i < numSegments; i++) {
      realtimeAvroFiles.add(avroFiles.get(i));
    }
    return realtimeAvroFiles;
  }

  protected void startKafka() {
    startKafkaWithoutTopic();
    createKafkaTopic(getKafkaTopic());
  }

  protected void startKafkaWithoutTopic() {
    int requestedBrokers = getNumKafkaBrokers();
    Properties props = new Properties();
    props.setProperty(EmbeddedKafkaCluster.BROKER_COUNT_PROP, Integer.toString(requestedBrokers));
    props.putAll(getKafkaExtraProperties());
    EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster();
    cluster.init(props);
    cluster.start();
    _kafkaStarters = List.of(cluster);
    try {
      waitForKafkaClusterReady(getKafkaBrokerList(), requestedBrokers, useKafkaTransaction());
    } catch (RuntimeException e) {
      try {
        cluster.stop();
      } catch (Exception suppressed) {
        e.addSuppressed(suppressed);
      }
      _kafkaStarters = null;
      throw e;
    }
  }

  private void waitForKafkaClusterReady(String brokerList, int brokerCount, boolean requireTransactions) {
    TestUtils.waitForCondition(aVoid -> isKafkaClusterReady(brokerList, brokerCount), 200L,
        KAFKA_CLUSTER_READY_TIMEOUT_MS, "Kafka brokers are not ready");
    if (requireTransactions) {
      // Wait for transaction coordinator with longer initial delay and timeout
      TestUtils.waitForCondition(aVoid -> canInitTransactions(brokerList), 1000L, KAFKA_CLUSTER_READY_TIMEOUT_MS,
          "Kafka transaction coordinator is not ready");
    }
  }

  private boolean isKafkaClusterReady(String brokerList, int brokerCount) {
    Properties adminProps = new Properties();
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
    adminProps.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");
    try (AdminClient adminClient = AdminClient.create(adminProps)) {
      return adminClient.describeCluster().nodes().get(5, TimeUnit.SECONDS).size() == brokerCount;
    } catch (Exception e) {
      return false;
    }
  }

  private boolean canInitTransactions(String brokerList) {
    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
    producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "pinot-txn-ready-" + UUID.randomUUID());
    producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000");
    producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps)) {
      producer.initTransactions();
      // Complete a full transaction cycle to verify coordinator is truly ready
      producer.beginTransaction();
      producer.commitTransaction();
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  protected void createKafkaTopic(String topic) {
    createKafkaTopic(topic, getNumKafkaPartitions(), getNumKafkaBrokers());
  }

  protected void createKafkaTopic(String topic, int numPartitions) {
    createKafkaTopic(topic, numPartitions, getNumKafkaBrokers());
  }

  protected void createKafkaTopic(String topic, int numPartitions, int replicationFactor) {
    int effectiveReplicationFactor = Math.max(1, Math.min(replicationFactor, getNumKafkaBrokers()));
    _kafkaStarters.get(0)
        .createTopic(topic, KafkaStarterUtils.getTopicCreationProps(numPartitions, effectiveReplicationFactor));
    waitForKafkaTopicReady(topic, numPartitions, effectiveReplicationFactor);
    waitForKafkaTopicMetadataReadyForConsumer(topic, numPartitions);
  }

  protected void deleteKafkaTopic(String topic) {
    _kafkaStarters.get(0).deleteTopic(topic);
    waitForKafkaTopicDeleted(topic);
  }

  protected void waitForKafkaTopicDeleted(String topic) {
    TestUtils.waitForCondition(aVoid -> isKafkaTopicDeleted(topic), 200L, KAFKA_TOPIC_READY_TIMEOUT_MS,
        "Kafka topic '" + topic + "' is not deleted");
  }

  private boolean isKafkaTopicDeleted(String topic) {
    Properties adminProps = new Properties();
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaBrokerList());
    adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
    adminProps.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");
    try (AdminClient adminClient = AdminClient.create(adminProps)) {
      return !adminClient.listTopics().names().get(5, TimeUnit.SECONDS).contains(topic);
    } catch (Exception e) {
      return false;
    }
  }

  private void waitForKafkaTopicReady(String topic, int expectedPartitions, int expectedReplicationFactor) {
    TestUtils.waitForCondition(aVoid -> isKafkaTopicReady(topic, expectedPartitions, expectedReplicationFactor), 200L,
        KAFKA_TOPIC_READY_TIMEOUT_MS, "Kafka topic '" + topic + "' is not fully ready");
  }

  private void waitForKafkaTopicMetadataReadyForConsumer(String topic, int expectedPartitions) {
    TestUtils.waitForCondition(aVoid -> isKafkaTopicMetadataReadyForConsumer(topic, expectedPartitions), 200L,
        KAFKA_TOPIC_READY_TIMEOUT_MS, "Kafka topic '" + topic + "' metadata is not visible to consumers");
  }

  private boolean isKafkaTopicReady(String topic, int expectedPartitions, int expectedReplicationFactor) {
    if (_kafkaStarters == null || _kafkaStarters.isEmpty()) {
      return false;
    }
    return isKafkaTopicReadyOnBroker(getKafkaBrokerList(), topic, expectedPartitions, expectedReplicationFactor);
  }

  private boolean isKafkaTopicMetadataReadyForConsumer(String topic, int expectedPartitions) {
    Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaBrokerList());
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "pinot-kafka-topic-ready-" + UUID.randomUUID());
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
    consumerProps.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");
    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
      List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic, Duration.ofSeconds(5));
      return partitionInfos != null && partitionInfos.size() >= expectedPartitions;
    } catch (Exception e) {
      return false;
    }
  }

  private boolean isKafkaTopicReadyOnBroker(String brokerList, String topic, int expectedPartitions,
      int expectedReplicationFactor) {
    Properties adminProps = new Properties();
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
    adminProps.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");
    try (AdminClient adminClient = AdminClient.create(adminProps)) {
      TopicDescription topicDescription =
          adminClient.describeTopics(List.of(topic)).allTopicNames().get(5, TimeUnit.SECONDS).get(topic);
      if (topicDescription.partitions().size() < expectedPartitions) {
        return false;
      }
      for (TopicPartitionInfo partitionInfo : topicDescription.partitions()) {
        if (partitionInfo.leader() == null) {
          return false;
        }
        if (partitionInfo.isr() == null || partitionInfo.isr().size() < expectedReplicationFactor) {
          return false;
        }
      }
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  protected void stopKafka() {
    if (_kafkaStarters != null) {
      Exception firstException = null;
      try {
        for (StreamDataServerStartable starter : _kafkaStarters) {
          try {
            starter.stop();
          } catch (Exception e) {
            if (firstException == null) {
              firstException = e;
            } else {
              firstException.addSuppressed(e);
            }
          }
        }
      } finally {
        _kafkaStarters = null;
      }
      if (firstException != null) {
        throw new RuntimeException("Failed to stop Kafka starters cleanly", firstException);
      }
    }
  }

  /// Get current result for "SELECT COUNT(\*)".
  ///
  /// @return Current count start result
  protected long getCurrentCountStarResult() {
    return getCurrentCountStarResult(getTableName());
  }

  protected long getCurrentCountStarResult(String tableName) {
    ResultSetGroup resultSetGroup = getPinotConnection().execute("SELECT COUNT(*) FROM " + tableName);
    if (resultSetGroup.getResultSetCount() > 0) {
      // count(*) can return null sometimes for initializing tables.
      String countStarStr = resultSetGroup.getResultSet(0).getString(0);
      try {
        return Long.parseLong(countStarStr);
      } catch (NumberFormatException e) {
        return 0;
      }
    }
    return 0;
  }

  /// Get current result for "SELECT COUNT(\*) ... OPTION(skipUpsert=true)": every ingested row of an upsert or dedup
  /// table, including the ones upsert replaced or deleted.
  ///
  /// @throws IllegalStateException when the broker returns an exception or no result, since a count from part of the
  ///   segments is not the table's count
  protected long getCurrentCountStarResultWithoutUpsert(String tableName) {
    ResultSetGroup resultSetGroup =
        getPinotConnection().execute("SELECT COUNT(*) FROM " + tableName + " OPTION(skipUpsert=true)");
    if (!resultSetGroup.getExceptions().isEmpty() || resultSetGroup.getResultSetCount() == 0) {
      throw new IllegalStateException(
          "No COUNT(*) result for table " + tableName + ": " + resultSetGroup.getExceptions());
    }
    String count = resultSetGroup.getResultSet(0).getString(0);
    try {
      return Long.parseLong(count);
    } catch (NumberFormatException e) {
      throw new IllegalStateException("Invalid COUNT(*) result for table " + tableName + ": " + count, e);
    }
  }

  protected void waitForMinionTaskCompletion(String taskId, long timeout) {
    // The task state (workflow context) and the subtask states (job context) live in different Helix znodes and are
    // not updated atomically, so a subtask can still read as a non-terminal state (null/INIT before start, RUNNING,
    // or STOPPED before a resume) right after the task turns COMPLETED. Wait until the task is COMPLETED and every
    // subtask reached a terminal state before validating them. A non-empty subtask map also proves the task was
    // actually run.
    PinotHelixTaskResourceManager taskResourceManager = _controllerStarter.getHelixTaskResourceManager();
    TestUtils.waitForCondition(aVoid -> {
      if (taskResourceManager.getTaskState(taskId) != TaskState.COMPLETED) {
        return false;
      }
      Map<String, TaskPartitionState> subtaskStates = taskResourceManager.getSubtaskStates(taskId);
      return !subtaskStates.isEmpty() && subtaskStates.values()
          .stream()
          .noneMatch(state -> state == null || state == TaskPartitionState.INIT || state == TaskPartitionState.RUNNING
              || state == TaskPartitionState.STOPPED);
    }, timeout, "Failed to complete the task " + taskId);

    // Validate that all subtasks are completed successfully. A task can be marked completed even if some subtasks
    // failed, so we need to check the subtask states.
    Map<String, TaskPartitionState> subtaskStates = taskResourceManager.getSubtaskStates(taskId);
    assertTrue(subtaskStates.values().stream().allMatch(state -> state == TaskPartitionState.COMPLETED),
        "Not all subtasks are completed for task " + taskId + " : " + subtaskStates);
  }

  protected List<String> getSegments(String tableNameWithType) {
    return getHelixResourceManager().getSegmentsFor(tableNameWithType, false);
  }

  protected int getSegmentCount(String tableNameWithType) {
    return getSegments(tableNameWithType).size();
  }

  /// Wait for all documents to get loaded.
  ///
  /// @param timeoutMs Timeout in milliseconds
  /// @throws Exception
  protected void waitForAllDocsLoaded(long timeoutMs)
      throws Exception {
    waitForAllDocsLoaded(getTableName(), timeoutMs);
  }

  /// Requires as many consecutive successful document count checks as there are servers in the cluster,
  /// since servers can finish loading their segments at different times.
  protected void waitForAllDocsLoaded(String tableName, long timeoutMs) {
    long countStarResult = getCountStarResult();
    int numServers = _serverStarters.size();
    AtomicInteger consecutiveSuccesses = new AtomicInteger();
    TestUtils.waitForCondition(() -> {
      try {
        if (getCurrentCountStarResult(tableName) != countStarResult) {
          consecutiveSuccesses.set(0);
          return false;
        }
        return consecutiveSuccesses.incrementAndGet() >= numServers;
      } catch (Exception e) {
        consecutiveSuccesses.set(0);
        throw e;
      }
    }, 100L, timeoutMs, "Failed to load " + countStarResult + " documents on all servers",
        Duration.ofMillis(timeoutMs / 10));
  }

  /// Runs a query and returns its rows in the order the broker returned them, each rendered as a JSON array such as
  /// `[100,"Zook",2050.0,false]`. Throws [IllegalStateException] when the broker reports an exception or returns
  /// no result table.
  private List<String> queryRows(@Language("sql") String query)
      throws Exception {
    return toRows(query, postQuery(query));
  }

  /// Returns the rows of a broker response as JSON arrays; throws with the query and the response on a broker
  /// exception or when the response has no rows.
  private static List<String> toRows(String query, JsonNode response) {
    JsonNode exceptions = response.get("exceptions");
    if (exceptions != null && !exceptions.isEmpty()) {
      throw new IllegalStateException("Query: " + query + " failed with: " + exceptions);
    }
    JsonNode resultTable = response.get("resultTable");
    JsonNode resultRows = resultTable != null ? resultTable.get("rows") : null;
    if (resultRows == null) {
      throw new IllegalStateException("Query: " + query + " returned no rows: " + response);
    }
    List<String> rows = new ArrayList<>();
    for (JsonNode row : resultRows) {
      rows.add(row.toString());
    }
    return rows;
  }

  /// Renders expected rows as JSON arrays, the same way [#queryRows] renders the rows a query returns.
  private static List<String> toJsonRows(List<List<Object>> rows) {
    List<String> rendered = new ArrayList<>(rows.size());
    for (List<Object> row : rows) {
      rendered.add(JsonUtils.objectToJsonNode(row).toString());
    }
    return rendered;
  }

  /// Asserts the rows of a single-stage query on a REALTIME table until every replica of every segment has served
  /// it: every (segment, instance) pair the IdealState has ONLINE or CONSUMING. The broker picks the replica per
  /// request id, and the routing table of the request id each response reports names the replicas that served it.
  /// Needs a broker started by this test, and a deterministic instance selector (no adaptive server selection).
  /// Settle the cluster first, so the routing does not change between a query and its lookup.
  protected void assertRowsFromEveryReplica(@Language("sql") String query, List<List<Object>> expectedRows)
      throws Exception {
    Preconditions.checkState(!useMultiStageQueryEngine(), "Replica routing is looked up for single-stage queries");
    Preconditions.checkState(!_brokerStarters.isEmpty(), "Needs a broker started by this test");
    BrokerRequest brokerRequest = CalciteSqlCompiler.compileToBrokerRequest(query);
    String realtimeTableName =
        TableNameBuilder.REALTIME.tableNameWithType(brokerRequest.getQuerySource().getTableName());
    IdealState idealState = getHelixResourceManager().getTableIdealState(realtimeTableName);
    Preconditions.checkState(idealState != null, "No REALTIME table %s for query: %s", realtimeTableName, query);
    Set<String> replicas = new TreeSet<>();
    idealState.getRecord().getMapFields().forEach((segment, instanceStates) -> instanceStates.forEach(
        (instance, state) -> {
          if (SegmentStateModel.ONLINE.equals(state) || SegmentStateModel.CONSUMING.equals(state)) {
            replicas.add(segment + "@" + instance);
          }
        }));
    List<String> expected = toJsonRows(expectedRows);
    Set<String> served = new TreeSet<>();
    for (int i = 0; i < MAX_QUERIES_TO_REACH_EVERY_REPLICA && !served.containsAll(replicas); i++) {
      JsonNode response = postQuery(query);
      List<String> actual = toRows(query, response);
      if (!actual.equals(expected)) {
        throw new AssertionError(describeRowDiff(query, expected, actual));
      }
      RoutingTable routingTable = _brokerStarters.get(0).getRoutingManager()
          .getRoutingTable(brokerRequest, realtimeTableName, Long.parseLong(response.get("requestId").asText()));
      if (routingTable != null) {
        routingTable.getServerInstanceToSegmentsMap().forEach((serverInstance, segmentsToQuery) -> {
          segmentsToQuery.getSegments().forEach(segment -> served.add(segment + "@" + serverInstance.getInstanceId()));
          segmentsToQuery.getOptionalSegments()
              .forEach(segment -> served.add(segment + "@" + serverInstance.getInstanceId()));
        });
      }
    }
    Set<String> missing = new TreeSet<>(replicas);
    missing.removeAll(served);
    assertTrue(missing.isEmpty(), "Replicas of " + realtimeTableName + " that never served the query " + query
        + " in " + MAX_QUERIES_TO_REACH_EVERY_REPLICA + " tries: " + missing);
  }

  /// Asserts that the query returns exactly the expected rows, in order. The query should have an `ORDER BY` and a
  /// `LIMIT` that covers every row. Rows are compared as JSON, so an INT and a LONG of the same value match. On failure
  /// the message shows the rows that are missing and the rows that are not expected.
  protected void assertRows(@Language("sql") String query, List<List<Object>> expectedRows)
      throws Exception {
    List<String> expected = toJsonRows(expectedRows);
    List<String> actual = queryRows(query);
    if (!actual.equals(expected)) {
      throw new AssertionError(describeRowDiff(query, expected, actual));
    }
  }

  /// Waits until the query returns exactly the expected rows, in order. A failing query (e.g. while servers restart)
  /// counts as a miss and is retried after the poll interval. On timeout the message shows the row diff of the last
  /// successful query, and the last query error if any.
  protected void waitForRows(@Language("sql") String query, List<List<Object>> expectedRows, long timeoutMs) {
    List<String> expected = toJsonRows(expectedRows);
    AtomicReference<List<String>> lastRows = new AtomicReference<>();
    AtomicReference<Exception> lastError = new AtomicReference<>();
    try {
      TestUtils.waitForCondition(aVoid -> {
        try {
          List<String> rows = queryRows(query);
          lastRows.set(rows);
          lastError.set(null);
          return rows.equals(expected);
        } catch (Exception e) {
          lastError.set(e);
          return false;
        }
      }, 100L, timeoutMs, null);
    } catch (AssertionError e) {
      List<String> last = lastRows.get();
      String message = last != null ? describeRowDiff(query, expected, last) : "No query succeeded: " + query;
      AssertionError error = new AssertionError("Rows did not match in " + timeoutMs + "ms. " + message, e);
      if (lastError.get() != null) {
        error.addSuppressed(lastError.get());
      }
      throw error;
    }
  }

  /// Asserts that an upsert table fed with the given Avro files serves, for each primary key, the record with the
  /// largest comparison value, with the given columns. See [ClusterIntegrationTestUtils#latestRowPerKey].
  protected void assertLatestRowPerKey(String tableName, List<File> avroFiles, String primaryKeyColumn,
      String comparisonColumn, List<String> columns)
      throws Exception {
    assertRows("SELECT " + String.join(", ", columns) + " FROM " + tableName + " ORDER BY " + primaryKeyColumn
            + " LIMIT 10000",
        ClusterIntegrationTestUtils.latestRowPerKey(avroFiles, primaryKeyColumn, comparisonColumn, columns));
  }

  /// Describes how the actual rows differ from the expected rows: which expected rows are missing, which served rows
  /// are not expected, and both full lists.
  private static String describeRowDiff(String query, List<String> expected, List<String> actual) {
    List<String> missing = new ArrayList<>(expected);
    List<String> unexpected = new ArrayList<>();
    for (String row : actual) {
      if (!missing.remove(row)) {
        unexpected.add(row);
      }
    }
    StringBuilder message = new StringBuilder("Query: ").append(query);
    if (missing.isEmpty() && unexpected.isEmpty()) {
      message.append("\nSame rows in a different order");
    } else {
      message.append("\nMissing (expected, not served): ").append(missing);
      message.append("\nUnexpected (served, not expected): ").append(unexpected);
    }
    message.append("\nExpected (").append(expected.size()).append("): ").append(expected);
    message.append("\nActual (").append(actual.size()).append("): ").append(actual);
    return message.toString();
  }

  /// Returns whether the ExternalView of the table has exactly the segments of its IdealState, each instance in its
  /// prescribed state. Queries route by the ExternalView, so until then some segments may not be served where
  /// assigned. For a CONSUMING segment this does not mean the consumer caught up; pair it with a row or lag wait.
  protected boolean isExternalViewConverged(String tableNameWithType) {
    IdealState idealState = getHelixResourceManager().getTableIdealState(tableNameWithType);
    ExternalView externalView = getHelixResourceManager().getTableExternalView(tableNameWithType);
    if (idealState == null || externalView == null) {
      return false;
    }
    return idealState.getRecord().getMapFields().equals(externalView.getRecord().getMapFields());
  }

  /// Waits until [#isExternalViewConverged(String)] holds once. [#waitForClusterStateSettled(String, long)] also
  /// requires no OFFLINE segment and a few stable polls.
  protected void waitForExternalViewToConverge(String tableNameWithType, long timeoutMs) {
    TestUtils.waitForCondition(aVoid -> isExternalViewConverged(tableNameWithType), 1000L, timeoutMs,
        "ExternalView failed to converge to the IdealState for table: " + tableNameWithType);
  }

  /// Like [#waitForClusterStateSettled(String, Integer, int, long)] for any number of segments: waits until the
  /// ExternalView of the table (OFFLINE or REALTIME) equals its IdealState with no OFFLINE segment, so the next row
  /// check reads every segment from where it is assigned.
  protected void waitForClusterStateSettled(String tableNameWithType, long timeoutMs) {
    waitForClusterStateSettled(tableNameWithType, null, 5, timeoutMs);
  }

  /// Waits until the cluster state has settled after a reload, rebalance or restart:
  ///
  /// - IdealState has `expectedSegments` entries (any number when null).
  /// - No segment in IdealState has any instance in OFFLINE state.
  /// - [#isExternalViewConverged(String)] (queries route via ExternalView, so this is required
  ///   before asserting on IdealState).
  ///
  /// All conditions must hold for `stableSamples` consecutive polls, because IdealState can
  /// transiently look correct between two intermediate states during force-commit +
  /// new-consuming-segment creation.
  protected void waitForClusterStateSettled(String tableNameWithType, @Nullable Integer expectedSegments,
      int stableSamples, long timeoutMs) {
    AtomicInteger streak = new AtomicInteger(0);
    TestUtils.waitForCondition(aVoid -> {
      IdealState idealState = getHelixResourceManager().getTableIdealState(tableNameWithType);
      if (idealState == null) {
        streak.set(0);
        return false;
      }
      Map<String, Map<String, String>> isMap = idealState.getRecord().getMapFields();
      if (expectedSegments != null && isMap.size() != expectedSegments) {
        streak.set(0);
        return false;
      }
      boolean anyOffline = isMap.values().stream()
          .flatMap(m -> m.values().stream())
          .anyMatch(SegmentStateModel.OFFLINE::equals);
      if (anyOffline) {
        streak.set(0);
        return false;
      }
      if (!isExternalViewConverged(tableNameWithType)) {
        streak.set(0);
        return false;
      }
      return streak.incrementAndGet() >= stableSamples;
    }, 200L, timeoutMs,
        "Cluster state never settled to " + expectedSegments
            + " segments with no OFFLINE and the ExternalView equal to the IdealState for table " + tableNameWithType);
  }

  /// Waits until every replica of every stream partition of the table has consumed up to the latest upstream offset,
  /// i.e. reports no records lag. Use it after pushing rows that the table is expected to drop (for example
  /// duplicates on a dedup table), where no row or count change shows that they were consumed.
  ///
  /// Every instance the IdealState has in CONSUMING state must report, so a replica that has not reported yet holds
  /// the wait instead of being skipped.
  ///
  /// @param tableName table name, with or without the REALTIME suffix; the stream partitions are the ones of the
  ///   LLC segments in the IdealState
  protected void waitForAllStreamRecordsConsumed(String tableName, long timeoutMs) {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    // Consuming segment to the records lag reported by each server
    AtomicReference<Map<String, Map<String, Map<String, String>>>> lastLags = new AtomicReference<>();
    AtomicReference<Exception> lastError = new AtomicReference<>();
    try {
      TestUtils.waitForCondition(aVoid -> {
        try {
          IdealState idealState = getHelixResourceManager().getTableIdealState(realtimeTableName);
          ConsumingSegmentsInfoMap info = getOrCreateAdminClient().getTableClient()
              .getConsumingSegmentsInfo(realtimeTableName, ConsumingSegmentsInfoMap.class);
          Map<String, Map<String, Map<String, String>>> segmentToServerLags = new HashMap<>();
          info._segmentToConsumingInfoMap.forEach((segment, segmentInfos) -> {
            for (ConsumingSegmentInfo segmentInfo : segmentInfos) {
              PartitionOffsetInfo offsetInfo = segmentInfo._partitionOffsetInfo;
              segmentToServerLags.computeIfAbsent(segment, k -> new HashMap<>()).put(segmentInfo._serverName,
                  offsetInfo != null && offsetInfo._recordsLagMap != null ? offsetInfo._recordsLagMap : Map.of());
            }
          });
          lastLags.set(segmentToServerLags);
          lastError.set(null);
          if (idealState == null || info._serversFailingToRespond != 0) {
            return false;
          }
          // Every stream partition of the table must have a consuming segment
          Set<Integer> partitions = new HashSet<>();
          Set<Integer> consumingPartitions = new HashSet<>();
          for (Map.Entry<String, Map<String, String>> entry : idealState.getRecord().getMapFields().entrySet()) {
            LLCSegmentName llcSegmentName = LLCSegmentName.of(entry.getKey());
            if (llcSegmentName != null) {
              partitions.add(llcSegmentName.getPartitionGroupId());
            }
            for (Map.Entry<String, String> instanceState : entry.getValue().entrySet()) {
              if (!SegmentStateModel.CONSUMING.equals(instanceState.getValue())) {
                continue;
              }
              Map<String, String> lags =
                  segmentToServerLags.getOrDefault(entry.getKey(), Map.of()).get(instanceState.getKey());
              if (lags == null || lags.isEmpty() || !lags.values().stream().allMatch("0"::equals)) {
                return false;
              }
              if (llcSegmentName != null) {
                consumingPartitions.add(llcSegmentName.getPartitionGroupId());
              }
            }
          }
          return !partitions.isEmpty() && consumingPartitions.equals(partitions);
        } catch (Exception e) {
          lastError.set(e);
          return false;
        }
      }, 1000L, timeoutMs, null);
    } catch (AssertionError e) {
      AssertionError error = new AssertionError("Table " + realtimeTableName + " did not consume every stream "
          + "partition on every replica in " + timeoutMs + "ms; last records lag per consuming segment and server: "
          + lastLags.get(), e);
      if (lastError.get() != null) {
        error.addSuppressed(lastError.get());
      }
      throw error;
    }
  }

  protected void waitForAnyDocLoaded(String tableName, long timeoutMs) {
    TestUtils.waitForCondition(() -> getCurrentCountStarResult(tableName) > 0, 100L, timeoutMs,
        "Failed to load any document", Duration.ofMillis(timeoutMs / 10));
  }

  /// @deprecated Always raise error when condition is not met.
  @Deprecated
  protected void waitForDocsLoaded(long timeoutMs, boolean raiseError, String tableName) {
    long countStarResult = getCountStarResult();
    TestUtils.waitForCondition(() -> getCurrentCountStarResult(tableName) == countStarResult, 100L, timeoutMs,
        "Failed to load " + countStarResult + " documents", raiseError, Duration.ofMillis(timeoutMs / 10));
  }

  protected void waitForAllRealtimePartitionsConsuming(String tableNameWithType, long timeoutMs) {
    int expectedPartitions = getNumKafkaPartitions();
    TestUtils.waitForCondition(() -> getNumConsumingPartitions(tableNameWithType) == expectedPartitions, 200L,
        timeoutMs,
        "Failed to get CONSUMING segments for all " + expectedPartitions + " partitions for table " + tableNameWithType,
        Duration.ofMillis(timeoutMs / 10));
  }

  private int getNumConsumingPartitions(String tableNameWithType) {
    IdealState idealState = getHelixResourceManager().getTableIdealState(tableNameWithType);
    if (idealState == null) {
      return 0;
    }
    Set<Integer> consumingPartitions = new HashSet<>();
    for (String segmentName : idealState.getPartitionSet()) {
      if (!LLCSegmentName.isLLCSegment(segmentName)) {
        continue;
      }
      Map<String, String> stateMap = idealState.getInstanceStateMap(segmentName);
      if (stateMap != null && stateMap.containsValue(SegmentStateModel.CONSUMING)) {
        consumingPartitions.add(new LLCSegmentName(segmentName).getPartitionGroupId());
      }
    }
    return consumingPartitions.size();
  }

  /// Wait for servers to remove the table data manager after the table is deleted.
  protected void waitForTableDataManagerRemoved(String tableNameWithType) {
    TestUtils.waitForCondition(aVoid -> {
      for (BaseServerStarter serverStarter : _serverStarters) {
        if (serverStarter.getServerInstance().getInstanceDataManager().getTableDataManager(tableNameWithType) != null) {
          return false;
        }
      }
      return true;
    }, 60_000L, "Failed to remove table data manager for table: " + tableNameWithType);
  }

  /// Reset table utils.
  protected void resetTable(String tableName, TableType tableType, @Nullable String targetInstance)
      throws IOException {
    try {
      String tableNameWithType = TableNameBuilder.forType(tableType).tableNameWithType(tableName);
      getOrCreateAdminClient().getSegmentClient().resetSegments(tableNameWithType, false, targetInstance);
    } catch (org.apache.pinot.client.admin.PinotAdminException e) {
      throw new IOException(e);
    }
  }

  /// Run equivalent Pinot and H2 query and compare the results.
  protected void testQuery(@Language("sql") String query)
      throws Exception {
    testQuery(query, query);
  }

  /// Run equivalent Pinot and H2 query and compare the results.
  protected void testQuery(@Language("sql") String pinotQuery, @Language("sql") String h2Query)
      throws Exception {
    ClusterIntegrationTestUtils.testQuery(pinotQuery, getBrokerBaseApiUrl(), getPinotConnection(), h2Query,
        getH2Connection(), null, getExtraQueryProperties(), useMultiStageQueryEngine());
  }

  /// Run equivalent Pinot and H2 query and compare the results.
  protected void testQueryWithMatchingRowCount(@Language("sql") String pinotQuery, @Language("sql") String h2Query)
      throws Exception {
    ClusterIntegrationTestUtils.testQueryWithMatchingRowCount(pinotQuery, getBrokerBaseApiUrl(), getPinotConnection(),
        h2Query, getH2Connection(), null, getExtraQueryProperties(), useMultiStageQueryEngine());
  }

  protected String getType(JsonNode jsonNode, int colIndex) {
    return jsonNode.get("resultTable").get("dataSchema").get("columnDataTypes").get(colIndex).asText();
  }

  protected <T> T getCellValue(JsonNode jsonNode, int colIndex, int rowIndex, Function<JsonNode, T> extract) {
    JsonNode cellResult = jsonNode.get("resultTable").get("rows").get(rowIndex).get(colIndex);
    return extract.apply(cellResult);
  }

  protected long getLongCellValue(JsonNode jsonNode, int colIndex, int rowIndex) {
    return getCellValue(jsonNode, colIndex, rowIndex, JsonNode::asLong).longValue();
  }

  protected JsonNode getColumnIndexSize(String column)
      throws Exception {
    TableMetadataInfo metadata = getOrCreateAdminClient().getTableClient()
        .getAggregateMetadata(getTableName(), String.join(",", List.of(column)));
    return JsonUtils.objectToJsonNode(metadata).get("columnIndexSizeMap").get(column);
  }

  /// Get all segment names for a given tableName and tableType.
  protected List<String> getSegmentNames(String tableName, @Nullable String tableType)
      throws Exception {
    return getOrCreateAdminClient().getSegmentClient().listSegments(tableName, tableType, true);
  }

  protected List<ValidDocIdsMetadataInfo> getValidDocIdsMetadata(String tableNameWithType,
      ValidDocIdsType validDocIdsType)
      throws Exception {

    String responseString =
        getOrCreateAdminClient().getTableClient().getValidDocIdsMetadata(tableNameWithType, validDocIdsType.toString());
    return JsonUtils.stringToObject(responseString, new TypeReference<>() {
    });
  }
}
