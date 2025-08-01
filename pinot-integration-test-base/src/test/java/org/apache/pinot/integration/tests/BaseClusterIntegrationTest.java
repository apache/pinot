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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskState;
import org.apache.pinot.client.ConnectionFactory;
import org.apache.pinot.client.JsonAsyncHttpPinotClientTransportFactory;
import org.apache.pinot.client.PinotClientTransportFactory;
import org.apache.pinot.client.ResultSetGroup;
import org.apache.pinot.common.restlet.resources.ValidDocIdsMetadataInfo;
import org.apache.pinot.common.restlet.resources.ValidDocIdsType;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.plugin.inputformat.csv.CSVMessageDecoder;
import org.apache.pinot.plugin.stream.kafka.KafkaStreamConfigProperties;
import org.apache.pinot.server.starter.helix.BaseServerStarter;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.QueryConfig;
import org.apache.pinot.spi.config.table.ReplicaGroupStrategyConfig;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.stream.StreamDataServerStartable;
import org.apache.pinot.spi.utils.Enablement;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.tools.utils.KafkaStarterUtils;
import org.apache.pinot.util.TestUtils;
import org.intellij.lang.annotations.Language;
import org.testng.Assert;


/**
 * Shared implementation details of the cluster integration tests.
 */
public abstract class BaseClusterIntegrationTest extends ClusterTest {

  // Default settings
  protected static final String DEFAULT_TABLE_NAME = "mytable";
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
  protected static final List<String> DEFAULT_NO_DICTIONARY_COLUMNS =
      Arrays.asList("ActualElapsedTime", "ArrDelay", "DepDelay", "CRSDepTime");
  protected static final String DEFAULT_SORTED_COLUMN = "Carrier";
  protected static final List<String> DEFAULT_INVERTED_INDEX_COLUMNS = Arrays.asList("FlightNum", "Origin", "Quarter");
  private static final List<String> DEFAULT_BLOOM_FILTER_COLUMNS = Arrays.asList("FlightNum", "Origin");
  private static final List<String> DEFAULT_RANGE_INDEX_COLUMNS = Collections.singletonList("Origin");
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

  /**
   * The following getters can be overridden to change default settings.
   */

  protected String getTableName() {
    return DEFAULT_TABLE_NAME;
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

  protected int getKafkaPort() {
    int idx = RANDOM.nextInt(_kafkaStarters.size());
    return _kafkaStarters.get(idx).getPort();
  }

  protected String getKafkaZKAddress() {
    return getZkUrl() + "/kafka";
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

  protected boolean isCreateInvertedIndexDuringSegmentGeneration() {
    return false;
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

  @Nullable
  protected ReplicaGroupStrategyConfig getReplicaGroupStrategyConfig() {
    return null;
  }

  /**
   * Creates a new schema.
   */
  protected Schema createSchema()
      throws IOException {
    Schema schema = createSchema(getSchemaFileName());
    schema.setSchemaName(getTableName());
    return schema;
  }

  protected Schema createSchema(String schemaFileName)
      throws IOException {
    InputStream inputStream = getClass().getClassLoader().getResourceAsStream(schemaFileName);
    Assert.assertNotNull(inputStream);
    return Schema.fromInputStream(inputStream);
  }

  protected Schema createSchema(File schemaFile)
      throws IOException {
    return Schema.fromInputStream(new FileInputStream(schemaFile));
  }

  protected TableConfig createTableConfig(String tableConfigFileName)
      throws IOException {
    URL configPathUrl = getClass().getClassLoader().getResource(tableConfigFileName);
    Assert.assertNotNull(configPathUrl);
    return createTableConfig(new File(configPathUrl.getFile()));
  }

  protected TableConfig createTableConfig(File tableConfigFile)
      throws IOException {
    InputStream inputStream = new FileInputStream(tableConfigFile);
    Assert.assertNotNull(inputStream);
    return JsonUtils.inputStreamToObject(inputStream, TableConfig.class);
  }

  /**
   * Creates a new OFFLINE table config.
   */
  protected TableConfig createOfflineTableConfig() {
    // @formatter:off
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(getTableName())
        .setTimeColumnName(getTimeColumnName())
        .setSortedColumn(getSortedColumn())
        .setInvertedIndexColumns(getInvertedIndexColumns())
        .setCreateInvertedIndexDuringSegmentGeneration(isCreateInvertedIndexDuringSegmentGeneration())
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
        .build();
    // @formatter:on
  }

  /**
   * Returns the OFFLINE table config in the cluster.
   */
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
            KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_BROKER_LIST),
        "localhost:" + _kafkaStarters.get(0).getPort());
    if (useKafkaTransaction()) {
      streamConfigMap.put(KafkaStreamConfigProperties.constructStreamProperty(
              KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_ISOLATION_LEVEL),
          KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_ISOLATION_LEVEL_READ_COMMITTED);
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

  /**
   * Creates a new REALTIME table config.
   */
  protected TableConfig createRealtimeTableConfig(File sampleAvroFile) {
    AvroFileSchemaKafkaAvroMessageDecoder._avroFile = sampleAvroFile;
    return getTableConfigBuilder(TableType.REALTIME).build();
  }

  // TODO - Use this method to create table config for all table types to avoid redundant code
  protected TableConfigBuilder getTableConfigBuilder(TableType tableType) {
    return new TableConfigBuilder(tableType)
        .setTableName(getTableName())
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
        .setReplicaGroupStrategyConfig(getReplicaGroupStrategyConfig());
  }

  /**
   * Creates a new Upsert enabled table config.
   */
  protected TableConfig createUpsertTableConfig(File sampleAvroFile, String primaryKeyColumn, String deleteColumn,
      int numPartitions) {
    AvroFileSchemaKafkaAvroMessageDecoder._avroFile = sampleAvroFile;
    Map<String, ColumnPartitionConfig> columnPartitionConfigMap = new HashMap<>();
    columnPartitionConfigMap.put(primaryKeyColumn, new ColumnPartitionConfig("Murmur", numPartitions));

    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setDeleteRecordColumn(deleteColumn);

    return new TableConfigBuilder(TableType.REALTIME).setTableName(getTableName())
        .setTimeColumnName(getTimeColumnName()).setFieldConfigList(getFieldConfigs()).setNumReplicas(getNumReplicas())
        .setSegmentVersion(getSegmentVersion()).setLoadMode(getLoadMode()).setTaskConfig(getTaskConfig())
        .setBrokerTenant(getBrokerTenant()).setServerTenant(getServerTenant()).setIngestionConfig(getIngestionConfig())
        .setStreamConfigs(getStreamConfigs()).setNullHandlingEnabled(getNullHandlingEnabled()).setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .setSegmentPartitionConfig(new SegmentPartitionConfig(columnPartitionConfigMap))
        .setReplicaGroupStrategyConfig(new ReplicaGroupStrategyConfig(primaryKeyColumn, 1))
        .setUpsertConfig(upsertConfig).build();
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

  /**
   * Creates a new Upsert enabled table config.
   */
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

    return new TableConfigBuilder(TableType.REALTIME).setTableName(tableName).setTimeColumnName(getTimeColumnName())
        .setFieldConfigList(getFieldConfigs()).setNumReplicas(getNumReplicas()).setSegmentVersion(getSegmentVersion())
        .setLoadMode(getLoadMode()).setTaskConfig(getTaskConfig()).setBrokerTenant(getBrokerTenant())
        .setServerTenant(getServerTenant()).setIngestionConfig(getIngestionConfig()).setStreamConfigs(streamConfigsMap)
        .setNullHandlingEnabled(UpsertConfig.Mode.PARTIAL.equals(upsertConfig.getMode()) || getNullHandlingEnabled())
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .setSegmentPartitionConfig(new SegmentPartitionConfig(columnPartitionConfigMap))
        .setReplicaGroupStrategyConfig(new ReplicaGroupStrategyConfig(primaryKeyColumn, 1))
        .setUpsertConfig(upsertConfig).build();
  }

  /**
   * Creates a new Dedup enabled table config
   */
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
        .setReplicaGroupStrategyConfig(new ReplicaGroupStrategyConfig(primaryKeyColumn, 1))
        .setDedupConfig(new DedupConfig())
        .build();
  }

  /**
   * Returns the REALTIME table config in the cluster.
   */
  protected TableConfig getRealtimeTableConfig() {
    return getRealtimeTableConfig(getTableName());
  }

  /**
   * Returns the headers to be used for the connection to Pinot cluster.
   * {@link PinotClientTransportFactory}
   */
  protected Map<String, String> getPinotClientTransportHeaders() {
    return Map.of();
  }

  /**
   * Get the Pinot connection.
   *
   * @return Pinot connection
   */
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
      JsonAsyncHttpPinotClientTransportFactory factory = new JsonAsyncHttpPinotClientTransportFactory()
        .withConnectionProperties(getPinotConnectionProperties());
      factory.setHeaders(getPinotClientTransportHeaders());
      _pinotConnection = ConnectionFactory.fromZookeeper(getZkUrl() + "/" + getHelixClusterName(),
          factory.buildTransport());
    }
    return _pinotConnection;
  }

  protected Properties getPinotConnectionProperties() {
    Properties properties = new Properties();
    properties.putAll(getExtraQueryProperties());
    return properties;
  }

  /**
   * Get the H2 connection. H2 connection must be set up before calling this method.
   *
   * @return H2 connection
   */
  protected Connection getH2Connection() {
    Assert.assertNotNull(_h2Connection, "H2 Connection has not been initialized");
    return _h2Connection;
  }

  /**
   * Get the query generator. Query generator must be set up before calling this method.
   *
   * @return Query generator.
   */
  protected QueryGenerator getQueryGenerator() {
    Assert.assertNotNull(_queryGenerator, "Query Generator has not been initialized");
    return _queryGenerator;
  }

  /**
   * Sets up the H2 connection
   */
  protected void setUpH2Connection()
      throws Exception {
    Assert.assertNull(_h2Connection);
    Class.forName("org.h2.Driver");
    _h2Connection = DriverManager.getConnection("jdbc:h2:mem:");
  }

  /**
   * Sets up the H2 connection to a table with pre-loaded data.
   */
  protected void setUpH2Connection(List<File> avroFiles)
      throws Exception {
    setUpH2Connection();
    ClusterIntegrationTestUtils.setUpH2TableWithAvro(avroFiles, getTableName(), _h2Connection);
  }

  /**
   * Sets up the query generator using the given Avro files.
   */
  protected void setUpQueryGenerator(List<File> avroFiles) {
    Assert.assertNull(_queryGenerator);
    String tableName = getTableName();
    _queryGenerator = new QueryGenerator(avroFiles, tableName, tableName);
  }

  protected List<File> unpackAvroData(File outputDir)
      throws Exception {
    return unpackTarData(getAvroTarFileName(), outputDir);
  }

  /**
   * Unpack the tarred data into the given directory.
   *
   * @param tarFileName Input tar filename
   * @param outputDir Output directory
   * @return List of files unpacked.
   * @throws Exception
   */
  protected List<File> unpackTarData(String tarFileName, File outputDir)
      throws Exception {
    InputStream inputStream = getClass().getClassLoader().getResourceAsStream(tarFileName);
    Assert.assertNotNull(inputStream);
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
      FileFormat fileFormat, long expectedNoOfDocs, long timeoutMs) throws Exception {
    URL dataPathUrl = getClass().getClassLoader().getResource(dataFilePath);
    assert dataPathUrl != null;
    File file = new File(dataPathUrl.getFile());

    createAndUploadSegmentFromFile(tableConfig, schema, file, fileFormat, expectedNoOfDocs, timeoutMs);
  }

  /// @deprecated use createAndUploadSegmentFromClasspath instead, given what this class does is to look for
  /// dataFilePath on the classpath
  @Deprecated
  protected void createAndUploadSegmentFromFile(TableConfig tableConfig, Schema schema, String dataFilePath,
      FileFormat fileFormat, long expectedNoOfDocs, long timeoutMs) throws Exception {
    createAndUploadSegmentFromClasspath(tableConfig, schema, dataFilePath, fileFormat, expectedNoOfDocs, timeoutMs);
  }

  protected void createAndUploadSegmentFromFile(TableConfig tableConfig, Schema schema, File file,
      FileFormat fileFormat, long expectedNoOfDocs, long timeoutMs) throws Exception {

    TestUtils.ensureDirectoriesExistAndEmpty(_segmentDir, _tarDir);
    ClusterIntegrationTestUtils.buildSegmentFromFile(file, tableConfig, schema, "%", _segmentDir, _tarDir, fileFormat);
    uploadSegments(tableConfig.getTableName(), _tarDir);

    TestUtils.waitForCondition(() -> getCurrentCountStarResult(tableConfig.getTableName()) == expectedNoOfDocs, 100L,
        timeoutMs, "Failed to load " + expectedNoOfDocs + " documents in table " + tableConfig.getTableName(),
        true, Duration.ofMillis(timeoutMs / 10));
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
    startKafkaWithoutTopic(KafkaStarterUtils.DEFAULT_KAFKA_PORT);
  }

  protected void startKafkaWithoutTopic(int port) {
    _kafkaStarters = KafkaStarterUtils.startServers(getNumKafkaBrokers(), port, getKafkaZKAddress(),
        KafkaStarterUtils.getDefaultKafkaConfiguration());
  }

  protected void createKafkaTopic(String topic) {
    _kafkaStarters.get(0).createTopic(topic, KafkaStarterUtils.getTopicCreationProps(getNumKafkaPartitions()));
  }

  protected void stopKafka() {
    for (StreamDataServerStartable kafkaStarter : _kafkaStarters) {
      kafkaStarter.stop();
    }
  }

  /**
   * Get current result for "SELECT COUNT(*)".
   *
   * @return Current count start result
   */
  protected long getCurrentCountStarResult() {
    return getCurrentCountStarResult(getTableName());
  }

  protected long getCurrentCountStarResult(String tableName) {
    ResultSetGroup resultSetGroup = getPinotConnection().execute("SELECT COUNT(*) FROM " + tableName);
    if (resultSetGroup.getResultSetCount() > 0) {
      return resultSetGroup.getResultSet(0).getLong(0);
    }
    return 0;
  }

  protected void waitForMinionTaskCompletion(String taskId, long timeout) {
    TestUtils.waitForCondition(aVoid ->
            _controllerStarter.getHelixTaskResourceManager().getTaskState(taskId) == TaskState.COMPLETED,
        timeout, "Failed to complete the task " + taskId);

    // Validate that there were > 0 subtasks so that we know the task was actually run
    Assert.assertFalse(_controllerStarter.getHelixTaskResourceManager().getSubtaskStates(taskId).isEmpty());

    // Validate that all subtasks are completed successfully. A task can be marked completed even if some subtasks
    // failed, so we need to check the subtask states.
    Map<String, TaskPartitionState> subTaskStates = _controllerStarter.getHelixTaskResourceManager()
        .getSubtaskStates(taskId);
    Assert.assertTrue(subTaskStates.values().stream().allMatch(x -> x == TaskPartitionState.COMPLETED),
        "Not all subtasks are completed for task " + taskId + " : " + subTaskStates);
  }

  protected List<String> getSegments(String tableNameWithType) {
    return _controllerStarter.getHelixResourceManager().getSegmentsFor(tableNameWithType, false);
  }

  protected int getSegmentCount(String tableNameWithType) {
    return getSegments(tableNameWithType).size();
  }

  /**
   * Wait for all documents to get loaded.
   *
   * @param timeoutMs Timeout in milliseconds
   * @throws Exception
   */
  protected void waitForAllDocsLoaded(long timeoutMs)
      throws Exception {
    waitForDocsLoaded(timeoutMs, true, getTableName());
  }

  protected void waitForDocsLoaded(long timeoutMs, boolean raiseError, String tableName) {
    long countStarResult = getCountStarResult();
    TestUtils.waitForCondition(() -> getCurrentCountStarResult(tableName) == countStarResult, 100L, timeoutMs,
        "Failed to load " + countStarResult + " documents", raiseError, Duration.ofMillis(timeoutMs / 10));
  }

  /**
   * Wait for servers to remove the table data manager after the table is deleted.
   */
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

  /**
   * Reset table utils.
   */
  protected void resetTable(String tableName, TableType tableType, @Nullable String targetInstance)
      throws IOException {
    getControllerRequestClient().resetTable(TableNameBuilder.forType(tableType).tableNameWithType(tableName),
        targetInstance);
  }

  /**
   * Run equivalent Pinot and H2 query and compare the results.
   */
  protected void testQuery(@Language("sql") String query)
      throws Exception {
    testQuery(query, query);
  }

  /**
   * Run equivalent Pinot and H2 query and compare the results.
   */
  protected void testQuery(@Language("sql") String pinotQuery, @Language("sql") String h2Query)
      throws Exception {
    ClusterIntegrationTestUtils.testQuery(pinotQuery, getBrokerBaseApiUrl(), getPinotConnection(), h2Query,
        getH2Connection(), null, getExtraQueryProperties(), useMultiStageQueryEngine());
  }

  /**
   * Run equivalent Pinot and H2 query and compare the results.
   */
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
    return JsonUtils.stringToJsonNode(
            sendGetRequest(_controllerRequestURLBuilder.forTableAggregateMetadata(getTableName(), List.of(column))))
        .get("columnIndexSizeMap").get(column);
  }

  protected List<ValidDocIdsMetadataInfo> getValidDocIdsMetadata(String tableNameWithType,
      ValidDocIdsType validDocIdsType)
      throws Exception {

    StringBuilder urlBuilder = new StringBuilder(
        _controllerRequestURLBuilder.forValidDocIdsMetadata(tableNameWithType, validDocIdsType.toString()));
    String responseString = sendGetRequest(urlBuilder.toString());
    return JsonUtils.stringToObject(responseString, new TypeReference<>() { });
  }
}
