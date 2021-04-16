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

import com.google.common.base.Function;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.client.ConnectionFactory;
import org.apache.pinot.client.Request;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.plugin.stream.kafka.KafkaStreamConfigProperties;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.ReplicaGroupStrategyConfig;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.stream.StreamDataServerStartable;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.tools.utils.KafkaStarterUtils;
import org.apache.pinot.util.TestUtils;
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
  protected static final int DEFAULT_HLC_SEGMENT_FLUSH_SIZE = 20000;
  protected static final int DEFAULT_LLC_NUM_KAFKA_BROKERS = 2;
  protected static final int DEFAULT_HLC_NUM_KAFKA_BROKERS = 1;
  protected static final int DEFAULT_LLC_NUM_KAFKA_PARTITIONS = 2;
  protected static final int DEFAULT_HLC_NUM_KAFKA_PARTITIONS = 10;
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
  protected Connection _h2Connection;
  protected QueryGenerator _queryGenerator;

  /**
   * The following getters can be overridden to change default settings.
   */

  protected String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  protected String getSchemaName() {
    return DEFAULT_SCHEMA_NAME;
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

  protected boolean useLlc() {
    return false;
  }

  protected boolean useKafkaTransaction() {
    return false;
  }

  protected String getStreamConsumerFactoryClassName() {
    return KafkaStarterUtils.KAFKA_STREAM_CONSUMER_FACTORY_CLASS_NAME;
  }

  protected int getRealtimeSegmentFlushSize() {
    if (useLlc()) {
      return DEFAULT_LLC_SEGMENT_FLUSH_SIZE;
    } else {
      return DEFAULT_HLC_SEGMENT_FLUSH_SIZE;
    }
  }

  protected int getNumKafkaBrokers() {
    if (useLlc()) {
      return DEFAULT_LLC_NUM_KAFKA_BROKERS;
    } else {
      return DEFAULT_HLC_NUM_KAFKA_BROKERS;
    }
  }

  protected int getBaseKafkaPort() {
    return KafkaStarterUtils.DEFAULT_KAFKA_PORT;
  }

  protected String getKafkaZKAddress() {
    return getZkUrl() + "/kafka";
  }

  protected int getNumKafkaPartitions() {
    if (useLlc()) {
      return DEFAULT_LLC_NUM_KAFKA_PARTITIONS;
    } else {
      return DEFAULT_HLC_NUM_KAFKA_PARTITIONS;
    }
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
    return DEFAULT_INVERTED_INDEX_COLUMNS;
  }

  @Nullable
  protected List<String> getNoDictionaryColumns() {
    return DEFAULT_NO_DICTIONARY_COLUMNS;
  }

  @Nullable
  protected List<String> getRangeIndexColumns() {
    return DEFAULT_RANGE_INDEX_COLUMNS;
  }

  @Nullable
  protected List<String> getBloomFilterColumns() {
    return DEFAULT_BLOOM_FILTER_COLUMNS;
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

  protected boolean getNullHandlingEnabled() {
    return DEFAULT_NULL_HANDLING_ENABLED;
  }

  /**
   * The following methods are based on the getters. Override the getters for non-default settings before calling these
   * methods.
   */

  /**
   * Creates a new schema.
   */
  protected Schema createSchema()
      throws IOException {
    URL resourceUrl = BaseClusterIntegrationTest.class.getClassLoader().getResource(getSchemaFileName());
    Assert.assertNotNull(resourceUrl);
    return Schema.fromFile(new File(resourceUrl.getFile()));
  }

  /**
   * Returns the schema in the cluster.
   */
  protected Schema getSchema() {
    return getSchema(getSchemaName());
  }

  /**
   * Creates a new OFFLINE table config.
   */
  protected TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(getTableName()).setSchemaName(getSchemaName())
        .setTimeColumnName(getTimeColumnName()).setSortedColumn(getSortedColumn())
        .setInvertedIndexColumns(getInvertedIndexColumns()).setNoDictionaryColumns(getNoDictionaryColumns())
        .setRangeIndexColumns(getRangeIndexColumns()).setBloomFilterColumns(getBloomFilterColumns())
        .setFieldConfigList(getFieldConfigs()).setNumReplicas(getNumReplicas()).setSegmentVersion(getSegmentVersion())
        .setLoadMode(getLoadMode()).setTaskConfig(getTaskConfig()).setBrokerTenant(getBrokerTenant())
        .setServerTenant(getServerTenant()).setIngestionConfig(getIngestionConfig())
        .setNullHandlingEnabled(getNullHandlingEnabled()).build();
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
    boolean useLlc = useLlc();
    if (useLlc) {
      // LLC
      streamConfigMap
          .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_TYPES),
              StreamConfig.ConsumerType.LOWLEVEL.toString());
      streamConfigMap.put(KafkaStreamConfigProperties
              .constructStreamProperty(KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_BROKER_LIST),
          KafkaStarterUtils.DEFAULT_KAFKA_BROKER);
      if (useKafkaTransaction()) {
        streamConfigMap.put(KafkaStreamConfigProperties
                .constructStreamProperty(KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_ISOLATION_LEVEL),
            KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_ISOLATION_LEVEL_READ_COMMITTED);
      }
    } else {
      // HLC
      streamConfigMap
          .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_TYPES),
              StreamConfig.ConsumerType.HIGHLEVEL.toString());
      streamConfigMap.put(KafkaStreamConfigProperties
              .constructStreamProperty(KafkaStreamConfigProperties.HighLevelConsumer.KAFKA_HLC_ZK_CONNECTION_STRING),
          getKafkaZKAddress());
      streamConfigMap.put(KafkaStreamConfigProperties
              .constructStreamProperty(KafkaStreamConfigProperties.HighLevelConsumer.KAFKA_HLC_BOOTSTRAP_SERVER),
          KafkaStarterUtils.DEFAULT_KAFKA_BROKER);
    }
    streamConfigMap.put(StreamConfigProperties
            .constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS),
        getStreamConsumerFactoryClassName());
    streamConfigMap
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_TOPIC_NAME),
            getKafkaTopic());
    streamConfigMap
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_DECODER_CLASS),
            AvroFileSchemaKafkaAvroMessageDecoder.class.getName());
    streamConfigMap
        .put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, Integer.toString(getRealtimeSegmentFlushSize()));
    streamConfigMap.put(StreamConfigProperties
        .constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_OFFSET_CRITERIA), "smallest");
    return streamConfigMap;
  }

  /**
   * Creates a new REALTIME table config.
   */
  protected TableConfig createRealtimeTableConfig(File sampleAvroFile) {
    AvroFileSchemaKafkaAvroMessageDecoder.avroFile = sampleAvroFile;
    return new TableConfigBuilder(TableType.REALTIME).setTableName(getTableName()).setSchemaName(getSchemaName())
        .setTimeColumnName(getTimeColumnName()).setSortedColumn(getSortedColumn())
        .setInvertedIndexColumns(getInvertedIndexColumns()).setNoDictionaryColumns(getNoDictionaryColumns())
        .setRangeIndexColumns(getRangeIndexColumns()).setBloomFilterColumns(getBloomFilterColumns())
        .setFieldConfigList(getFieldConfigs()).setNumReplicas(getNumReplicas()).setSegmentVersion(getSegmentVersion())
        .setLoadMode(getLoadMode()).setTaskConfig(getTaskConfig()).setBrokerTenant(getBrokerTenant())
        .setServerTenant(getServerTenant()).setIngestionConfig(getIngestionConfig()).setLLC(useLlc())
        .setStreamConfigs(getStreamConfigs()).setNullHandlingEnabled(getNullHandlingEnabled()).build();
  }

  /**
   * Creates a new Upsert enabled table config.
   */
  protected TableConfig createUpsertTableConfig(File sampleAvroFile, String primaryKeyColumn, int numPartitions) {
    AvroFileSchemaKafkaAvroMessageDecoder.avroFile = sampleAvroFile;
    Map<String, ColumnPartitionConfig> columnPartitionConfigMap = new HashMap<>();
    columnPartitionConfigMap.put(primaryKeyColumn, new ColumnPartitionConfig("Murmur", numPartitions));

    return new TableConfigBuilder(TableType.REALTIME).setTableName(getTableName()).setSchemaName(getSchemaName())
        .setTimeColumnName(getTimeColumnName())
        .setFieldConfigList(getFieldConfigs()).setNumReplicas(getNumReplicas()).setSegmentVersion(getSegmentVersion())
        .setLoadMode(getLoadMode()).setTaskConfig(getTaskConfig()).setBrokerTenant(getBrokerTenant())
        .setServerTenant(getServerTenant()).setIngestionConfig(getIngestionConfig()).setLLC(useLlc())
        .setStreamConfigs(getStreamConfigs()).setNullHandlingEnabled(getNullHandlingEnabled())
        .setRoutingConfig(new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE))
        .setSegmentPartitionConfig(new SegmentPartitionConfig(columnPartitionConfigMap))
        .setReplicaGroupStrategyConfig(new ReplicaGroupStrategyConfig(primaryKeyColumn, 1))
        .setUpsertConfig(new UpsertConfig((UpsertConfig.Mode.FULL))).build();
  }

  /**
   * Returns the REALTIME table config in the cluster.
   */
  protected TableConfig getRealtimeTableConfig() {
    return getRealtimeTableConfig(getTableName());
  }

  /**
   * Get the Pinot connection.
   *
   * @return Pinot connection
   */
  protected org.apache.pinot.client.Connection getPinotConnection() {
    if (_pinotConnection == null) {
      _pinotConnection = ConnectionFactory.fromZookeeper(getZkUrl() + "/" + getHelixClusterName());
    }
    return _pinotConnection;
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
   * Sets up the H2 connection to a table with pre-loaded data.
   */
  protected void setUpH2Connection(List<File> avroFiles)
      throws Exception {
    Assert.assertNull(_h2Connection);
    Class.forName("org.h2.Driver");
    _h2Connection = DriverManager.getConnection("jdbc:h2:mem:");
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

  /**
   * Unpack the tarred Avro data into the given directory.
   *
   * @param outputDir Output directory
   * @return List of files unpacked.
   * @throws Exception
   */
  protected List<File> unpackAvroData(File outputDir)
      throws Exception {
    URL resourceUrl = BaseClusterIntegrationTest.class.getClassLoader().getResource(getAvroTarFileName());
    Assert.assertNotNull(resourceUrl);
    return TarGzCompressionUtils.untar(new File(resourceUrl.getFile()), outputDir);
  }

  /**
   * Pushes the data in the given Avro files into a Kafka stream.
   *
   * @param avroFiles List of Avro files
   */
  protected void pushAvroIntoKafka(List<File> avroFiles)
      throws Exception {
    ClusterIntegrationTestUtils.pushAvroIntoKafka(avroFiles, "localhost:" + getBaseKafkaPort(), getKafkaTopic(),
        getMaxNumKafkaMessagesPerBatch(), getKafkaMessageHeader(), getPartitionColumn());
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
    Properties kafkaConfig = KafkaStarterUtils.getDefaultKafkaConfiguration();
    _kafkaStarters = KafkaStarterUtils.startServers(getNumKafkaBrokers(), getBaseKafkaPort(), getKafkaZKAddress(),
        kafkaConfig);
    _kafkaStarters.get(0)
        .createTopic(getKafkaTopic(), KafkaStarterUtils.getTopicCreationProps(getNumKafkaPartitions()));
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
   * @throws Exception
   */
  protected long getCurrentCountStarResult()
      throws Exception {
    return getPinotConnection().execute(new Request("pql", "SELECT COUNT(*) FROM " + getTableName())).getResultSet(0)
        .getLong(0);
  }

  /**
   * Wait for all documents to get loaded.
   *
   * @param timeoutMs Timeout in milliseconds
   * @throws Exception
   */
  protected void waitForAllDocsLoaded(long timeoutMs)
      throws Exception {
    waitForDocsLoaded(timeoutMs, true);
  }

  protected void waitForDocsLoaded(long timeoutMs, boolean raiseError) {
    final long countStarResult = getCountStarResult();
    TestUtils.waitForCondition(new Function<Void, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable Void aVoid) {
        try {
          return getCurrentCountStarResult() == countStarResult;
        } catch (Exception e) {
          return null;
        }
      }
    }, 100L, timeoutMs, "Failed to load " + countStarResult + " documents", raiseError);
  }

  /**
   * Run equivalent Pinot and H2 query and compare the results.
   *
   * @param pqlQuery Pinot query
   * @param sqlQueries H2 query
   * @throws Exception
   */
  protected void testQuery(String pqlQuery, @Nullable List<String> sqlQueries)
      throws Exception {
    ClusterIntegrationTestUtils
        .testPqlQuery(pqlQuery, _brokerBaseApiUrl, getPinotConnection(), sqlQueries, getH2Connection());
  }

  /**
   * Run equivalent Pinot SQL and H2 query and compare the results.
   *
   * @param pinotQuery Pinot query
   * @param sqlQueries H2 query
   * @throws Exception
   */
  protected void testSqlQuery(String pinotQuery, @Nullable List<String> sqlQueries)
      throws Exception {
    ClusterIntegrationTestUtils
        .testSqlQuery(pinotQuery, _brokerBaseApiUrl, getPinotConnection(), sqlQueries, getH2Connection());
  }
}
