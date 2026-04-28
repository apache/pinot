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

import com.google.common.primitives.Longs;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.integration.tests.kafka.schemaregistry.SchemaRegistryStarter;
import org.apache.pinot.plugin.inputformat.avro.AvroUtils;
import org.apache.pinot.plugin.inputformat.avro.confluent.KafkaConfluentSchemaRegistryAvroMessageDecoder;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Integration test that extends RealtimeClusterIntegrationTest but uses low-level Kafka consumer.
 * TODO: Add separate module-level tests and remove the randomness of this test
 */
public class KafkaConfluentSchemaRegistryAvroMessageDecoderRealtimeClusterIntegrationTest
    extends BaseRealtimeClusterIntegrationTest {
  private static final String CONSUMER_DIRECTORY = "/tmp/consumer-test";
  private static final String SHARED_TABLE_NAME = "confluent_schema_registry_avro";
  private static final String SHARED_KAFKA_TOPIC = "confluent_schema_registry_avro";
  private static final long RANDOM_SEED = System.currentTimeMillis();
  private static final Random RANDOM = new Random(RANDOM_SEED);
  private static final int NUM_INVALID_RECORDS = 5;

  private final String _resourceSuffix = Long.toUnsignedString(RANDOM.nextLong(), Character.MAX_RADIX);
  private final boolean _isDirectAlloc = RANDOM.nextBoolean();
  private final boolean _isConsumerDirConfigured = RANDOM.nextBoolean();
  private final boolean _enableLeadControllerResource = RANDOM.nextBoolean();
  private SchemaRegistryStarter.KafkaSchemaRegistryInstance _schemaRegistry;
  private File _classTempDir;
  private String _previousMaxSegmentPreprocessParallelism;
  private String _previousMaxSegmentStarTreePreprocessParallelism;
  private boolean _clusterConfigOverridesApplied;

  @Override
  protected int getNumKafkaBrokers() {
    return 1;
  }

  @Override
  protected int getSharedNumBrokers() {
    return 1;
  }

  @Override
  protected int getSharedNumServers() {
    return 1;
  }

  @Override
  protected boolean shouldStartSharedKafka() {
    return true;
  }

  @Override
  protected boolean shouldStartSharedMinion() {
    return false;
  }

  @Override
  protected void startKafka() {
    super.startKafka();
    startSchemaRegistry();
  }

  @Override
  protected void stopKafka() {
    stopSchemaRegistry();
    super.stopKafka();
  }

  private void startSchemaRegistry() {
    if (_schemaRegistry == null) {
      _schemaRegistry = SchemaRegistryStarter.startLocalInstance(SchemaRegistryStarter.DEFAULT_PORT);
    }
  }

  private void stopSchemaRegistry() {
    try {
      if (_schemaRegistry != null) {
        _schemaRegistry.stop();
        _schemaRegistry = null;
      }
    } catch (Exception e) {
      // Swallow exceptions
    }
  }

  @Override
  protected void pushAvroIntoKafka(List<File> avroFiles)
      throws Exception {
    Properties avroProducerProps = new Properties();
    avroProducerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getKafkaPort());
    avroProducerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, _schemaRegistry.getUrl());
    avroProducerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArraySerializer");
    avroProducerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "io.confluent.kafka.serializers.KafkaAvroSerializer");
    Producer<byte[], GenericRecord> avroProducer = new KafkaProducer<>(avroProducerProps);

    // this producer produces intentionally malformatted records so that
    // we can test the behavior when consuming such records
    Properties nonAvroProducerProps = new Properties();
    nonAvroProducerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getKafkaPort());
    nonAvroProducerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArraySerializer");
    nonAvroProducerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArraySerializer");
    Producer<byte[], byte[]> invalidDataProducer = new KafkaProducer<>(nonAvroProducerProps);

    if (injectTombstones()) {
      // publish lots of tombstones to livelock the consumer if it can't handle this properly
      for (int i = 0; i < 1000; i++) {
        // publish a tombstone first
        avroProducer.send(
            new ProducerRecord<>(getKafkaTopic(), Longs.toByteArray(System.currentTimeMillis()), null));
      }
    }

    for (File avroFile : avroFiles) {
      int numInvalidRecords = 0;
      try (DataFileStream<GenericRecord> reader = AvroUtils.getAvroReader(avroFile)) {
        for (GenericRecord genericRecord : reader) {
          byte[] keyBytes = (getPartitionColumn() == null) ? Longs.toByteArray(System.currentTimeMillis())
              : (genericRecord.get(getPartitionColumn())).toString().getBytes();

          if (numInvalidRecords < NUM_INVALID_RECORDS) {
            // send a few rubbish records to validate that the consumer will skip over non-avro records, but
            // don't spam them every time as it causes log spam
            invalidDataProducer.send(new ProducerRecord<>(getKafkaTopic(), keyBytes, "Rubbish".getBytes(UTF_8)));
            numInvalidRecords++;
          }

          avroProducer.send(new ProducerRecord<>(getKafkaTopic(), keyBytes, genericRecord));
        }
      }
    }
  }

  @Override
  protected Map<String, String> getStreamConfigs() {
    Map<String, String> streamConfigMap = super.getStreamConfigs();
    String streamType = "kafka";
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_DECODER_CLASS),
        KafkaConfluentSchemaRegistryAvroMessageDecoder.class.getName());
    streamConfigMap.put("stream.kafka.decoder.prop.schema.registry.rest.url", _schemaRegistry.getUrl());
    return streamConfigMap;
  }

  @Override
  protected boolean injectTombstones() {
    return true;
  }

  @Override
  protected String getLoadMode() {
    return ReadMode.mmap.name();
  }

  @Override
  public void startController()
      throws Exception {
    super.startController();
    enableResourceConfigForLeadControllerResource(_enableLeadControllerResource);
  }

  @Override
  protected void overrideServerConf(PinotConfiguration configuration) {
    configuration.setProperty(CommonConstants.Server.CONFIG_OF_REALTIME_OFFHEAP_ALLOCATION, true);
    configuration.setProperty(CommonConstants.Server.CONFIG_OF_REALTIME_OFFHEAP_DIRECT_ALLOCATION, _isDirectAlloc);
    if (_isConsumerDirConfigured) {
      configuration.setProperty(CommonConstants.Server.CONFIG_OF_CONSUMER_DIR,
          getConsumerDirectory().getAbsolutePath());
    }
  }

  @Override
  protected void createSegmentsAndUpload(List<File> avroFiles, Schema schema, TableConfig tableConfig)
      throws Exception {
    File tarDir = getClassTarDir();
    File segmentDir = getClassSegmentDir();
    if (!tarDir.exists()) {
      tarDir.mkdir();
    }
    if (!segmentDir.exists()) {
      segmentDir.mkdir();
    }

    // create segments out of the avro files (segments will be placed in tarDir)
    List<File> copyOfAvroFiles = new ArrayList<>(avroFiles);
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(copyOfAvroFiles, tableConfig, schema, 0, segmentDir, tarDir);

    // upload segments to controller
    uploadSegmentsToController(getTableName(), tarDir, false, false);

    // upload the first segment again to verify refresh
    uploadSegmentsToController(getTableName(), tarDir, true, false);

    // upload the first segment again to verify refresh with different segment crc
    uploadSegmentsToController(getTableName(), tarDir, true, true);

    // add avro files to the original list so H2 will have the uploaded data as well
    avroFiles.addAll(copyOfAvroFiles);
  }

  private void uploadSegmentsToController(String tableName, File tarDir, boolean onlyFirstSegment, boolean changeCrc)
      throws Exception {
    File[] segmentTarFiles = tarDir.listFiles();
    assertNotNull(segmentTarFiles);
    int numSegments = segmentTarFiles.length;
    assertTrue(numSegments > 0);
    if (onlyFirstSegment) {
      numSegments = 1;
    }
    URI uploadSegmentHttpURI = URI.create(getOrCreateAdminClient().getSegmentUploadUrl());
    try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient()) {
      if (numSegments == 1) {
        File segmentTarFile = segmentTarFiles[0];
        if (changeCrc) {
          changeCrcInSegmentZKMetadata(tableName, segmentTarFile.toString());
        }
        assertEquals(
            fileUploadDownloadClient.uploadSegment(uploadSegmentHttpURI, segmentTarFile.getName(), segmentTarFile,
                tableName, TableType.REALTIME).getStatusCode(), HttpStatus.SC_OK);
      } else {
        // Upload segments in parallel
        ExecutorService executorService = Executors.newFixedThreadPool(numSegments);
        List<Future<Integer>> futures = new ArrayList<>(numSegments);
        for (File segmentTarFile : segmentTarFiles) {
          futures.add(executorService.submit(
              () -> fileUploadDownloadClient.uploadSegment(uploadSegmentHttpURI, segmentTarFile.getName(),
                  segmentTarFile, tableName, TableType.REALTIME).getStatusCode()));
        }
        executorService.shutdown();
        for (Future<Integer> future : futures) {
          assertEquals((int) future.get(), HttpStatus.SC_OK);
        }
      }
    }
  }

  private void changeCrcInSegmentZKMetadata(String tableName, String segmentFilePath) {
    int startIdx = segmentFilePath.lastIndexOf(tableName + "_");
    int endIdx = segmentFilePath.indexOf(".tar.gz");
    String segmentName = segmentFilePath.substring(startIdx, endIdx);
    String tableNameWithType = TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(tableName);
    SegmentZKMetadata segmentZKMetadata = _helixResourceManager.getSegmentZKMetadata(tableNameWithType, segmentName);
    segmentZKMetadata.setCrc(111L);
    _helixResourceManager.updateZkMetadata(tableNameWithType, segmentZKMetadata);
  }

  @Override
  protected long getCountStarResult() {
    // all the data that was ingested from Kafka also got uploaded via the controller's upload endpoint
    return super.getCountStarResult() * 2;
  }

  @BeforeClass
  @Override
  public void setUp()
      throws Exception {
    System.out.println(format(
        "Using random seed: %s, isDirectAlloc: %s, isConsumerDirConfigured: %s, enableLeadControllerResource: %s",
        RANDOM_SEED, _isDirectAlloc, _isConsumerDirConfigured, _enableLeadControllerResource));

    _classTempDir = getClassTempDir();
    TestUtils.ensureDirectoriesExistAndEmpty(_classTempDir);

    // Remove the consumer directory
    FileUtils.deleteQuietly(getConsumerDirectory());

    // Start the Pinot cluster
    startZk();
    // Start Kafka
    startKafka();
    startController();
    if (isSharedRichClusterEnabled()) {
      enableResourceConfigForLeadControllerResource(_enableLeadControllerResource);
      rememberClusterConfigOverrides();
    }

    HelixConfigScope scope = getClusterConfigScope();
    // Set max segment preprocess parallelism to 8
    _helixManager.getConfigAccessor()
        .set(scope, CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM, Integer.toString(8));
    // Set max segment startree preprocess parallelism to 6
    _helixManager.getConfigAccessor()
        .set(scope, CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM, Integer.toString(6));

    startBroker();
    startServer();

    cleanRealtimeTableAndSchema();
    resetKafkaTopic();

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_classTempDir);

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createRealtimeTableConfig(avroFiles.get(0));
    addTableConfig(tableConfig);
    waitForAllRealtimePartitionsConsuming(TableNameBuilder.REALTIME.tableNameWithType(getTableName()),
        getRealtimePartitionsReadyTimeoutMs());

    // Push data into Kafka
    pushAvroIntoKafka(avroFiles);

    // create segments and upload them to controller
    createSegmentsAndUpload(avroFiles, schema, tableConfig);

    // Set up the H2 connection
    setUpH2Connection(avroFiles);

    // Initialize the query generator
    setUpQueryGenerator(avroFiles);

    runValidationJob(600_000);

    // Wait for all documents loaded
    waitForAllDocsLoaded(getDocsLoadedTimeoutMs());
  }

  @AfterClass(alwaysRun = true)
  @Override
  public void tearDown()
      throws Exception {
    Exception exception = null;
    exception = runCleanup(exception, this::cleanRealtimeTableAndSchema);
    exception = runCleanup(exception, this::deleteKafkaTopicIfPresent);
    exception = runCleanup(exception, this::restoreClusterConfigOverrides);
    exception = runCleanup(exception, this::stopSchemaRegistry);
    if (!isSharedRichClusterEnabled()) {
      exception = runCleanup(exception, this::stopServerIfStarted);
      exception = runCleanup(exception, this::stopBrokerIfStarted);
      exception = runCleanup(exception, this::stopControllerIfStarted);
      exception = runCleanup(exception, this::stopKafkaIfStarted);
      exception = runCleanup(exception, this::stopZk);
    }
    exception = runCleanup(exception, () -> FileUtils.deleteDirectory(getConsumerDirectory()));
    exception = runCleanup(exception, this::deleteClassTempDir);
    if (exception != null) {
      throw exception;
    }
  }

  @Override
  protected String getTableName() {
    return isSharedRichClusterEnabled() ? SHARED_TABLE_NAME + "_" + _resourceSuffix : super.getTableName();
  }

  @Override
  protected String getKafkaTopic() {
    return isSharedRichClusterEnabled() ? SHARED_KAFKA_TOPIC + "_" + _resourceSuffix : super.getKafkaTopic();
  }

  @Override
  protected void testQuery(String query)
      throws Exception {
    super.testQuery(rewriteSharedTableName(query));
  }

  @Override
  protected void testQuery(String pinotQuery, String h2Query)
      throws Exception {
    super.testQuery(rewriteSharedTableName(pinotQuery), rewriteSharedTableName(h2Query));
  }

  @Override
  protected void testQueryWithMatchingRowCount(String pinotQuery, String h2Query)
      throws Exception {
    super.testQueryWithMatchingRowCount(rewriteSharedTableName(pinotQuery), rewriteSharedTableName(h2Query));
  }

  private String rewriteSharedTableName(String query) {
    return isSharedRichClusterEnabled() ? query.replace(DEFAULT_TABLE_NAME, getTableName()) : query;
  }

  private File getClassTempDir() {
    return isSharedRichClusterEnabled()
        ? new File(FileUtils.getTempDirectory(), getClass().getSimpleName() + "-" + _resourceSuffix)
        : _tempDir;
  }

  private File getClassSegmentDir() {
    return isSharedRichClusterEnabled() ? new File(getClassTempDir(), "segmentDir") : _segmentDir;
  }

  private File getClassTarDir() {
    return isSharedRichClusterEnabled() ? new File(getClassTempDir(), "tarDir") : _tarDir;
  }

  private File getConsumerDirectory() {
    return isSharedRichClusterEnabled()
        ? new File(FileUtils.getTempDirectory(), getClass().getSimpleName() + "-consumer-" + _resourceSuffix)
        : new File(CONSUMER_DIRECTORY);
  }

  private void cleanRealtimeTableAndSchema()
      throws Exception {
    if (_helixResourceManager == null) {
      return;
    }

    String tableName = getTableName();
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    if (_helixResourceManager.getTableConfig(realtimeTableName) != null
        || _helixResourceManager.hasRealtimeTable(tableName)) {
      dropRealtimeTable(tableName);
      waitForTableDataManagerRemoved(realtimeTableName);
      waitForEVToDisappear(realtimeTableName);
    }
    if (_helixResourceManager.getSchema(tableName) != null) {
      deleteSchema(tableName);
    }
  }

  private void resetKafkaTopic() {
    deleteKafkaTopicIfPresent();
    createKafkaTopic(getKafkaTopic());
  }

  private void deleteKafkaTopicIfPresent() {
    if (isKafkaTopicPresent()) {
      deleteKafkaTopic(getKafkaTopic());
    }
  }

  private boolean isKafkaTopicPresent() {
    if (_kafkaStarters == null || _kafkaStarters.isEmpty()) {
      return false;
    }
    Properties adminProps = new Properties();
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaBrokerList());
    adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
    adminProps.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");
    try (AdminClient adminClient = AdminClient.create(adminProps)) {
      return adminClient.listTopics().names().get(5, TimeUnit.SECONDS).contains(getKafkaTopic());
    } catch (Exception e) {
      return false;
    }
  }

  private void rememberClusterConfigOverrides() {
    if (_clusterConfigOverridesApplied) {
      return;
    }

    _previousMaxSegmentPreprocessParallelism =
        getClusterConfig(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM);
    _previousMaxSegmentStarTreePreprocessParallelism =
        getClusterConfig(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM);
    _clusterConfigOverridesApplied = true;
  }

  private void restoreClusterConfigOverrides()
      throws Exception {
    if (!_clusterConfigOverridesApplied) {
      return;
    }

    restoreClusterConfig(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM,
        _previousMaxSegmentPreprocessParallelism);
    restoreClusterConfig(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM,
        _previousMaxSegmentStarTreePreprocessParallelism);
    _clusterConfigOverridesApplied = false;
  }

  private String getClusterConfig(String key) {
    return _helixManager.getConfigAccessor().get(getClusterConfigScope(), key);
  }

  private void restoreClusterConfig(String key, String value)
      throws Exception {
    if (value == null) {
      deleteClusterConfig(key);
    } else {
      updateClusterConfig(Map.of(key, value));
    }
  }

  private HelixConfigScope getClusterConfigScope() {
    return new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(getHelixClusterName())
        .build();
  }

  private void stopServerIfStarted() {
    if (!_serverStarters.isEmpty()) {
      stopServer();
    }
  }

  private void stopBrokerIfStarted() {
    if (!_brokerStarters.isEmpty()) {
      stopBroker();
    }
  }

  private void stopControllerIfStarted() {
    if (_controllerStarter != null) {
      stopController();
    }
  }

  private void stopKafkaIfStarted() {
    if (_kafkaStarters != null && !_kafkaStarters.isEmpty()) {
      stopKafka();
    }
  }

  private void deleteClassTempDir()
      throws Exception {
    if (_classTempDir != null) {
      FileUtils.deleteDirectory(_classTempDir);
    }
  }

  private Exception runCleanup(Exception firstException, Cleanup cleanup) {
    try {
      cleanup.run();
    } catch (Exception e) {
      if (firstException == null) {
        return e;
      }
      firstException.addSuppressed(e);
    }
    return firstException;
  }

  private interface Cleanup {
    void run()
        throws Exception;
  }
}
