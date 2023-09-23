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
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
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
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpStatus;
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
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Integration test that extends RealtimeClusterIntegrationTest but uses low-level Kafka consumer.
 * TODO: Add separate module-level tests and remove the randomness of this test
 */
@Test(suiteName = "integration-suite-1", groups = {"integration-suite-1"})
public class KafkaConfluentSchemaRegistryAvroMessageDecoderRealtimeClusterIntegrationTest
    extends BaseRealtimeClusterIntegrationTest {
  private static final String CONSUMER_DIRECTORY = "/tmp/consumer-test";
  private static final long RANDOM_SEED = System.currentTimeMillis();
  private static final Random RANDOM = new Random(RANDOM_SEED);
  private static final int NUM_INVALID_RECORDS = 5;

  private final boolean _isDirectAlloc = RANDOM.nextBoolean();
  private final boolean _isConsumerDirConfigured = RANDOM.nextBoolean();
  private final boolean _enableLeadControllerResource = RANDOM.nextBoolean();
  private SchemaRegistryStarter.KafkaSchemaRegistryInstance _schemaRegistry;

  @Override
  protected int getNumKafkaBrokers() {
    return 1;
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
    avroProducerProps.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, _schemaRegistry.getUrl());
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
  protected void startController()
      throws Exception {
    super.startController();
    enableResourceConfigForLeadControllerResource(_enableLeadControllerResource);
  }

  @Override
  protected void overrideServerConf(PinotConfiguration configuration) {
    configuration.setProperty(CommonConstants.Server.CONFIG_OF_REALTIME_OFFHEAP_ALLOCATION, true);
    configuration.setProperty(CommonConstants.Server.CONFIG_OF_REALTIME_OFFHEAP_DIRECT_ALLOCATION, _isDirectAlloc);
    if (_isConsumerDirConfigured) {
      configuration.setProperty(CommonConstants.Server.CONFIG_OF_CONSUMER_DIR, CONSUMER_DIRECTORY);
    }
  }

  @Override
  protected void createSegmentsAndUpload(List<File> avroFiles, Schema schema, TableConfig tableConfig)
      throws Exception {
    if (!_tarDir.exists()) {
      _tarDir.mkdir();
    }
    if (!_segmentDir.exists()) {
      _segmentDir.mkdir();
    }

    // create segments out of the avro files (segments will be placed in _tarDir)
    List<File> copyOfAvroFiles = new ArrayList<>(avroFiles);
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(copyOfAvroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);

    // upload segments to controller
    uploadSegmentsToController(getTableName(), _tarDir, false, false);

    // upload the first segment again to verify refresh
    uploadSegmentsToController(getTableName(), _tarDir, true, false);

    // upload the first segment again to verify refresh with different segment crc
    uploadSegmentsToController(getTableName(), _tarDir, true, true);

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
    URI uploadSegmentHttpURI = URI.create(getControllerRequestURLBuilder().forSegmentUpload());
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
    int startIdx = segmentFilePath.indexOf("mytable_");
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
    System.out.println("this.getClass().getName() = " + this.getClass().getName());
    System.out.println(format(
        "Using random seed: %s, isDirectAlloc: %s, isConsumerDirConfigured: %s, enableLeadControllerResource: %s",
        RANDOM_SEED, _isDirectAlloc, _isConsumerDirConfigured, _enableLeadControllerResource));

    // Remove the consumer directory
    FileUtils.deleteQuietly(new File(CONSUMER_DIRECTORY));

    super.setUp();
  }

  @AfterClass
  @Override
  public void tearDown()
      throws Exception {
    FileUtils.deleteDirectory(new File(CONSUMER_DIRECTORY));
    super.tearDown();
  }
}
