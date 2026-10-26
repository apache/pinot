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

import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.pinot.common.metrics.AbstractMetrics;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.plugin.inputformat.avro.confluent.KafkaConfluentSchemaRegistryAvroMessageDecoder;
import org.apache.pinot.plugin.inputformat.json.JSONMessageDecoder;
import org.apache.pinot.plugin.stream.kafka.KafkaStreamConfigProperties;
import org.apache.pinot.plugin.stream.kafka30.KafkaConsumerFactory;
import org.apache.pinot.plugin.stream.kafka30.server.EmbeddedKafkaCluster;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// End-to-end proof that a real controller and server record the failures of a stream source as timestamp gauges:
/// AUTH when the source refuses our credential or permission, SOURCE_READ when it can't serve us. The server writes
/// them per partition, the controller per table.
///
/// Every test starts its own embedded Kafka with a SASL/PLAIN listener and the standard authorizer, with a fresh
/// super-user for setup and a fresh principal for Pinot, so breaking one test's broker or ACLs touches nothing another
/// test uses. A table created with bad credentials is rejected and deleted, so each case first creates a table that
/// consumes and then breaks it the way a source does. The credentials are random and fake, and never go into a log line
/// or an assertion message.
///
/// The server's delay tracker refreshes the latest offsets every 30 seconds and records SOURCE_READ only after 6
/// failures in a row, so each SOURCE_READ case takes about 3.5 minutes.
public class IngestionSourceErrorSaslKafkaIntegrationTest extends BaseClusterIntegrationTest {
  private static final String PLAIN_LOGIN_MODULE = "org.apache.kafka.common.security.plain.PlainLoginModule";
  private static final int PARTITION = 0;
  private static final long SERVER_AUTH_TIMEOUT_MS = 60_000L;
  // 6 offset refreshes 30 s apart, each failing after its 5 s timeout, plus the wait for the first one
  private static final long DELAY_TRACKER_SOURCE_READ_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(6);
  private static final int DELAY_TRACKER_FAILURES_TO_GIVE_UP = 6;

  private static final String EVENT_SCHEMA_JSON = "{\"type\":\"record\",\"name\":\"Event\",\"fields\":["
      + "{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"ts\",\"type\":\"long\"}]}";
  private static final org.apache.avro.Schema EVENT_SCHEMA = new org.apache.avro.Schema.Parser().parse(
      EVENT_SCHEMA_JSON);
  private static final String REGISTRY_DENIED =
      "{\"error_code\":40301,\"message\":\"User is denied operation on this server.\"}";

  private EmbeddedKafkaCluster _kafka;
  private AdminClient _kafkaAdmin;
  private String _adminJaasConfig;
  private String _pinotPrincipal;
  private String _pinotJaasConfig;

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);
    startZk();
    startController();
    startBroker();
    startServer();
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    stopServer();
    stopBroker();
    stopController();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  /// Starts a Kafka broker that serves only SASL/PLAIN on its client listener and authorizes every request, with a
  /// super-user for the test's own setup and a principal for Pinot that starts with no ACLs.
  @BeforeMethod
  public void startSaslKafka() {
    String adminUser = "admin" + randomHex();
    String adminPassword = "fakesecret" + randomHex();
    _pinotPrincipal = "pinot" + randomHex();
    String pinotPassword = "fakesecret" + randomHex();
    _adminJaasConfig = jaasConfig(adminUser, adminPassword, "");
    _pinotJaasConfig = jaasConfig(_pinotPrincipal, pinotPassword, "");

    Properties brokerProps = new Properties();
    // The embedded cluster names its client listener EXTERNAL and its KRaft listener CONTROLLER
    brokerProps.setProperty("listener.security.protocol.map", "EXTERNAL:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT");
    brokerProps.setProperty("sasl.enabled.mechanisms", "PLAIN");
    brokerProps.setProperty("sasl.mechanism.inter.broker.protocol", "PLAIN");
    brokerProps.setProperty("listener.name.external.plain.sasl.jaas.config", jaasConfig(adminUser, adminPassword,
        " user_" + adminUser + "=\"" + adminPassword + "\" user_" + _pinotPrincipal + "=\"" + pinotPassword + "\""));
    brokerProps.setProperty("authorizer.class.name", "org.apache.kafka.metadata.authorizer.StandardAuthorizer");
    // ANONYMOUS is the broker itself on the plaintext KRaft listener
    brokerProps.setProperty("super.users", "User:" + adminUser + ";User:ANONYMOUS");
    brokerProps.setProperty("allow.everyone.if.no.acl.found", "false");
    // As on managed Kafka. With auto-creation on, a missing topic reads as a topic authorization failure to a
    // principal that may not create it.
    brokerProps.setProperty("auto.create.topics.enable", "false");
    _kafka = new EmbeddedKafkaCluster();
    _kafka.init(brokerProps);
    _kafka.start();

    _kafkaAdmin = AdminClient.create(saslClientProps(_adminJaasConfig));
    TestUtils.waitForCondition(aVoid -> {
      try {
        return _kafkaAdmin.describeCluster().nodes().get(5, TimeUnit.SECONDS).size() == 1;
      } catch (Exception e) {
        return false;
      }
    }, 200L, 60_000L, "SASL Kafka is not ready");
  }

  @AfterMethod(alwaysRun = true)
  public void stopSaslKafka() {
    if (_kafkaAdmin != null) {
      _kafkaAdmin.close(Duration.ofSeconds(10));
      _kafkaAdmin = null;
    }
    stopKafkaBroker();
  }

  /// Revoking the topic ACL: the server partition's AUTH gauge is set at the first refusal, and the controller table's
  /// after a forced validation run. The consumer still gives up and stops consuming, as before.
  @Test
  public void testRevokedTopicAclSetsAuth()
      throws Exception {
    String tableName = "sourceErrorRevokedAcl";
    String tableNameWithType = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    createConsumingJsonTable(tableName, Map.of());
    try {
      long brokenAtSeconds = nowSeconds();
      deletePinotAcls(tableName);

      waitForGaugeSince(ServerMetrics.get(), serverGaugeName(ServerGauge.INGESTION_SOURCE_AUTH_FAILURE_TIME_SECONDS,
          tableNameWithType), brokenAtSeconds, SERVER_AUTH_TIMEOUT_MS);
      TestUtils.waitForCondition(aVoid -> !Long.valueOf(1L).equals(partitionConsuming(tableNameWithType, tableName)),
          200L, 60_000L, "Consumption did not stop for table: " + tableNameWithType);

      runRealtimeSegmentValidation();
      waitForGaugeSince(ControllerMetrics.get(), controllerGaugeName(
          ControllerGauge.INGESTION_SOURCE_AUTH_FAILURE_TIME_SECONDS, tableNameWithType), brokenAtSeconds, 10_000L);

      assertNoSourceReadGauges(tableNameWithType);
    } finally {
      dropTable(tableName);
    }
  }

  /// Deleting the topic: the controller's SOURCE_READ gauge is set through the Kafka plugin's evidence on "Topic does
  /// not exist", and the server's through the delay tracker, since the consumer only sees empty fetches.
  @Test
  public void testDeletedTopicSetsSourceRead()
      throws Exception {
    String tableName = "sourceErrorDeletedTopic";
    String tableNameWithType = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    createConsumingJsonTable(tableName, Map.of());
    try {
      long brokenAtSeconds = nowSeconds();
      _kafkaAdmin.deleteTopics(List.of(tableName)).all().get(30, TimeUnit.SECONDS);
      TestUtils.waitForCondition(aVoid -> {
        try {
          return !_kafkaAdmin.listTopics().names().get(5, TimeUnit.SECONDS).contains(tableName);
        } catch (Exception e) {
          return false;
        }
      }, 200L, 60_000L, "Topic is not deleted: " + tableName);

      runRealtimeSegmentValidation();
      waitForGaugeSince(ControllerMetrics.get(), controllerGaugeName(
          ControllerGauge.INGESTION_SOURCE_READ_FAILURE_TIME_SECONDS, tableNameWithType), brokenAtSeconds, 10_000L);

      waitForGaugeSince(ServerMetrics.get(), serverGaugeName(ServerGauge.INGESTION_SOURCE_READ_FAILURE_TIME_SECONDS,
          tableNameWithType), brokenAtSeconds, DELAY_TRACKER_SOURCE_READ_TIMEOUT_MS);
      assertSetByDelayTrackerOnly(tableNameWithType, tableName);
      assertNoAuthGauges(tableNameWithType);
    } finally {
      dropTable(tableName);
    }
  }

  /// Stopping the broker: Kafka's consumer answers with empty fetches, so the server's SOURCE_READ gauge comes from the
  /// delay tracker. The controller's comes from its metadata fetch timing out.
  ///
  /// Each controller fetch attempt against the dead broker waits out its 15 s deadline, then closes Kafka's admin
  /// client, which waits for its abandoned topic listings to time out: `default.api.timeout.ms`, shortened here from
  /// 60 s (with `request.timeout.ms`, which it must not undercut). The controller half runs the segment status checker,
  /// which reaches the metadata fetch once (3 attempts). A forced validation run reaches the same fetch but retries its
  /// IdealState update 10 times, so it takes 10 times as long.
  @Test
  public void testStoppedBrokerSetsSourceRead()
      throws Exception {
    String tableName = "sourceErrorStoppedBroker";
    String tableNameWithType = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    createConsumingJsonTable(tableName, Map.of(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, "5000",
        CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000"));
    try {
      long brokenAtSeconds = nowSeconds();
      _kafkaAdmin.close(Duration.ofSeconds(10));
      _kafkaAdmin = null;
      stopKafkaBroker();

      getOrCreateAdminClient().getClusterClient().runPeriodicTask("SegmentStatusChecker", tableName, "REALTIME");
      waitForGaugeSince(ControllerMetrics.get(), controllerGaugeName(
          ControllerGauge.INGESTION_SOURCE_READ_FAILURE_TIME_SECONDS, tableNameWithType), brokenAtSeconds, 180_000L);

      waitForGaugeSince(ServerMetrics.get(), serverGaugeName(ServerGauge.INGESTION_SOURCE_READ_FAILURE_TIME_SECONDS,
          tableNameWithType), brokenAtSeconds, DELAY_TRACKER_SOURCE_READ_TIMEOUT_MS);
      assertSetByDelayTrackerOnly(tableNameWithType, tableName);
      assertNoAuthGauges(tableNameWithType);
    } finally {
      dropTable(tableName);
    }
  }

  /// The real Confluent Avro decoder against a registry that starts refusing the decoder's credential: the partition's
  /// AUTH gauge is set while every fetch from Kafka succeeds.
  @Test
  public void testSchemaRegistryRefusalSetsAuthWhileFetchesSucceed()
      throws Exception {
    String tableName = "sourceErrorRegistryDenied";
    String tableNameWithType = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    try (RegistryStub registry = new RegistryStub(200, schemaResponse())) {
      createConfluentAvroTable(tableName, registry);
      try {
        // Working: schema 1 is served, and the client caches it
        produce(tableName, List.of(registryFramed(1, avroEvent(1)), registryFramed(1, avroEvent(2))));
        waitForCount(tableName, 2);

        // The registry now refuses us; rows that need a new schema can no longer be decoded
        registry.answer(403, REGISTRY_DENIED);
        int requestsBeforeRefusal = registry.requests();
        long brokenAtSeconds = nowSeconds();
        produce(tableName, List.of(registryFramed(2, avroEvent(3)), registryFramed(2, avroEvent(4))));

        waitForGaugeSince(ServerMetrics.get(), serverGaugeName(ServerGauge.INGESTION_SOURCE_AUTH_FAILURE_TIME_SECONDS,
            tableNameWithType), brokenAtSeconds, SERVER_AUTH_TIMEOUT_MS);
        assertTrue(registry.requests() > requestsBeforeRefusal, "The decoder never asked the registry for schema 2");
        assertTrue(rowsDropped(tableNameWithType, tableName) > 0, "No row was fetched and dropped");
        assertEquals(partitionConsuming(tableNameWithType, tableName), Long.valueOf(1L),
            "The partition stopped consuming");
        assertNull(ServerMetrics.get().getGaugeValue(
            serverGaugeName(ServerGauge.INGESTION_SOURCE_READ_FAILURE_TIME_SECONDS, tableNameWithType)));

        // The controller never talks to the registry
        runRealtimeSegmentValidation();
        assertNoControllerGauges(tableNameWithType);
      } finally {
        dropTable(tableName);
      }
    }
  }

  /// The same decoder and a registry that serves the schema, fed rows that won't parse: every bad row is dropped and
  /// nothing is recorded, since the content is the producer's, not the source's verdict.
  @Test
  public void testUnparsableContentSetsNothing()
      throws Exception {
    String tableName = "sourceErrorBadBytes";
    String tableNameWithType = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    try (RegistryStub registry = new RegistryStub(200, schemaResponse())) {
      createConfluentAvroTable(tableName, registry);
      try {
        List<byte[]> payloads = new ArrayList<>();
        // Not registry-framed
        payloads.add("not avro".getBytes(StandardCharsets.UTF_8));
        // Registry-framed, but the bytes don't match the schema
        payloads.add(registryFramed(1, new byte[]{(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff}));
        // Valid rows after the bad ones prove the bad ones were consumed
        payloads.add(registryFramed(1, avroEvent(1)));
        payloads.add(registryFramed(1, avroEvent(2)));
        produce(tableName, payloads);

        waitForCount(tableName, 2);
        assertEquals(rowsDropped(tableNameWithType, tableName), 2L);
        assertTrue(registry.requests() > 0, "The decoder never asked the registry for the schema");
        assertNoServerGauges(tableNameWithType);

        runRealtimeSegmentValidation();
        assertNoControllerGauges(tableNameWithType);
      } finally {
        dropTable(tableName);
      }
    }
  }

  private void createConsumingJsonTable(String tableName, Map<String, String> extraStreamConfigs)
      throws Exception {
    createTopicReadableByPinot(tableName);
    Map<String, String> streamConfigs = streamConfigs(tableName, JSONMessageDecoder.class.getName(), Map.of());
    streamConfigs.putAll(extraStreamConfigs);
    createTable(tableName, streamConfigs);
    long now = System.currentTimeMillis();
    List<byte[]> rows = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      rows.add(JsonUtils.objectToString(Map.of("id", i, "ts", now)).getBytes(StandardCharsets.UTF_8));
    }
    produce(tableName, rows);
    waitForCount(tableName, rows.size());
  }

  private void createConfluentAvroTable(String tableName, RegistryStub registry)
      throws Exception {
    createTopicReadableByPinot(tableName);
    Map<String, String> decoderProps = new HashMap<>();
    decoderProps.put("schema.registry.rest.url", registry.url());
    decoderProps.put("basic.auth.credentials.source", "USER_INFO");
    decoderProps.put("basic.auth.user.info", "registry" + randomHex() + ":fakeregistrysecret" + randomHex());
    createTable(tableName,
        streamConfigs(tableName, KafkaConfluentSchemaRegistryAvroMessageDecoder.class.getName(), decoderProps));
  }

  private void createTopicReadableByPinot(String topic)
      throws Exception {
    _kafkaAdmin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);
    _kafkaAdmin.createAcls(pinotAcls(topic)).all().get(30, TimeUnit.SECONDS);
    // Wait until Pinot's principal can see the topic
    Properties consumerProps = saslClientProps(_pinotJaasConfig);
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
      TestUtils.waitForCondition(aVoid -> {
        try {
          return !consumer.partitionsFor(topic, Duration.ofSeconds(5)).isEmpty();
        } catch (Exception e) {
          return false;
        }
      }, 200L, 60_000L, "Pinot's principal cannot see topic: " + topic);
    }
  }

  private void deletePinotAcls(String topic)
      throws Exception {
    List<AclBinding> acls = pinotAcls(topic);
    _kafkaAdmin.deleteAcls(List.of(acls.get(0).toFilter(), acls.get(1).toFilter())).all().get(30, TimeUnit.SECONDS);
  }

  private List<AclBinding> pinotAcls(String topic) {
    ResourcePattern pattern = new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL);
    return List.of(
        new AclBinding(pattern,
            new AccessControlEntry("User:" + _pinotPrincipal, "*", AclOperation.READ, AclPermissionType.ALLOW)),
        new AclBinding(pattern,
            new AccessControlEntry("User:" + _pinotPrincipal, "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW)));
  }

  private void produce(String topic, List<byte[]> payloads)
      throws Exception {
    Properties producerProps = saslClientProps(_adminJaasConfig);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps)) {
      for (byte[] payload : payloads) {
        producer.send(new ProducerRecord<>(topic, payload)).get(30, TimeUnit.SECONDS);
      }
    }
  }

  private Map<String, String> streamConfigs(String topic, String decoderClass, Map<String, String> decoderProps) {
    String streamType = "kafka";
    Map<String, String> streamConfigs = new HashMap<>();
    streamConfigs.put(StreamConfigProperties.STREAM_TYPE, streamType);
    streamConfigs.put(StreamConfigProperties.constructStreamProperty(streamType,
        StreamConfigProperties.STREAM_TOPIC_NAME), topic);
    streamConfigs.put(KafkaStreamConfigProperties.constructStreamProperty(
        KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_BROKER_LIST), _kafka.bootstrapServers());
    streamConfigs.put(StreamConfigProperties.constructStreamProperty(streamType,
        StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS), KafkaConsumerFactory.class.getName());
    streamConfigs.put(StreamConfigProperties.constructStreamProperty(streamType,
        StreamConfigProperties.STREAM_DECODER_CLASS), decoderClass);
    for (Map.Entry<String, String> entry : decoderProps.entrySet()) {
      streamConfigs.put(StreamConfigProperties.constructStreamProperty(streamType,
          StreamConfigProperties.DECODER_PROPS_PREFIX + "." + entry.getKey()), entry.getValue());
    }
    streamConfigs.put(StreamConfigProperties.constructStreamProperty(streamType,
        StreamConfigProperties.STREAM_CONSUMER_OFFSET_CRITERIA), "smallest");
    streamConfigs.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, "1000000");
    streamConfigs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
    streamConfigs.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
    streamConfigs.put(SaslConfigs.SASL_JAAS_CONFIG, _pinotJaasConfig);
    return streamConfigs;
  }

  private void createTable(String tableName, Map<String, String> streamConfigs)
      throws Exception {
    addSchema(new Schema.SchemaBuilder().setSchemaName(tableName)
        .addSingleValueDimension("id", DataType.INT)
        .addDateTime("ts", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build());
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setStreamIngestionConfig(new StreamIngestionConfig(List.of(streamConfigs)));
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(tableName)
        .setTimeColumnName("ts")
        .setIngestionConfig(ingestionConfig)
        .build();
    addTableConfig(tableConfig);
  }

  private void dropTable(String tableName)
      throws Exception {
    dropRealtimeTable(tableName);
    waitForEVToDisappear(TableNameBuilder.REALTIME.tableNameWithType(tableName));
    deleteSchema(tableName);
  }

  private void waitForCount(String tableName, long expected) {
    TestUtils.waitForCondition(() -> getCurrentCountStarResult(tableName) == expected, 200L, 60_000L,
        "Table " + tableName + " did not reach " + expected + " rows", Duration.ofSeconds(10));
  }

  private void runRealtimeSegmentValidation() {
    _controllerStarter.getRealtimeSegmentValidationManager().run();
  }

  private void stopKafkaBroker() {
    if (_kafka != null) {
      _kafka.stop();
      _kafka = null;
    }
  }

  /// The consume loop never threw, and the delay tracker failed enough offset refreshes in a row to give up.
  private static void assertSetByDelayTrackerOnly(String tableNameWithType, String topic) {
    ServerMetrics serverMetrics = ServerMetrics.get();
    assertEquals(serverMetrics.getMeteredTableValue(tableNameWithType + "_" + topic,
        ServerMeter.REALTIME_CONSUMPTION_EXCEPTIONS).count(), 0L, "The consume loop saw an exception");
    assertEquals(partitionConsuming(tableNameWithType, topic), Long.valueOf(1L), "The partition stopped consuming");
    assertTrue(serverMetrics.getMeteredTableValue(tableNameWithType,
            ServerMeter.INGESTION_DELAY_LATEST_OFFSET_FETCH_ERRORS).count() >= DELAY_TRACKER_FAILURES_TO_GIVE_UP,
        "The delay tracker did not fail enough offset refreshes to give up");
  }

  private static void assertNoAuthGauges(String tableNameWithType) {
    assertNull(ServerMetrics.get().getGaugeValue(
        serverGaugeName(ServerGauge.INGESTION_SOURCE_AUTH_FAILURE_TIME_SECONDS, tableNameWithType)));
    assertNull(ControllerMetrics.get().getGaugeValue(
        controllerGaugeName(ControllerGauge.INGESTION_SOURCE_AUTH_FAILURE_TIME_SECONDS, tableNameWithType)));
  }

  private static void assertNoSourceReadGauges(String tableNameWithType) {
    assertNull(ServerMetrics.get().getGaugeValue(
        serverGaugeName(ServerGauge.INGESTION_SOURCE_READ_FAILURE_TIME_SECONDS, tableNameWithType)));
    assertNull(ControllerMetrics.get().getGaugeValue(
        controllerGaugeName(ControllerGauge.INGESTION_SOURCE_READ_FAILURE_TIME_SECONDS, tableNameWithType)));
  }

  private static void assertNoServerGauges(String tableNameWithType) {
    assertNull(ServerMetrics.get().getGaugeValue(
        serverGaugeName(ServerGauge.INGESTION_SOURCE_AUTH_FAILURE_TIME_SECONDS, tableNameWithType)));
    assertNull(ServerMetrics.get().getGaugeValue(
        serverGaugeName(ServerGauge.INGESTION_SOURCE_READ_FAILURE_TIME_SECONDS, tableNameWithType)));
  }

  private static void assertNoControllerGauges(String tableNameWithType) {
    assertNull(ControllerMetrics.get().getGaugeValue(
        controllerGaugeName(ControllerGauge.INGESTION_SOURCE_AUTH_FAILURE_TIME_SECONDS, tableNameWithType)));
    assertNull(ControllerMetrics.get().getGaugeValue(
        controllerGaugeName(ControllerGauge.INGESTION_SOURCE_READ_FAILURE_TIME_SECONDS, tableNameWithType)));
  }

  /// Waits for a timestamp gauge to record a failure at or after `sinceSeconds`.
  private static void waitForGaugeSince(AbstractMetrics<?, ?, ?, ?> metrics, String gaugeName, long sinceSeconds,
      long timeoutMs) {
    TestUtils.waitForCondition(aVoid -> {
      Long value = metrics.getGaugeValue(gaugeName);
      return value != null && value >= sinceSeconds;
    }, 500L, timeoutMs, "Gauge not set: " + gaugeName);
    Long value = metrics.getGaugeValue(gaugeName);
    assertNotNull(value);
    assertTrue(value <= nowSeconds(), "Gauge " + gaugeName + " is in the future: " + value);
  }

  private static String serverGaugeName(ServerGauge gauge, String tableNameWithType) {
    return gauge.getGaugeName() + "." + tableNameWithType + "." + PARTITION;
  }

  private static String controllerGaugeName(ControllerGauge gauge, String tableNameWithType) {
    return gauge.getGaugeName() + "." + tableNameWithType;
  }

  @Nullable
  private static Long partitionConsuming(String tableNameWithType, String topic) {
    return ServerMetrics.get().getGaugeValue(
        ServerGauge.LLC_PARTITION_CONSUMING.getGaugeName() + "." + consumerClientId(tableNameWithType, topic));
  }

  private static long rowsDropped(String tableNameWithType, String topic) {
    return ServerMetrics.get().getMeteredTableValue(consumerClientId(tableNameWithType, topic),
        ServerMeter.INVALID_REALTIME_ROWS_DROPPED).count();
  }

  private static String consumerClientId(String tableNameWithType, String topic) {
    return tableNameWithType + "-" + topic + "-" + PARTITION;
  }

  private Properties saslClientProps(String jaasConfig) {
    Properties props = new Properties();
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, _kafka.bootstrapServers());
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
    props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
    props.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);
    props.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, "10000");
    props.put(CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG, "10000");
    return props;
  }

  private static String jaasConfig(String user, String password, String extraOptions) {
    return PLAIN_LOGIN_MODULE + " required username=\"" + user + "\" password=\"" + password + "\"" + extraOptions
        + ";";
  }

  private static String randomHex() {
    return UUID.randomUUID().toString().replace("-", "").substring(0, 12);
  }

  private static long nowSeconds() {
    return TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
  }

  private static String schemaResponse()
      throws IOException {
    return JsonUtils.objectToString(Map.of("schema", EVENT_SCHEMA_JSON));
  }

  private static byte[] avroEvent(int id)
      throws IOException {
    GenericRecord event = new GenericData.Record(EVENT_SCHEMA);
    event.put("id", id);
    event.put("ts", System.currentTimeMillis());
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    new GenericDatumWriter<GenericRecord>(EVENT_SCHEMA).write(event, encoder);
    encoder.flush();
    return out.toByteArray();
  }

  /// The schema registry wire format: magic byte 0, the schema id, then the Avro body.
  private static byte[] registryFramed(int schemaId, byte[] body) {
    return ByteBuffer.allocate(5 + body.length).put((byte) 0).putInt(schemaId).put(body).array();
  }

  /// A schema registry that answers every request with one status and body, which the test can change.
  private static final class RegistryStub implements AutoCloseable {
    private final HttpServer _server;
    private final AtomicInteger _requests = new AtomicInteger();
    private volatile int _status;
    private volatile String _body;

    RegistryStub(int status, String body)
        throws IOException {
      answer(status, body);
      _server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
      _server.createContext("/", exchange -> {
        _requests.incrementAndGet();
        byte[] response = _body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/vnd.schemaregistry.v1+json");
        exchange.sendResponseHeaders(_status, response.length);
        exchange.getResponseBody().write(response);
        exchange.close();
      });
      _server.start();
    }

    void answer(int status, String body) {
      _body = body;
      _status = status;
    }

    String url() {
      return "http://localhost:" + _server.getAddress().getPort();
    }

    int requests() {
      return _requests.get();
    }

    @Override
    public void close() {
      _server.stop(0);
    }
  }
}
