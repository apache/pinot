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
package org.apache.pinot.plugin.stream.pulsar;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorReporter;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.PartitionGroupMetadata;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamConsumerFactoryProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.stream.TransientConsumerException;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.pulsar.PulsarContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.ingestion.error.IngestionSourceErrorAssertions.assertAuth;
import static org.apache.pinot.spi.ingestion.error.IngestionSourceErrorVerdict.TARGET_BROKER;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class PulsarSourceErrorComponentTest {
  private static final DockerImageName PULSAR_IMAGE = DockerImageName.parse("apachepulsar/pulsar:3.2.2");
  private static final String PULSAR_MEMORY = "-Xms512m -Xmx1g -XX:MaxDirectMemorySize=1g";
  private static final Duration PULSAR_STARTUP_TIMEOUT = Duration.ofMinutes(10);
  private static final String TABLE_NAME_WITH_TYPE = "tableName_REALTIME";
  private static final String CLIENT_ID = "clientId";
  private static final String NAMESPACE = "public/default";
  private static final String TOPIC = "persistent://public/default/source-errors";
  private static final int NUM_PARTITIONS = 2;
  private static final int NUM_MESSAGES = 5000;
  private static final int FETCH_TIMEOUT_MS = 1000;

  private static final String ADMIN_ROLE = "admin";
  private static final String READER_ROLE = "reader";
  private static final String PRODUCER_ROLE = "producer";
  private static final String STRANGER_ROLE = "stranger";

  private final byte[] _signingKey = randomKey();
  private String _adminToken;
  private PulsarContainer _pulsar;

  @BeforeClass
  public void setUp()
      throws Exception {
    _adminToken = token(_signingKey, ADMIN_ROLE);
    _pulsar = new PulsarContainer(PULSAR_IMAGE).withEnv("PULSAR_MEM", PULSAR_MEMORY)
        .withEnv("PULSAR_PREFIX_authenticationEnabled", "true")
        .withEnv("PULSAR_PREFIX_authenticationProviders",
            "org.apache.pulsar.broker.authentication.AuthenticationProviderToken")
        .withEnv("PULSAR_PREFIX_tokenSecretKey", "data:;base64," + Base64.getEncoder().encodeToString(_signingKey))
        .withEnv("PULSAR_PREFIX_authorizationEnabled", "true")
        .withEnv("PULSAR_PREFIX_superUserRoles", ADMIN_ROLE)
        .withEnv("PULSAR_PREFIX_brokerClientAuthenticationPlugin",
            "org.apache.pulsar.client.impl.auth.AuthenticationToken")
        .withEnv("PULSAR_PREFIX_brokerClientAuthenticationParameters", "token:" + _adminToken)
        .waitingFor(Wait.forHttp("/admin/v2/clusters").forPort(PulsarContainer.BROKER_HTTP_PORT)
            .withHeader("Authorization", "Bearer " + _adminToken)
            .forResponsePredicate(body -> body.contains("standalone")).withStartupTimeout(PULSAR_STARTUP_TIMEOUT));
    _pulsar.start();
    try (PulsarAdmin admin = admin()) {
      admin.topics().createPartitionedTopic(TOPIC, NUM_PARTITIONS);
      admin.namespaces().grantPermissionOnNamespace(NAMESPACE, READER_ROLE, EnumSet.of(AuthAction.consume));
      admin.namespaces().grantPermissionOnNamespace(NAMESPACE, PRODUCER_ROLE, EnumSet.of(AuthAction.produce));
    }
    try (PulsarClient client = PulsarClient.builder().serviceUrl(_pulsar.getPulsarBrokerUrl())
        .authentication(AuthenticationFactory.token(_adminToken)).build();
        Producer<byte[]> producer = client.newProducer().topic(partition(0)).enableBatching(false).create()) {
      List<CompletableFuture<MessageId>> sends = new ArrayList<>(NUM_MESSAGES);
      for (int i = 0; i < NUM_MESSAGES; i++) {
        sends.add(producer.sendAsync(("message-" + i).getBytes(StandardCharsets.UTF_8)));
      }
      producer.flush();
      CompletableFuture.allOf(sends.toArray(new CompletableFuture[0])).get();
    }
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    if (_pulsar != null) {
      _pulsar.stop();
    }
  }

  @Test(timeOut = 120_000)
  public void tokenTheBrokerRejectsIsAuth()
      throws Exception {
    StreamConsumerFactory consumerFactory =
        StreamConsumerFactoryProvider.create(streamConfig(token(randomKey(), READER_ROLE)));
    RuntimeException rejected = expectThrows(RuntimeException.class,
        () -> consumerFactory.createPartitionGroupConsumer(CLIENT_ID, consumingStatus(0)));
    assertAuth(rejected, TARGET_BROKER);
  }

  @Test(timeOut = 120_000)
  public void revokedGrantUnderALaggingReaderSurfacesAtTheRecreate()
      throws Exception {
    StreamConsumerFactory consumerFactory = StreamConsumerFactoryProvider.create(streamConfig(token(_signingKey,
        READER_ROLE)));
    long disconnectedAtMs;
    long gaveUpAtMs;
    try (PartitionGroupConsumer consumer = consumerFactory.createPartitionGroupConsumer(CLIENT_ID,
        consumingStatus(0))) {
      MessageBatch<?> batch = consumer.fetchMessages(new MessageIdStreamOffset(MessageId.earliest), 20);
      assertTrue(batch.getMessageCount() < NUM_MESSAGES / 2, "The reader must lag");
      StreamPartitionMsgOffset offset = batch.getOffsetOfNextBatch();

      try (PulsarAdmin admin = admin()) {
        admin.namespaces().revokePermissionsOnNamespace(NAMESPACE, READER_ROLE);
        admin.topics().unload(partition(0));
      }
      disconnectedAtMs = System.currentTimeMillis();
      long deadlineMs = disconnectedAtMs + PulsarPartitionLevelConsumer.MAX_DISCONNECTED_MS + 5 * FETCH_TIMEOUT_MS;
      gaveUpAtMs = -1;
      while (gaveUpAtMs < 0) {
        assertTrue(System.currentTimeMillis() < deadlineMs, "The reader neither reconnected nor gave up");
        try {
          offset = consumer.fetchMessages(offset, FETCH_TIMEOUT_MS).getOffsetOfNextBatch();
        } catch (TransientConsumerException e) {
          gaveUpAtMs = System.currentTimeMillis();
        }
      }
    }
    long disconnectedMs = gaveUpAtMs - disconnectedAtMs;
    assertTrue(disconnectedMs >= PulsarPartitionLevelConsumer.MAX_DISCONNECTED_MS
            && disconnectedMs < PulsarPartitionLevelConsumer.MAX_DISCONNECTED_MS + 5 * FETCH_TIMEOUT_MS,
        "Gave up after " + disconnectedMs + " ms disconnected");

    RuntimeException refused = expectThrows(RuntimeException.class,
        () -> consumerFactory.createPartitionGroupConsumer(CLIENT_ID, consumingStatus(0)));
    assertAuth(refused, TARGET_BROKER);
  }

  @Test(timeOut = 120_000)
  public void droppedLaggingReaderThatReconnectsReadsOn()
      throws Exception {
    try (PulsarAdmin admin = admin()) {
      admin.namespaces().grantPermissionOnNamespace(NAMESPACE, READER_ROLE, EnumSet.of(AuthAction.consume));
    }
    StreamConsumerFactory consumerFactory = StreamConsumerFactoryProvider.create(streamConfig(token(_signingKey,
        READER_ROLE)));
    try (PartitionGroupConsumer consumer = consumerFactory.createPartitionGroupConsumer(CLIENT_ID,
        consumingStatus(0))) {
      MessageBatch<?> batch = consumer.fetchMessages(new MessageIdStreamOffset(MessageId.earliest), 20);
      assertTrue(batch.getMessageCount() < NUM_MESSAGES / 2, "The reader must lag");
      int numConsumed = batch.getMessageCount();
      StreamPartitionMsgOffset offset = batch.getOffsetOfNextBatch();

      try (PulsarAdmin admin = admin()) {
        admin.topics().unload(partition(0));
      }
      long deadlineMs = System.currentTimeMillis() + PulsarPartitionLevelConsumer.MAX_DISCONNECTED_MS;
      while (numConsumed < NUM_MESSAGES) {
        assertTrue(System.currentTimeMillis() < deadlineMs, "The reader didn't reconnect: read " + numConsumed);
        batch = consumer.fetchMessages(offset, FETCH_TIMEOUT_MS);
        numConsumed += batch.getMessageCount();
        offset = batch.getOffsetOfNextBatch();
      }
    }
  }

  @Test(timeOut = 120_000)
  public void metadataReportsTheRefusalsItSwallows()
      throws Exception {
    assertRefusalReported(STRANGER_ROLE, ExecutionException.class);
    assertRefusalReported(PRODUCER_ROLE, PulsarClientException.class);
  }

  private void assertRefusalReported(String role, Class<? extends Throwable> reportedType)
      throws Exception {
    StreamConfig streamConfig = streamConfig(token(_signingKey, role));
    List<Throwable> reported = new ArrayList<>();
    List<PartitionGroupMetadata> metadata;
    try (PulsarStreamMetadataProvider metadataProvider = new PulsarStreamMetadataProvider(CLIENT_ID, streamConfig);
        IngestionSourceErrorReporter.Scope ignored = IngestionSourceErrorReporter.listen(reported::add)) {
      metadata = metadataProvider.computePartitionGroupMetadata(CLIENT_ID, streamConfig,
          List.of(consumingStatus(0)), FETCH_TIMEOUT_MS);
    }
    assertEquals(metadata.stream().map(PartitionGroupMetadata::getPartitionGroupId).collect(Collectors.toList()),
        List.of(0));
    assertEquals(reported.size(), 1, role + ": " + reported);
    assertTrue(reportedType.isInstance(reported.get(0)), role + ": " + reported.get(0));
    assertAuth(reported.get(0), TARGET_BROKER);
  }

  private StreamConfig streamConfig(String token) {
    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", "pulsar");
    streamConfigMap.put("stream.pulsar.topic.name", TOPIC);
    streamConfigMap.put("stream.pulsar.bootstrap.servers", _pulsar.getPulsarBrokerUrl());
    streamConfigMap.put("stream.pulsar.authenticationToken", token);
    streamConfigMap.put("stream.pulsar.consumer.prop.auto.offset.reset", "smallest");
    streamConfigMap.put("stream.pulsar.consumer.factory.class.name", PulsarConsumerFactory.class.getName());
    streamConfigMap.put("stream.pulsar.decoder.class.name", "dummy");
    return new StreamConfig(TABLE_NAME_WITH_TYPE, streamConfigMap);
  }

  private static PartitionGroupConsumptionStatus consumingStatus(int partition) {
    return new PartitionGroupConsumptionStatus(partition, 0, new MessageIdStreamOffset(MessageId.earliest), null,
        "CONSUMING");
  }

  private static String partition(int partition) {
    return TOPIC + "-partition-" + partition;
  }

  private PulsarAdmin admin()
      throws PulsarClientException {
    return PulsarAdmin.builder().serviceHttpUrl(_pulsar.getHttpServiceUrl())
        .authentication(AuthenticationFactory.token(_adminToken)).build();
  }

  private static byte[] randomKey() {
    byte[] key = new byte[32];
    new SecureRandom().nextBytes(key);
    return key;
  }

  private static String token(byte[] key, String role)
      throws Exception {
    Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();
    String headerAndPayload =
        encoder.encodeToString("{\"alg\":\"HS256\"}".getBytes(StandardCharsets.UTF_8)) + "." + encoder.encodeToString(
            ("{\"sub\":\"" + role + "\"}").getBytes(StandardCharsets.UTF_8));
    Mac mac = Mac.getInstance("HmacSHA256");
    mac.init(new SecretKeySpec(key, "HmacSHA256"));
    return headerAndPayload + "." + encoder.encodeToString(
        mac.doFinal(headerAndPayload.getBytes(StandardCharsets.UTF_8)));
  }
}
