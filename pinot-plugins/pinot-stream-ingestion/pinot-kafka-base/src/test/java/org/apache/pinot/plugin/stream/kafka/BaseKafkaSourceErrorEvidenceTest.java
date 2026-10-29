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
package org.apache.pinot.plugin.stream.kafka;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorEvidence;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorReporter;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.PermanentConsumerException;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.TransientConsumerException;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.apache.pinot.spi.utils.retry.RetryPolicy;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.ingestion.error.IngestionSourceErrorAssertions.assertSourceRead;
import static org.apache.pinot.spi.ingestion.error.IngestionSourceErrorVerdict.TARGET_BROKER;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Pins that a Kafka plugin hands the classifier what it knows about a failure: the real cause behind a consumer it
/// gave up recreating, the broker's answer behind a synthesized "topic does not exist" or a topic listing that timed
/// out. Each Kafka module runs it against its own plugin, its kafka-clients version and a real broker.
public abstract class BaseKafkaSourceErrorEvidenceTest {
  private static final String TOPIC = "events";

  /// The module's partition consumer, built through the constructor that creates its Kafka consumer under a retry
  /// policy, as the server does when it recreates a consumer.
  protected abstract PartitionGroupConsumer createConsumer(StreamConfig streamConfig, RetryPolicy retryPolicy);

  protected abstract StreamMetadataProvider createMetadataProvider(StreamConfig streamConfig);

  /// Starts a single broker and returns its bootstrap servers.
  protected abstract String startBroker()
      throws Exception;

  protected abstract void stopBroker()
      throws Exception;

  @Test
  public void recreateReportsTheLastAttemptsFailureAndThrowsWithoutIt() {
    StreamConfig streamConfig = streamConfig("no-such-broker.invalid:9092", Map.of());
    List<Throwable> reported = new ArrayList<>();
    RuntimeException giveUp;
    try (IngestionSourceErrorReporter.Scope ignored = IngestionSourceErrorReporter.listen(reported::add)) {
      giveUp = expectThrows(RuntimeException.class,
          () -> createConsumer(streamConfig, RetryPolicies.noDelayRetryPolicy(2)));
    }
    // Kafka's own messages can quote config values, and the thrown exception reaches the segment error
    assertTrue(giveUp.getCause() instanceof AttemptsExceededException, ExceptionUtils.getStackTrace(giveUp));
    assertNull(giveUp.getCause().getCause(), ExceptionUtils.getStackTrace(giveUp));
    assertEquals(reported.size(), 1);
    assertEquals(assertSourceRead(reported.get(0), TARGET_BROKER).getCode(), "ConfigException");
  }

  @Test
  public void missingTopicCarriesTheBrokersAnswer()
      throws Exception {
    String bootstrapServers = startBroker();
    try (StreamMetadataProvider provider = createMetadataProvider(
        streamConfig(bootstrapServers, Map.of("allow.auto.create.topics", "false")))) {
      PermanentConsumerException missing =
          expectThrows(PermanentConsumerException.class, () -> provider.fetchPartitionCount(2_000));
      assertEquals(assertSourceRead(missing, TARGET_BROKER).getCode(),
          IngestionSourceErrorEvidence.UNKNOWN_TOPIC_OR_PARTITION);
    } finally {
      stopBroker();
    }
  }

  @Test
  public void topicListingTimeoutIsUnreachable()
      throws Exception {
    // The kernel completes the TCP handshake into the backlog, but nothing ever answers a Kafka request.
    ServerSocket silentBroker = new ServerSocket(0);
    // Short client timeouts, and the broker gone before the provider closes: otherwise Kafka's admin client can wait
    // indefinitely in close() for the listing it abandoned.
    StreamConfig streamConfig = streamConfig("localhost:" + silentBroker.getLocalPort(),
        Map.of("request.timeout.ms", "1000", "default.api.timeout.ms", "1000"));
    try (StreamMetadataProvider provider = createMetadataProvider(streamConfig)) {
      TransientConsumerException timedOut;
      try {
        timedOut = expectThrows(TransientConsumerException.class, () -> provider.fetchPartitionCount(1_000));
      } finally {
        silentBroker.close();
      }
      assertTrue(timedOut.getCause() instanceof java.util.concurrent.TimeoutException,
          ExceptionUtils.getStackTrace(timedOut));
      assertEquals(assertSourceRead(timedOut, TARGET_BROKER).getCode(), IngestionSourceErrorEvidence.UNREACHABLE);
    } finally {
      silentBroker.close();
    }
  }

  private static StreamConfig streamConfig(String bootstrapServers, Map<String, String> kafkaConfigs) {
    Map<String, String> streamConfigMap = new HashMap<>(kafkaConfigs);
    streamConfigMap.put("streamType", "kafka");
    streamConfigMap.put("stream.kafka.topic.name", TOPIC);
    streamConfigMap.put("stream.kafka.broker.list", bootstrapServers);
    streamConfigMap.put("stream.kafka.decoder.class.name", "unused");
    return new StreamConfig("table_REALTIME", streamConfigMap);
  }
}
