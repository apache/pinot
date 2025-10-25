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
package org.apache.pinot.plugin.stream.kafka20;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.pinot.plugin.stream.kafka.KafkaAdminClientManager;
import org.apache.pinot.plugin.stream.kafka.KafkaPartitionLevelStreamConfig;
import org.apache.pinot.plugin.stream.kafka.KafkaSSLUtils;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.apache.pinot.spi.utils.retry.RetriableOperationException;
import org.apache.pinot.spi.utils.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * KafkaPartitionLevelConnectionHandler provides low level APIs to access Kafka partition level information.
 * E.g. partition counts, offsets per partition.
 *
 */
public abstract class KafkaPartitionLevelConnectionHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaPartitionLevelConnectionHandler.class);
  private static final Set<String> CONSUMER_CONFIG_NAMES = ConsumerConfig.configNames();
  private static final Set<String> ADMIN_CLIENT_CONFIG_NAMES = AdminClientConfig.configNames();
  protected final KafkaPartitionLevelStreamConfig _config;
  protected final String _clientId;
  protected final int _partition;
  protected final String _topic;
  protected final Consumer<String, Bytes> _consumer;
  protected final TopicPartition _topicPartition;
  protected final Properties _consumerProp;
  protected volatile KafkaAdminClientManager.AdminClientReference _sharedAdminClientRef;

  public KafkaPartitionLevelConnectionHandler(String clientId, StreamConfig streamConfig, int partition) {
    this(clientId, streamConfig, partition, null);
  }

  public KafkaPartitionLevelConnectionHandler(String clientId, StreamConfig streamConfig, int partition,
      @Nullable RetryPolicy retryPolicy) {
    _config = new KafkaPartitionLevelStreamConfig(streamConfig);
    _clientId = clientId;
    _partition = partition;
    _topic = _config.getKafkaTopicName();
    _consumerProp = buildProperties(streamConfig);
    KafkaSSLUtils.initSSL(_consumerProp);
    if (retryPolicy == null) {
      _consumer = createConsumer(_consumerProp);
    } else {
      _consumer = createConsumer(_consumerProp, retryPolicy);
    }
    _topicPartition = new TopicPartition(_topic, _partition);
    _consumer.assign(Collections.singletonList(_topicPartition));
  }

  private Properties buildProperties(StreamConfig streamConfig) {
    Properties consumerProp = new Properties();
    consumerProp.putAll(streamConfig.getStreamConfigsMap());
    consumerProp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, _config.getBootstrapHosts());
    consumerProp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
    if (_config.getKafkaIsolationLevel() != null) {
      consumerProp.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, _config.getKafkaIsolationLevel());
    }
    consumerProp.put(ConsumerConfig.CLIENT_ID_CONFIG, _clientId);
    return consumerProp;
  }

  /**
   * Filter properties to only include the specified Kafka configurations.
   * This prevents "was supplied but isn't a known config" warnings from Kafka clients.
   *
   * @param props The properties to filter
   * @param validConfigNames The set of valid configuration names for the target Kafka client
   * @return A new Properties object containing only the valid configurations
   */
  private Properties filterKafkaProperties(Properties props, Set<String> validConfigNames) {
    Properties filteredProps = new Properties();
    for (String key : props.stringPropertyNames()) {
      if (validConfigNames.contains(key)) {
        filteredProps.put(key, props.get(key));
      }
    }
    return filteredProps;
  }

  private Consumer<String, Bytes> createConsumer(Properties consumerProp, RetryPolicy retryPolicy) {
    AtomicReference<Consumer<String, Bytes>> consumer = new AtomicReference<>();
    try {
      retryPolicy.attempt(() -> {
        try {
          consumer.set(new KafkaConsumer<>(filterKafkaProperties(consumerProp, CONSUMER_CONFIG_NAMES)));
          return true;
        } catch (Exception e) {
          LOGGER.warn("Caught exception while creating Kafka consumer, retrying.", e);
          return false;
        }
      });
    } catch (AttemptsExceededException | RetriableOperationException e) {
      LOGGER.error("Caught exception while creating Kafka consumer, giving up", e);
      throw new RuntimeException(e);
    }
    return consumer.get();
  }

  @VisibleForTesting
  protected Consumer<String, Bytes> createConsumer(Properties consumerProp) {
    return retry(() -> new KafkaConsumer<>(filterKafkaProperties(consumerProp, CONSUMER_CONFIG_NAMES)), 5);
  }

  protected AdminClient createAdminClient() {
    return retry(() -> AdminClient.create(filterKafkaProperties(_consumerProp, ADMIN_CLIENT_CONFIG_NAMES)), 5);
  }

  /**
   * Gets or creates a reusable admin client instance. The admin client is lazily initialized
   * and reused across multiple calls to avoid the overhead of creating new connections.
   *
   * @return the admin client instance
   */
  @Deprecated
  protected AdminClient getOrCreateAdminClient() {
    return createAdminClient();
  }

  /**
   * Gets or creates a shared admin client instance that can be reused across multiple
   * connection handlers connecting to the same Kafka cluster. This provides better
   * resource efficiency when multiple consumers/producers connect to the same bootstrap servers.
   *
   * @return the shared admin client instance
   */
  protected AdminClient getOrCreateSharedAdminClient() {
    return getOrCreateSharedAdminClientInternal(false);
  }

  private AdminClient getOrCreateSharedAdminClientInternal(boolean isRetry) {
    KafkaAdminClientManager.AdminClientReference ref = _sharedAdminClientRef;
    if (ref == null) {
      synchronized (this) {
        ref = _sharedAdminClientRef;
        if (ref == null) {
          ref = KafkaAdminClientManager.getInstance().getOrCreateAdminClient(_consumerProp);
          _sharedAdminClientRef = ref;
        }
      }
    }
    try {
      return ref.getAdminClient();
    } catch (IllegalStateException e) {
      if (isRetry) {
        throw new RuntimeException("Failed to create admin client after retry", e);
      }
      // Reference was closed, retry once
      synchronized (this) {
        _sharedAdminClientRef = null;
      }
      return getOrCreateSharedAdminClientInternal(true);
    }
  }

  private static <T> T retry(Supplier<T> s, int nRetries) {
    // Creation of the KafkaConsumer can fail for multiple reasons including DNS issues.
    // We arbitrarily chose 5 retries with 2 seconds sleep in between retries. 10 seconds total felt
    // like a good balance of not waiting too long for a retry, but also not retrying too many times.
    int tries = 0;
    while (true) {
      try {
        return s.get();
      } catch (KafkaException e) {
        tries++;
        if (tries >= nRetries) {
          LOGGER.error("Caught exception while creating Kafka consumer, giving up", e);
          throw e;
        }
        LOGGER.warn("Caught exception while creating Kafka consumer, retrying {}/{}", tries, nRetries, e);
        // We are choosing to sleepUniterruptibly here because other parts of the Kafka consumer code do this
        // as well. We don't want random interrupts to cause us to fail to create the consumer and have the table
        // stuck in ERROR state.
        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
      }
    }
  }

  public void close()
      throws IOException {
    _consumer.close();
    if (_sharedAdminClientRef != null) {
      _sharedAdminClientRef.close();
      _sharedAdminClientRef = null;
    }
  }

  @VisibleForTesting
  protected KafkaPartitionLevelStreamConfig getKafkaPartitionLevelStreamConfig() {
    return _config;
  }
}
