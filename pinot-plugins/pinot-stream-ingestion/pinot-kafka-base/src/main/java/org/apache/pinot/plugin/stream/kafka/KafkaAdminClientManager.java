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

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A singleton manager that provides shared Kafka admin clients across multiple consumers/producers
 * connecting to the same Kafka cluster. This reduces connection overhead and improves resource efficiency.
 *
 * <p>Thread Safety: AdminClient instances are thread-safe and can be used concurrently by multiple threads.
 * The manager itself uses thread-safe data structures and reference counting to ensure safe sharing
 * of admin client instances across different connection handlers.
 */
public class KafkaAdminClientManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAdminClientManager.class);

  private static final KafkaAdminClientManager INSTANCE = new KafkaAdminClientManager();

  // Map from bootstrap servers to admin client wrapper
  private final ConcurrentHashMap<String, AdminClientWrapper> _adminClients = new ConcurrentHashMap<>();

  private KafkaAdminClientManager() {
    // Singleton
  }

  public static KafkaAdminClientManager getInstance() {
    return INSTANCE;
  }

  /**
   * Gets or creates a shared admin client for the given configuration.
   * The admin client is reference-counted and will be automatically cleaned up
   * when no longer in use.
   *
   * @param properties Kafka client properties containing bootstrap servers and other configs
   * @return AdminClientReference that should be closed when no longer needed
   */
  public AdminClientReference getOrCreateAdminClient(Properties properties) {
    String bootstrapServers = properties.getProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG);
    if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
      throw new IllegalArgumentException("Bootstrap servers must be specified");
    }

    // Create a cache key that includes essential connection properties
    String cacheKey = createCacheKey(properties);

    AdminClientWrapper wrapper = _adminClients.computeIfAbsent(cacheKey, k -> {
      LOGGER.info("Creating new shared admin client for bootstrap servers: {}", bootstrapServers);
      try {
        AdminClient adminClient = AdminClient.create(properties);
        return new AdminClientWrapper(adminClient, cacheKey);
      } catch (KafkaException e) {
        LOGGER.error("Failed to create admin client for bootstrap servers: {}", bootstrapServers, e);
        throw e;
      }
    });

    return wrapper.addReference();
  }

  /**
   * Creates a cache key from the properties that are relevant for admin client sharing.
   * Admin clients can be shared if they have the same bootstrap servers and security configuration.
   */
  private String createCacheKey(Properties properties) {
    StringBuilder keyBuilder = new StringBuilder();

    // Bootstrap servers are the primary key
    keyBuilder.append("bootstrap.servers=")
        .append(properties.getProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, ""));

    // Include security-related properties that affect connection
    appendIfPresent(keyBuilder, properties, AdminClientConfig.SECURITY_PROTOCOL_CONFIG);
    appendIfPresent(keyBuilder, properties, "sasl.mechanism");
    appendIfPresent(keyBuilder, properties, "sasl.jaas.config");
    appendIfPresent(keyBuilder, properties, "ssl.keystore.location");
    appendIfPresent(keyBuilder, properties, "ssl.truststore.location");

    return keyBuilder.toString();
  }

  private void appendIfPresent(StringBuilder builder, Properties properties, String key) {
    String value = properties.getProperty(key);
    if (value != null) {
      builder.append(";").append(key).append("=").append(value);
    }
  }

  // No standalone release method; cleanup is handled within AdminClientWrapper.removeReference()

  /**
   * Wrapper class that holds an admin client and its reference count
   */
  private class AdminClientWrapper {
    private final AdminClient _adminClient;
    private final String _cacheKey;
    private final AtomicInteger _referenceCount = new AtomicInteger(0);

    AdminClientWrapper(AdminClient adminClient, String cacheKey) {
      _adminClient = adminClient;
      _cacheKey = cacheKey;
    }

    AdminClient getAdminClient() {
      return _adminClient;
    }

    AdminClientReference addReference() {
      _referenceCount.incrementAndGet();
      return new AdminClientReference(this);
    }

    int removeReference() {
      int newCount = _referenceCount.decrementAndGet();
      if (newCount == 0) {
        // Attempt to remove and close exactly once
        if (_adminClients.remove(_cacheKey, this)) {
          try {
            _adminClient.close();
            LOGGER.info("Closed shared admin client for cache key: {}", _cacheKey);
          } catch (Exception e) {
            LOGGER.warn("Error closing admin client for cache key: {}", _cacheKey, e);
          }
        }
      }
      return newCount;
    }
  }

  /**
   * A reference to a shared admin client that automatically releases the reference when closed.
   */
  public class AdminClientReference implements AutoCloseable {
    private final AdminClientWrapper _wrapper;
    private volatile boolean _closed = false;

    AdminClientReference(AdminClientWrapper wrapper) {
      _wrapper = wrapper;
    }

    public AdminClient getAdminClient() {
      if (_closed) {
        throw new IllegalStateException("AdminClientReference has been closed");
      }
      return _wrapper.getAdminClient();
    }

    @Override
    public void close() {
      if (!_closed) {
        _closed = true;
        _wrapper.removeReference();
      }
    }
  }
}
