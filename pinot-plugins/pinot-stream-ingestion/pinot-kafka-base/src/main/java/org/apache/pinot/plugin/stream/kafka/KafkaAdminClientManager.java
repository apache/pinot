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

  /**
   * Releases an admin client reference. When the reference count reaches zero,
   * the admin client is closed and removed from the cache.
   */
  private void releaseAdminClient(String cacheKey) {
    AdminClientWrapper wrapper = _adminClients.get(cacheKey);
    if (wrapper != null && wrapper.removeReference() == 0) {
      // Use remove with condition to handle concurrent access
      if (_adminClients.remove(cacheKey, wrapper)) {
        try {
          wrapper.getAdminClient().close();
          LOGGER.info("Closed shared admin client for cache key: {}", cacheKey);
        } catch (Exception e) {
          LOGGER.warn("Error closing admin client for cache key: {}", cacheKey, e);
        }
      }
    }
  }

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
      return new AdminClientReference(_adminClient, _cacheKey);
    }

    int removeReference() {
      return _referenceCount.decrementAndGet();
    }
  }

  /**
   * A reference to a shared admin client that automatically releases the reference when closed.
   */
  public class AdminClientReference implements AutoCloseable {
    private final AdminClient _adminClient;
    private final String _cacheKey;
    private volatile boolean _closed = false;

    AdminClientReference(AdminClient adminClient, String cacheKey) {
      _adminClient = adminClient;
      _cacheKey = cacheKey;
    }

    public AdminClient getAdminClient() {
      if (_closed) {
        throw new IllegalStateException("AdminClientReference has been closed");
      }
      return _adminClient;
    }

    @Override
    public void close() {
      if (!_closed) {
        _closed = true;
        releaseAdminClient(_cacheKey);
      }
    }
  }
}
