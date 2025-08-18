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
package org.apache.pinot.spi.stream;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Cache manager for StreamMetadataProvider instances to avoid creating too many connections
 * when unique client IDs are used. This class manages the lifecycle of StreamMetadataProvider
 * instances and provides functionality to recreate them when failures occur.
 */
public class StreamMetadataProviderCacheManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamMetadataProviderCacheManager.class);

  // Cache expiry time - providers will be evicted after this time of inactivity
  private static final int CACHE_EXPIRY_MINUTES = 30;
  // Maximum cache size to prevent memory leaks
  private static final int MAX_CACHE_SIZE = 1000;

  private final LoadingCache<CacheKey, StreamMetadataProvider> _streamProviderCache;
  private final LoadingCache<PartitionCacheKey, StreamMetadataProvider> _partitionProviderCache;

  public StreamMetadataProviderCacheManager() {
    // Cache for stream-level metadata providers
    _streamProviderCache = CacheBuilder.newBuilder()
        .maximumSize(MAX_CACHE_SIZE)
        .expireAfterAccess(CACHE_EXPIRY_MINUTES, TimeUnit.MINUTES)
        .removalListener((RemovalListener<CacheKey, StreamMetadataProvider>) notification -> {
          try {
            if (notification.getValue() != null) {
              LOGGER.debug("Closing stream metadata provider for key: {}", notification.getKey());
              notification.getValue().close();
            }
          } catch (IOException e) {
            LOGGER.warn("Error closing stream metadata provider for key: {}", notification.getKey(), e);
          }
        })
        .build(new CacheLoader<CacheKey, StreamMetadataProvider>() {
          @Override
          public StreamMetadataProvider load(CacheKey key) {
            return key._factory.createStreamMetadataProvider(key._clientId);
          }
        });

    // Cache for partition-level metadata providers
    _partitionProviderCache = CacheBuilder.newBuilder()
        .maximumSize(MAX_CACHE_SIZE)
        .expireAfterAccess(CACHE_EXPIRY_MINUTES, TimeUnit.MINUTES)
        .removalListener((RemovalListener<PartitionCacheKey, StreamMetadataProvider>) notification -> {
          try {
            if (notification.getValue() != null) {
              LOGGER.debug("Closing partition metadata provider for key: {}", notification.getKey());
              notification.getValue().close();
            }
          } catch (IOException e) {
            LOGGER.warn("Error closing partition metadata provider for key: {}", notification.getKey(), e);
          }
        })
        .build(new CacheLoader<PartitionCacheKey, StreamMetadataProvider>() {
          @Override
          public StreamMetadataProvider load(PartitionCacheKey key) {
            return key._factory.createPartitionMetadataProvider(key._clientId, key._partition);
          }
        });
  }

  /**
   * Get a cached stream metadata provider, creating one if it doesn't exist
   */
  public StreamMetadataProvider getOrCreateStreamMetadataProvider(StreamConsumerFactory factory,
      String clientId, StreamConfig streamConfig) {
    try {
      CacheKey key = new CacheKey(factory, clientId, streamConfig);
      return _streamProviderCache.get(key);
    } catch (ExecutionException e) {
      LOGGER.error("Error creating stream metadata provider for clientId: {}", clientId, e);
      throw new RuntimeException("Failed to create stream metadata provider", e.getCause());
    }
  }

  /**
   * Get a cached partition metadata provider, creating one if it doesn't exist
   */
  public StreamMetadataProvider getOrCreatePartitionMetadataProvider(StreamConsumerFactory factory,
      String clientId, StreamConfig streamConfig, int partition) {
    try {
      PartitionCacheKey key = new PartitionCacheKey(factory, clientId, streamConfig, partition);
      return _partitionProviderCache.get(key);
    } catch (ExecutionException e) {
      LOGGER.error("Error creating partition metadata provider for clientId: {}, partition: {}",
          clientId, partition, e);
      throw new RuntimeException("Failed to create partition metadata provider", e.getCause());
    }
  }

  /**
   * Invalidate and recreate a stream metadata provider when a failure occurs.
   * This method ensures the old provider is properly closed before creating a new one.
   * Synchronized to prevent race conditions during recreation.
   */
  public synchronized StreamMetadataProvider recreateStreamMetadataProvider(StreamConsumerFactory factory,
      String clientId, StreamConfig streamConfig) {
    CacheKey key = new CacheKey(factory, clientId, streamConfig);

    // Get the old provider before invalidating to ensure proper cleanup
    StreamMetadataProvider oldProvider = _streamProviderCache.getIfPresent(key);

    // Invalidate the cache entry
    _streamProviderCache.invalidate(key);

    // Explicitly close the old provider if it exists
    if (oldProvider != null) {
      try {
        LOGGER.info("Closing old stream metadata provider for key: {}", key);
        oldProvider.close();
      } catch (Exception e) {
        LOGGER.warn("Error closing old stream metadata provider for key: {}", key, e);
      }
    }

    // Create and return new provider
    return getOrCreateStreamMetadataProvider(factory, clientId, streamConfig);
  }

  /**
   * Invalidate and recreate a partition metadata provider when a failure occurs.
   * This method ensures the old provider is properly closed before creating a new one.
   * Synchronized to prevent race conditions during recreation.
   */
  public synchronized StreamMetadataProvider recreatePartitionMetadataProvider(StreamConsumerFactory factory,
      String clientId, StreamConfig streamConfig, int partition) {
    PartitionCacheKey key = new PartitionCacheKey(factory, clientId, streamConfig, partition);

    // Get the old provider before invalidating to ensure proper cleanup
    StreamMetadataProvider oldProvider = _partitionProviderCache.getIfPresent(key);

    // Invalidate the cache entry
    _partitionProviderCache.invalidate(key);

    // Explicitly close the old provider if it exists
    if (oldProvider != null) {
      try {
        LOGGER.info("Closing old partition metadata provider for key: {}", key);
        oldProvider.close();
      } catch (Exception e) {
        LOGGER.warn("Error closing old partition metadata provider for key: {}", key, e);
      }
    }

    // Create and return new provider
    return getOrCreatePartitionMetadataProvider(factory, clientId, streamConfig, partition);
  }

  /**
   * Clear all cached providers (used for cleanup).
   * This method ensures all providers are properly closed before clearing the cache.
   */
  public synchronized void clearAll() {
    LOGGER.info("Clearing all cached stream metadata providers");

    // Close all stream providers before clearing
    for (StreamMetadataProvider provider : _streamProviderCache.asMap().values()) {
      try {
        provider.close();
      } catch (Exception e) {
        LOGGER.warn("Error closing stream metadata provider during clearAll", e);
      }
    }

    // Close all partition providers before clearing
    for (StreamMetadataProvider provider : _partitionProviderCache.asMap().values()) {
      try {
        provider.close();
      } catch (Exception e) {
        LOGGER.warn("Error closing partition metadata provider during clearAll", e);
      }
    }

    // Clear the caches
    _streamProviderCache.invalidateAll();
    _partitionProviderCache.invalidateAll();

    LOGGER.info("Completed clearing all cached stream metadata providers");
  }

  /**
   * Shutdown the cache manager and clean up all resources.
   * This should be called when the application shuts down.
   */
  public synchronized void shutdown() {
    LOGGER.info("Shutting down StreamMetadataProviderCacheManager");
    clearAll();

    // Additional cleanup can be added here if needed
    LOGGER.info("StreamMetadataProviderCacheManager shutdown complete");
  }

  /**
   * Get cache statistics for monitoring
   */
  public String getCacheStats() {
    return String.format("StreamProviderCache: %s, PartitionProviderCache: %s",
        _streamProviderCache.stats(), _partitionProviderCache.stats());
  }

  /**
   * Cache key for stream-level metadata providers
   */
  private static class CacheKey {
    final StreamConsumerFactory _factory;
    final String _clientId;
    final String _tableName;
    final String _topicName;

    CacheKey(StreamConsumerFactory factory, String clientId, StreamConfig streamConfig) {
      _factory = factory;
      _clientId = clientId;
      _tableName = streamConfig.getTableNameWithType();
      _topicName = streamConfig.getTopicName();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      CacheKey cacheKey = (CacheKey) o;
      return Objects.equals(_factory.getClass(), cacheKey._factory.getClass())
          && Objects.equals(_clientId, cacheKey._clientId)
          && Objects.equals(_tableName, cacheKey._tableName)
          && Objects.equals(_topicName, cacheKey._topicName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(_factory.getClass(), _clientId, _tableName, _topicName);
    }

    @Override
    public String toString() {
      return String.format("CacheKey{factory=%s, clientId=%s, table=%s, topic=%s}",
          _factory.getClass().getSimpleName(), _clientId, _tableName, _topicName);
    }
  }

  /**
   * Cache key for partition-level metadata providers
   */
  private static class PartitionCacheKey {
    final StreamConsumerFactory _factory;
    final String _clientId;
    final String _tableName;
    final String _topicName;
    final int _partition;

    PartitionCacheKey(StreamConsumerFactory factory, String clientId, StreamConfig streamConfig, int partition) {
      _factory = factory;
      _clientId = clientId;
      _tableName = streamConfig.getTableNameWithType();
      _topicName = streamConfig.getTopicName();
      _partition = partition;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PartitionCacheKey that = (PartitionCacheKey) o;
      return _partition == that._partition
          && Objects.equals(_factory.getClass(), that._factory.getClass())
          && Objects.equals(_clientId, that._clientId)
          && Objects.equals(_tableName, that._tableName)
          && Objects.equals(_topicName, that._topicName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(_factory.getClass(), _clientId, _tableName, _topicName, _partition);
    }

    @Override
    public String toString() {
      return String.format("PartitionCacheKey{factory=%s, clientId=%s, table=%s, topic=%s, partition=%d}",
          _factory.getClass().getSimpleName(), _clientId, _tableName, _topicName, _partition);
    }
  }

  // Singleton instance
  private static volatile StreamMetadataProviderCacheManager _instance;

  public static StreamMetadataProviderCacheManager getInstance() {
    if (_instance == null) {
      synchronized (StreamMetadataProviderCacheManager.class) {
        if (_instance == null) {
          _instance = new StreamMetadataProviderCacheManager();

          // Add shutdown hook to ensure proper cleanup
          Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
              _instance.shutdown();
            } catch (Exception e) {
              // Use System.err since logger might not be available during shutdown
              System.err.println("Error during StreamMetadataProviderCacheManager shutdown: " + e.getMessage());
            }
          }, "StreamMetadataProviderCacheManager-ShutdownHook"));
        }
      }
    }
    return _instance;
  }
}
