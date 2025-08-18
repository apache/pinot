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

import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;


public class StreamMetadataProviderCacheManagerTest {

  @Test
  public void testCachedStreamMetadataProviderReuse() {
    StreamMetadataProviderCacheManager cacheManager = new StreamMetadataProviderCacheManager();
    TestStreamConsumerFactory factory = new TestStreamConsumerFactory();
    StreamConfig streamConfig = createTestStreamConfig();

    // First call should create a new provider
    StreamMetadataProvider provider1 = cacheManager.getOrCreateStreamMetadataProvider(
        factory, "test-client", streamConfig);
    assertNotNull(provider1);

    // Second call with same parameters should return the same cached provider
    StreamMetadataProvider provider2 = cacheManager.getOrCreateStreamMetadataProvider(
        factory, "test-client", streamConfig);
    assertSame(provider1, provider2, "Should return the same cached provider");

    // Verify factory was called only once
    verify(factory, times(1)).createStreamMetadataProvider(anyString());
  }

  @Test
  public void testCachedPartitionMetadataProviderReuse() {
    StreamMetadataProviderCacheManager cacheManager = new StreamMetadataProviderCacheManager();
    TestStreamConsumerFactory factory = new TestStreamConsumerFactory();
    StreamConfig streamConfig = createTestStreamConfig();

    // First call should create a new provider
    StreamMetadataProvider provider1 = cacheManager.getOrCreatePartitionMetadataProvider(
        factory, "test-client", streamConfig, 0);
    assertNotNull(provider1);

    // Second call with same parameters should return the same cached provider
    StreamMetadataProvider provider2 = cacheManager.getOrCreatePartitionMetadataProvider(
        factory, "test-client", streamConfig, 0);
    assertSame(provider1, provider2, "Should return the same cached provider");

    // Verify factory was called only once
    verify(factory, times(1)).createPartitionMetadataProvider(anyString(), anyInt());
  }

  @Test
  public void testRecreateInvalidatesCachedProvider() {
    StreamMetadataProviderCacheManager cacheManager = new StreamMetadataProviderCacheManager();
    TestStreamConsumerFactory factory = new TestStreamConsumerFactory();
    StreamConfig streamConfig = createTestStreamConfig();

    // First call creates a cached provider
    StreamMetadataProvider provider1 = cacheManager.getOrCreateStreamMetadataProvider(
        factory, "test-client", streamConfig);

    // Recreate should invalidate cache and create a new provider
    StreamMetadataProvider provider2 = cacheManager.recreateStreamMetadataProvider(
        factory, "test-client", streamConfig);

    // Should be different instances
    assertNotNull(provider1);
    assertNotNull(provider2);
    // Note: Due to mocking, they might be the same mock instance, but factory should be called twice
    verify(factory, times(2)).createStreamMetadataProvider(anyString());
  }

  @Test
  public void testDifferentClientIdsCachedseparately() {
    StreamMetadataProviderCacheManager cacheManager = new StreamMetadataProviderCacheManager();
    TestStreamConsumerFactory factory = new TestStreamConsumerFactory();
    StreamConfig streamConfig = createTestStreamConfig();

    // Different client IDs should create separate cache entries
    StreamMetadataProvider provider1 = cacheManager.getOrCreateStreamMetadataProvider(
        factory, "test-client-1", streamConfig);
    StreamMetadataProvider provider2 = cacheManager.getOrCreateStreamMetadataProvider(
        factory, "test-client-2", streamConfig);

    assertNotNull(provider1);
    assertNotNull(provider2);

    // Factory should be called twice for different client IDs
    verify(factory, times(2)).createStreamMetadataProvider(anyString());
  }

  @Test
  public void testRecreateProperlyClosesOldProvider() throws Exception {
    StreamMetadataProviderCacheManager cacheManager = new StreamMetadataProviderCacheManager();
    TestStreamConsumerFactory factory = new TestStreamConsumerFactory();
    StreamConfig streamConfig = createTestStreamConfig();

    // Create initial provider
    StreamMetadataProvider provider1 = cacheManager.getOrCreateStreamMetadataProvider(
        factory, "test-client", streamConfig);
    assertNotNull(provider1);

    // Recreate should close the old provider and create a new one
    StreamMetadataProvider provider2 = cacheManager.recreateStreamMetadataProvider(
        factory, "test-client", streamConfig);
    assertNotNull(provider2);

    // Verify old provider was closed
    verify(provider1, times(1)).close();

    // Factory should be called twice (initial + recreate)
    verify(factory, times(2)).createStreamMetadataProvider(anyString());
  }

  @Test
  public void testClearAllProperlyClosesProviders() throws Exception {
    StreamMetadataProviderCacheManager cacheManager = new StreamMetadataProviderCacheManager();
    TestStreamConsumerFactory factory = new TestStreamConsumerFactory();
    StreamConfig streamConfig = createTestStreamConfig();

    // Create multiple providers
    StreamMetadataProvider streamProvider = cacheManager.getOrCreateStreamMetadataProvider(
        factory, "test-client", streamConfig);
    StreamMetadataProvider partitionProvider = cacheManager.getOrCreatePartitionMetadataProvider(
        factory, "test-client", streamConfig, 0);

    assertNotNull(streamProvider);
    assertNotNull(partitionProvider);

    // Clear all should close both providers
    cacheManager.clearAll();

    // Verify both providers were closed
    verify(streamProvider, times(1)).close();
    verify(partitionProvider, times(1)).close();
  }

  private StreamConfig createTestStreamConfig() {
    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getTableNameWithType()).thenReturn("testTable_REALTIME");
    when(streamConfig.getTopicName()).thenReturn("testTopic");
    return streamConfig;
  }

  // Test implementation of StreamConsumerFactory
  private static class TestStreamConsumerFactory extends StreamConsumerFactory {
    private StreamMetadataProvider _mockStreamProvider = mock(StreamMetadataProvider.class);
    private StreamMetadataProvider _mockPartitionProvider = mock(StreamMetadataProvider.class);

    @Override
    public StreamMetadataProvider createPartitionMetadataProvider(String clientId, int partition) {
      return _mockPartitionProvider;
    }

    @Override
    public StreamMetadataProvider createStreamMetadataProvider(String clientId) {
      return _mockStreamProvider;
    }
  }
}
