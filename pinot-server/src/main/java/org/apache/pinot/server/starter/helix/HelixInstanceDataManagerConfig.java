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
package org.apache.pinot.server.starter.helix;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.spi.utils.ReadMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.Server.CONFIG_OF_SEGMENT_STORE_URI;


/**
 * The config used for HelixInstanceDataManager.
 *
 *
 */
public class HelixInstanceDataManagerConfig implements InstanceDataManagerConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(HelixInstanceDataManagerConfig.class);

  // Average number of values in multi-valued columns in any table in this instance.
  // This value is used to allocate initial memory for multi-valued columns in realtime segments in consuming state.
  private static final String AVERAGE_MV_COUNT = "realtime.averageMultiValueEntriesPerRow";
  // Key of instance id
  public static final String INSTANCE_ID = "id";
  // Key of instance data directory
  public static final String INSTANCE_DATA_DIR = "dataDir";
  // Key of consumer directory
  public static final String CONSUMER_DIR = "consumerDir";
  // Key of instance segment tar directory
  public static final String INSTANCE_SEGMENT_TAR_DIR = "segmentTarDir";
  // Key of segment directory
  public static final String INSTANCE_BOOTSTRAP_SEGMENT_DIR = "bootstrap.segment.dir";
  // Key of table data directory
  public static final String kEY_OF_TABLE_DATA_DIRECTORY = "directory";
  // Key of table data directory
  public static final String kEY_OF_TABLE_NAME = "name";
  // Key of instance level segment read mode
  public static final String READ_MODE = "readMode";
  // Key of the segment format this server can read
  public static final String SEGMENT_FORMAT_VERSION = "segment.format.version";
  // Key of whether to enable reloading consuming segments
  public static final String INSTANCE_RELOAD_CONSUMING_SEGMENT = "reload.consumingSegment";
  // Key of the auth token
  public static final String AUTH_TOKEN = "auth.token";
  // Tier properties
  public static final String TIER_BACKEND = "tier.backend";
  public static final String DEFAULT_TIER_BACKEND = "local";
  // Prefix for tier config
  public static final String TIER_CONFIGS_PREFIX = "tier";

  // Key of how many parallel realtime segments can be built.
  // A value of <= 0 indicates unlimited.
  // Unlimited parallel builds can cause high GC pauses during segment builds, causing
  // response times to suffer.
  private static final String MAX_PARALLEL_SEGMENT_BUILDS = "realtime.max.parallel.segment.builds";
  private static final int DEFAULT_MAX_PARALLEL_SEGMENT_BUILDS = 4;

  // Key of whether to enable split commit
  private static final String ENABLE_SPLIT_COMMIT = "enable.split.commit";
  // Key of whether to enable split commit end with segment metadata files.
  private static final String ENABLE_SPLIT_COMMIT_END_WITH_METADATA = "enable.commitend.metadata";

  // Whether memory for realtime consuming segments should be allocated off-heap.
  private static final String REALTIME_OFFHEAP_ALLOCATION = "realtime.alloc.offheap";
  // And whether the allocation should be direct (default is to allocate via mmap)
  // Direct memory allocation may mean setting heap size appropriately when starting JVM.
  // The metric ServerGauge.REALTIME_OFFHEAP_MEMORY_USED should indicate how much memory is needed.
  private static final String DIRECT_REALTIME_OFFHEAP_ALLOCATION = "realtime.alloc.offheap.direct";

  // Number of simultaneous segments that can be refreshed on one server.
  // Segment refresh works by loading the old as well as new versions of segments in memory, assigning
  // new incoming queries to use the new version. The old version is dropped when all the queries that
  // use the old version have completed. A server-wide semaphore is acquired before refreshing a segment so
  // that we exceed the memory in some limited fashion. If there are multiple
  // refresh requests, then they are queued on the semaphore (FIFO).
  // In some multi-tenant use cases, it may be fine to over-allocate memory.
  // Setting this config variable to a value greater than 1 will cause as many refresh threads to run simultaneously.
  //
  // NOTE: While segment load can be faster, multiple threads will be taken up loading segments, so
  //       it is possible that the query latencies increase during that period.
  //
  private static final String MAX_PARALLEL_REFRESH_THREADS = "max.parallel.refresh.threads";

  // Size of cache that holds errors.
  private static final String ERROR_CACHE_SIZE = "error.cache.size";

  private final static String[] REQUIRED_KEYS = {INSTANCE_ID, INSTANCE_DATA_DIR, READ_MODE};
  private static final long DEFAULT_ERROR_CACHE_SIZE = 100L;
  private PinotConfiguration _instanceDataManagerConfiguration = null;

  public HelixInstanceDataManagerConfig(PinotConfiguration serverConfig)
      throws ConfigurationException {
    _instanceDataManagerConfiguration = serverConfig;

    for (String key : serverConfig.getKeys()) {
      LOGGER.info("InstanceDataManagerConfig, key: {} , value: {}", key, serverConfig.getProperty(key));
    }

    checkRequiredKeys();
  }

  private void checkRequiredKeys()
      throws ConfigurationException {
    for (String keyString : REQUIRED_KEYS) {
      Optional.ofNullable(_instanceDataManagerConfiguration.getProperty(keyString))

          .orElseThrow(() -> new ConfigurationException("Cannot find required key : " + keyString));
    }
  }

  @Override
  public PinotConfiguration getConfig() {
    return _instanceDataManagerConfiguration;
  }

  @Override
  public String getInstanceId() {
    return _instanceDataManagerConfiguration.getProperty(INSTANCE_ID);
  }

  @Override
  public String getInstanceDataDir() {
    return _instanceDataManagerConfiguration.getProperty(INSTANCE_DATA_DIR);
  }

  @Override
  public String getConsumerDir() {
    return _instanceDataManagerConfiguration.getProperty(CONSUMER_DIR);
  }

  @Override
  public String getInstanceSegmentTarDir() {
    return _instanceDataManagerConfiguration.getProperty(INSTANCE_SEGMENT_TAR_DIR);
  }

  @Override
  public String getInstanceBootstrapSegmentDir() {
    return _instanceDataManagerConfiguration.getProperty(INSTANCE_BOOTSTRAP_SEGMENT_DIR);
  }

  @Override
  public String getSegmentStoreUri() {
    return _instanceDataManagerConfiguration.getProperty(CONFIG_OF_SEGMENT_STORE_URI);
  }

  @Override
  public ReadMode getReadMode() {
    return ReadMode.valueOf(_instanceDataManagerConfiguration.getProperty(READ_MODE));
  }

  @Override
  public String getSegmentFormatVersion() {
    return _instanceDataManagerConfiguration.getProperty(SEGMENT_FORMAT_VERSION);
  }

  @Override
  public boolean isEnableSplitCommit() {
    return _instanceDataManagerConfiguration.getProperty(ENABLE_SPLIT_COMMIT, false);
  }

  @Override
  public boolean isEnableSplitCommitEndWithMetadata() {
    return _instanceDataManagerConfiguration.getProperty(ENABLE_SPLIT_COMMIT_END_WITH_METADATA, true);
  }

  @Override
  public boolean isRealtimeOffHeapAllocation() {
    return _instanceDataManagerConfiguration.getProperty(REALTIME_OFFHEAP_ALLOCATION, false);
  }

  @Override
  public boolean isDirectRealtimeOffHeapAllocation() {
    return _instanceDataManagerConfiguration.getProperty(DIRECT_REALTIME_OFFHEAP_ALLOCATION, false);
  }

  public boolean shouldReloadConsumingSegment() {
    return _instanceDataManagerConfiguration
        .getProperty(INSTANCE_RELOAD_CONSUMING_SEGMENT, Server.DEFAULT_RELOAD_CONSUMING_SEGMENT);
  }

  @Override
  public String getAvgMultiValueCount() {
    return _instanceDataManagerConfiguration.getProperty(AVERAGE_MV_COUNT);
  }

  public int getMaxParallelRefreshThreads() {
    return _instanceDataManagerConfiguration.getProperty(MAX_PARALLEL_REFRESH_THREADS, 1);
  }

  public int getMaxParallelSegmentBuilds() {
    return _instanceDataManagerConfiguration
        .getProperty(MAX_PARALLEL_SEGMENT_BUILDS, DEFAULT_MAX_PARALLEL_SEGMENT_BUILDS);
  }

  @Override
  public String getAuthToken() {
    return _instanceDataManagerConfiguration.getProperty(AUTH_TOKEN);
  }

  @Override
  public String getTierBackend() {
    return _instanceDataManagerConfiguration.getProperty(TIER_BACKEND, DEFAULT_TIER_BACKEND);
  }

  @Override
  public PinotConfiguration getTierConfigs() {
    String tierBackend = getTierBackend();
    String tierConfigsPrefix = String.format("%s.%s", TIER_CONFIGS_PREFIX, tierBackend);
    Map<String, Object> tierConfigs =
        new HashMap<>(_instanceDataManagerConfiguration.subset(tierConfigsPrefix).toMap());
    if (!tierConfigs.containsKey(READ_MODE)) {
      tierConfigs.put(READ_MODE, getReadMode());
    }
    return new PinotConfiguration(tierConfigs);
  }

  @Override
  public long getErrorCacheSize() {
    return _instanceDataManagerConfiguration.getProperty(ERROR_CACHE_SIZE, DEFAULT_ERROR_CACHE_SIZE);
  }

  @Override
  public String toString() {
    String configString = "";
    configString += "Instance Id: " + getInstanceId();
    configString += "\n\tInstance Data Dir: " + getInstanceDataDir();
    configString += "\n\tInstance Segment Tar Dir: " + getInstanceSegmentTarDir();
    configString += "\n\tBootstrap Segment Dir: " + getInstanceBootstrapSegmentDir();
    configString += "\n\tRead Mode: " + getReadMode();
    configString += "\n\tSegment format version: " + getSegmentFormatVersion();
    return configString;
  }
}
