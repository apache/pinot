/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.server.starter.helix;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.data.manager.config.InstanceDataManagerConfig;
import java.util.Iterator;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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

  // Key of whether to enable default columns
  private static final String ENABLE_DEFAULT_COLUMNS = "enable.default.columns";

  // Key of how many parallel realtime segments can be built.
  // A value of <= 0 indicates unlimited.
  // Unlimited parallel builds can cause high GC pauses during segment builds, causing
  // response times to suffer.
  private static final String MAX_PARALLEL_SEGMENT_BUILDS = "realtime.max.parallel.segment.builds";

  // Key of whether to enable split commit
  private static final String ENABLE_SPLIT_COMMIT = "enable.split.commit";

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

  private final static String[] REQUIRED_KEYS = { INSTANCE_ID, INSTANCE_DATA_DIR, READ_MODE };
  private Configuration _instanceDataManagerConfiguration = null;

  public HelixInstanceDataManagerConfig(Configuration serverConfig) throws ConfigurationException {
    _instanceDataManagerConfiguration = serverConfig;
    Iterator keysIterator = serverConfig.getKeys();
    while (keysIterator.hasNext()) {
      String key = (String) keysIterator.next();
      LOGGER.info("InstanceDataManagerConfig, key: {} , value: {}",  key,
          serverConfig.getProperty(key));
    }
    checkRequiredKeys();
  }

  private void checkRequiredKeys() throws ConfigurationException {
    for (String keyString : REQUIRED_KEYS) {
      if (!_instanceDataManagerConfiguration.containsKey(keyString)) {
        throw new ConfigurationException("Cannot find required key : " + keyString);
      }
    }
  }

  @Override
  public Configuration getConfig() {
    return _instanceDataManagerConfiguration;
  }

  @Override
  public String getInstanceId() {
    return _instanceDataManagerConfiguration.getString(INSTANCE_ID);
  }

  @Override
  public String getInstanceDataDir() {
    return _instanceDataManagerConfiguration.getString(INSTANCE_DATA_DIR);
  }

  @Override
  public String getConsumerDir() {
    return _instanceDataManagerConfiguration.getString(CONSUMER_DIR);
  }

  @Override
  public String getInstanceSegmentTarDir() {
    return _instanceDataManagerConfiguration.getString(INSTANCE_SEGMENT_TAR_DIR);
  }

  @Override
  public String getInstanceBootstrapSegmentDir() {
    return _instanceDataManagerConfiguration.getString(INSTANCE_BOOTSTRAP_SEGMENT_DIR);
  }

  @Override
  public ReadMode getReadMode() {
    return ReadMode.valueOf(_instanceDataManagerConfiguration.getString(READ_MODE));
  }

  @Override
  public String getSegmentFormatVersion() {
    return _instanceDataManagerConfiguration.getString(SEGMENT_FORMAT_VERSION);
  }

  @Override
  public boolean isEnableDefaultColumns() {
    return _instanceDataManagerConfiguration.getBoolean(ENABLE_DEFAULT_COLUMNS, false);
  }

  @Override
  public boolean isEnableSplitCommit() {
    return _instanceDataManagerConfiguration.getBoolean(ENABLE_SPLIT_COMMIT, false);
  }

  @Override
  public boolean isRealtimeOffHeapAllocation() {
    return _instanceDataManagerConfiguration.getBoolean(REALTIME_OFFHEAP_ALLOCATION, false);
  }

  @Override
  public boolean isDirectRealtimeOffheapAllocation() {
    return _instanceDataManagerConfiguration.getBoolean(DIRECT_REALTIME_OFFHEAP_ALLOCATION, false);
  }

  @Override
  public String getAvgMultiValueCount() {
    return _instanceDataManagerConfiguration.getString(AVERAGE_MV_COUNT, null);
  }

  public int getMaxParallelRefreshThreads() {
    return _instanceDataManagerConfiguration.getInt(MAX_PARALLEL_REFRESH_THREADS, 1);
  }

  public int getMaxParallelSegmentBuilds() {
    return _instanceDataManagerConfiguration.getInt(MAX_PARALLEL_SEGMENT_BUILDS, 0);
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
