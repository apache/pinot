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
package org.apache.pinot.spi.config.instance;

import java.util.Map;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.ReadMode;


public interface InstanceDataManagerConfig {

  PinotConfiguration getConfig();

  String getInstanceId();

  String getInstanceDataDir();

  String getConsumerDir();

  String getInstanceSegmentTarDir();

  String getTableDataManagerProviderClass();

  String getSegmentStoreUri();

  String getConsumerClientIdSuffix();

  ReadMode getReadMode();

  String getSegmentFormatVersion();

  String getAvgMultiValueCount();

  boolean isRealtimeOffHeapAllocation();

  boolean isDirectRealtimeOffHeapAllocation();

  boolean shouldReloadConsumingSegment();

  int getMaxParallelRefreshThreads();

  boolean isAsyncSegmentRefreshEnabled();

  int getMaxSegmentPreloadThreads();

  int getMaxParallelSegmentBuilds();

  int getMaxParallelSegmentDownloads();

  String getSegmentDirectoryLoader();

  long getErrorCacheSize();

  boolean isStreamSegmentDownloadUntar();

  long getStreamSegmentDownloadUntarRateLimit();

  int getDeletedTablesCacheTtlMinutes();

  int getDeletedSegmentsCacheSize();

  int getDeletedSegmentsCacheTtlMinutes();

  String getSegmentPeerDownloadScheme();

  PinotConfiguration getUpsertConfig();

  PinotConfiguration getDedupConfig();

  PinotConfiguration getAuthConfig();

  Map<String, Map<String, String>> getTierConfigs();

  boolean isUploadSegmentToDeepStore();

  boolean shouldCheckCRCOnSegmentLoad();

  boolean isDimensionTablePreloadDisabled();

  /// Whether immutable segments create the index container and data source of a physical column on its first access
  /// instead of for every column at load. Off by default: it trades per-column heap on wide segments for reporting an
  /// unreadable index on first access rather than at load (see the segment loader for the full trade-off).
  default boolean isLazyColumnMaterialization() {
    return false;
  }
}
