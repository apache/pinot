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

import com.google.common.annotations.VisibleForTesting;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.ReadMode;


@VisibleForTesting
public class DummyInstanceDataManagerConfig implements InstanceDataManagerConfig {

  @Override
  public PinotConfiguration getConfig() {
    return new PinotConfiguration();
  }

  @Override
  public String getInstanceId() {
    return "dummyInstance";
  }

  @Override
  public String getInstanceDataDir() {
    return CommonConstants.Server.DEFAULT_INSTANCE_DATA_DIR;
  }

  @Override
  public String getConsumerDir() {
    return null;
  }

  @Override
  public String getInstanceSegmentTarDir() {
    return CommonConstants.Server.DEFAULT_INSTANCE_SEGMENT_TAR_DIR;
  }

  @Override
  public String getInstanceBootstrapSegmentDir() {
    return null;
  }

  @Override
  public String getSegmentStoreUri() {
    return null;
  }

  @Override
  public ReadMode getReadMode() {
    return ReadMode.DEFAULT_MODE;
  }

  @Override
  public String getSegmentFormatVersion() {
    return null;
  }

  @Override
  public String getAvgMultiValueCount() {
    return null;
  }

  @Override
  public boolean isEnableSplitCommit() {
    return false;
  }

  @Override
  public boolean isEnableSplitCommitEndWithMetadata() {
    return false;
  }

  @Override
  public boolean isRealtimeOffHeapAllocation() {
    return false;
  }

  @Override
  public boolean isDirectRealtimeOffHeapAllocation() {
    return false;
  }

  @Override
  public int getMaxParallelSegmentBuilds() {
    return 0;
  }

  @Override
  public int getMaxParallelSegmentDownloads() {
    return 0;
  }

  @Override
  public String getSegmentDirectoryLoader() {
    return null;
  }

  @Override
  public long getErrorCacheSize() {
    return 0;
  }

  @Override
  public boolean isStreamSegmentDownloadUntar() {
    return false;
  }

  @Override
  public long getStreamSegmentDownloadUntarRateLimit() {
    return 0;
  }

  @Override
  public int getDeletedSegmentsCacheSize() {
    return 0;
  }

  @Override
  public int getDeletedSegmentsCacheTtlMinutes() {
    return 0;
  }

  @Override
  public String getSegmentPeerDownloadScheme() {
    return null;
  }
}
