/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.data.manager.config;

import com.linkedin.pinot.common.segment.ReadMode;
import org.apache.commons.configuration.Configuration;


public interface InstanceDataManagerConfig {
  Configuration getConfig();

  String getInstanceId();

  String getInstanceDataDir();

  String getConsumerDir();

  String getInstanceSegmentTarDir();

  String getInstanceBootstrapSegmentDir();

  ReadMode getReadMode();

  String getSegmentFormatVersion();

  String getAvgMultiValueCount();

  boolean isEnableDefaultColumns();

  boolean isEnableSplitCommit();

  boolean isRealtimeOffHeapAllocation();

  boolean isDirectRealtimeOffheapAllocation();

  int getMaxParallelSegmentBuilds();
}
