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
package com.linkedin.pinot.common.data;

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadataLoader;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.configuration.Configuration;


public interface DataManager {
  void init(Configuration dataManagerConfig);

  void start();

  /**
   * Adds a segment from local disk into the OFFLINE table.
   */
  void addSegment(@Nonnull SegmentMetadata segmentMetadata, @Nullable TableConfig tableConfig, @Nullable Schema schema)
      throws Exception;

  void removeSegment(String segmentName);

  void reloadSegment(@Nonnull String tableNameWithType, @Nonnull SegmentMetadata segmentMetadata,
      @Nullable TableConfig tableConfig, @Nullable Schema schema)
      throws Exception;

  void shutDown();

  String getSegmentDataDirectory();

  String getSegmentFileDirectory();

  SegmentMetadataLoader getSegmentMetadataLoader();

  @Nonnull
  List<SegmentMetadata> getAllSegmentsMetadata(@Nonnull String tableNameWithType);

  @Nullable
  SegmentMetadata getSegmentMetadata(@Nonnull String tableNameWithType, @Nonnull String segmentName);

  boolean isStarted();
}
