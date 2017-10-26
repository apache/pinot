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
package com.linkedin.pinot.controller.helix.core.minion.generator;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.config.PinotTaskConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableTaskConfig;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.controller.helix.core.minion.ClusterInfoProvider;
import com.linkedin.pinot.core.common.MinionConstants;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConvertToRawIndexTaskGenerator extends BaseSegmentMutatingPinotTaskGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConvertToRawIndexTaskGenerator.class);
  ClusterInfoProvider _clusterInfoProvider;

  public ConvertToRawIndexTaskGenerator(ClusterInfoProvider clusterInfoProvider) {
    super(clusterInfoProvider);
    _clusterInfoProvider = clusterInfoProvider;
  }

  @Nonnull
  @Override
  public String getTaskType() {
    return MinionConstants.ConvertToRawIndexTask.TASK_TYPE;
  }

  @Override
  public Map<String, String> getConfigsForTask(TableConfig tableConfig, OfflineSegmentZKMetadata offlineSegmentZKMetadata) {
    Map<String, String> configs = new HashMap<>();
    configs.put(MinionConstants.TABLE_NAME_KEY, tableConfig.getTableName());
    configs.put(MinionConstants.SEGMENT_NAME_KEY, offlineSegmentZKMetadata.getSegmentName());
    configs.put(MinionConstants.DOWNLOAD_URL_KEY, offlineSegmentZKMetadata.getDownloadUrl());
    configs.put(MinionConstants.UPLOAD_URL_KEY, _clusterInfoProvider.getVipUrl() + "/segments");
    return configs;
  }

  @Override
  public PinotTaskConfig getScheduledSegmentTaskConfig(TableConfig tableConfig,
      OfflineSegmentZKMetadata offlineSegmentZKMetadata) {
    Map<String, String> configs = getConfigsForTask(tableConfig, offlineSegmentZKMetadata);

    TableTaskConfig tableTaskConfig = tableConfig.getTaskConfig();
    Preconditions.checkNotNull(tableTaskConfig);
    Map<String, String> taskConfigs =
        tableTaskConfig.getConfigsForTaskType(getTaskType());

    // Only submit segments that have not been optimized
    String columnsToConvertConfig = taskConfigs.get(MinionConstants.ConvertToRawIndexTask.COLUMNS_TO_CONVERT_KEY);
    List<String> optimizations = offlineSegmentZKMetadata.getOptimizations();
    if (optimizations == null || !optimizations.contains(V1Constants.MetadataKeys.Optimization.RAW_INDEX)) {
      if (columnsToConvertConfig != null) {
        configs.put(MinionConstants.ConvertToRawIndexTask.COLUMNS_TO_CONVERT_KEY, columnsToConvertConfig);
      }
      return new PinotTaskConfig(MinionConstants.ConvertToRawIndexTask.COLUMNS_TO_CONVERT_KEY, configs);
    }
    return null;
  }
}
