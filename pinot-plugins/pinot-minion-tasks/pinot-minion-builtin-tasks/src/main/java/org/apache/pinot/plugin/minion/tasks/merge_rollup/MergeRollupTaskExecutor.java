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
package org.apache.pinot.plugin.minion.tasks.merge_rollup;

import com.google.common.base.Preconditions;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.core.minion.rollup.MergeRollupSegmentConverter;
import org.apache.pinot.core.minion.rollup.MergeType;
import org.apache.pinot.plugin.minion.tasks.BaseMultipleSegmentsConversionExecutor;
import org.apache.pinot.plugin.minion.tasks.SegmentConversionResult;
import org.apache.pinot.spi.config.table.TableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Task executor that provides merge and rollup service
 *
 * TODO:
 *   1. Add the support for roll-up
 *   2. Add the support for time split to provide backfill support for merged segments
 *   3. Change the way to decide the number of output segments (explicit numPartition config -> maxNumRowsPerSegment)
 */
public class MergeRollupTaskExecutor extends BaseMultipleSegmentsConversionExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(MergeRollupTaskExecutor.class);

  @Override
  protected List<SegmentConversionResult> convert(PinotTaskConfig pinotTaskConfig, List<File> originalIndexDirs,
      File workingDir)
      throws Exception {
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    String mergeTypeString = configs.get(MinionConstants.MergeRollupTask.MERGE_TYPE_KEY);
    // TODO: add the support for rollup
    Preconditions.checkNotNull(mergeTypeString, "MergeType cannot be null");

    MergeType mergeType = MergeType.fromString(mergeTypeString);
    Preconditions.checkState(mergeType == MergeType.CONCATENATE, "Only 'CONCATENATE' mode is currently supported.");

    String mergedSegmentName = configs.get(MinionConstants.MergeRollupTask.MERGED_SEGMENT_NAME_KEY);
    String tableNameWithType = configs.get(MinionConstants.TABLE_NAME_KEY);

    TableConfig tableConfig = getTableConfig(tableNameWithType);

    MergeRollupSegmentConverter rollupSegmentConverter =
        new MergeRollupSegmentConverter.Builder().setMergeType(mergeType).setTableName(tableNameWithType)
            .setSegmentName(mergedSegmentName).setInputIndexDirs(originalIndexDirs).setWorkingDir(workingDir)
            .setTableConfig(tableConfig).build();

    List<File> resultFiles = rollupSegmentConverter.convert();
    List<SegmentConversionResult> results = new ArrayList<>();
    for (File file : resultFiles) {
      String outputSegmentName = file.getName();
      results.add(new SegmentConversionResult.Builder().setFile(file).setSegmentName(outputSegmentName)
          .setTableNameWithType(tableNameWithType).build());
    }
    return results;
  }
}
