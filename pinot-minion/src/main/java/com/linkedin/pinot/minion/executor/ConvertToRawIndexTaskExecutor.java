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
package com.linkedin.pinot.minion.executor;

import com.linkedin.pinot.common.config.PinotTaskConfig;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import com.linkedin.pinot.core.common.MinionConstants;
import com.linkedin.pinot.core.minion.RawIndexConverter;
import java.io.File;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nonnull;


public class ConvertToRawIndexTaskExecutor extends BaseSingleSegmentConversionExecutor {

  @Override
  protected SegmentConversionResult convert(@Nonnull PinotTaskConfig pinotTaskConfig, @Nonnull File originalIndexDir,
      @Nonnull File workingDir) throws Exception {
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    new RawIndexConverter(originalIndexDir, workingDir,
        configs.get(MinionConstants.ConvertToRawIndexTask.COLUMNS_TO_CONVERT_KEY)).convert();
    return new SegmentConversionResult.Builder().setFile(workingDir)
        .setTableNameWithType(configs.get(MinionConstants.TABLE_NAME_KEY))
        .setSegmentName(configs.get(MinionConstants.SEGMENT_NAME_KEY))
        .build();
  }

  @Override
  protected SegmentZKMetadataCustomMapModifier getSegmentZKMetadataCustomMapModifier() {
    return new SegmentZKMetadataCustomMapModifier(SegmentZKMetadataCustomMapModifier.ModifyMode.UPDATE,
        Collections.singletonMap(MinionConstants.ConvertToRawIndexTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX,
            String.valueOf(System.currentTimeMillis())));
  }
}
