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
package com.linkedin.pinot.minion.executor;

import com.linkedin.pinot.common.config.PinotTaskConfig;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import com.linkedin.pinot.core.common.MinionConstants;
import com.linkedin.pinot.core.minion.RawIndexConverter;
import java.io.File;
import java.util.Collections;
import javax.annotation.Nonnull;


public class ConvertToRawIndexTaskExecutor extends BaseSegmentConversionExecutor {

  @Override
  protected File convert(@Nonnull PinotTaskConfig pinotTaskConfig, @Nonnull File originalIndexDir,
      @Nonnull File workingDir) throws Exception {
    new RawIndexConverter(originalIndexDir, workingDir,
        pinotTaskConfig.getConfigs().get(MinionConstants.ConvertToRawIndexTask.COLUMNS_TO_CONVERT_KEY)).convert();
    return workingDir;
  }

  @Override
  protected SegmentZKMetadataCustomMapModifier getSegmentZKMetadataCustomMapModifier() throws Exception {
    return new SegmentZKMetadataCustomMapModifier(SegmentZKMetadataCustomMapModifier.ModifyMode.UPDATE,
        Collections.singletonMap(MinionConstants.ConvertToRawIndexTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX,
            String.valueOf(System.currentTimeMillis())));
  }
}
