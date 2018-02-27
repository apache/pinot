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
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import com.linkedin.pinot.core.common.MinionConstants;
import com.linkedin.pinot.core.minion.SegmentPurger;
import java.io.File;
import java.util.Collections;
import javax.annotation.Nonnull;


public class PurgeTaskExecutor extends BaseSegmentConversionExecutor {

  @Override
  protected File convert(@Nonnull PinotTaskConfig pinotTaskConfig, @Nonnull File originalIndexDir,
      @Nonnull File workingDir) throws Exception {
    String rawTableName =
        TableNameBuilder.extractRawTableName(pinotTaskConfig.getConfigs().get(MinionConstants.TABLE_NAME_KEY));
    SegmentPurger.RecordPurgerFactory recordPurgerFactory = MINION_CONTEXT.getRecordPurgerFactory();
    SegmentPurger.RecordPurger recordPurger = null;
    if (recordPurgerFactory != null) {
      recordPurger = recordPurgerFactory.getRecordPurger(rawTableName);
    }
    SegmentPurger.RecordModifierFactory recordModifierFactory = MINION_CONTEXT.getRecordModifierFactory();
    SegmentPurger.RecordModifier recordModifier = null;
    if (recordModifierFactory != null) {
      recordModifier = recordModifierFactory.getRecordModifier(rawTableName);
    }

    return new SegmentPurger(originalIndexDir, workingDir, recordPurger, recordModifier).purgeSegment();
  }

  @Override
  protected SegmentZKMetadataCustomMapModifier getSegmentZKMetadataCustomMapModifier() throws Exception {
    return new SegmentZKMetadataCustomMapModifier(SegmentZKMetadataCustomMapModifier.ModifyMode.REPLACE,
        Collections.singletonMap(MinionConstants.PurgeTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX,
            String.valueOf(System.currentTimeMillis())));
  }
}
