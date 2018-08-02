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
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import com.linkedin.pinot.core.common.MinionConstants;
import com.linkedin.pinot.core.minion.SegmentPurger;
import java.io.File;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nonnull;


public class PurgeTaskExecutor extends BaseSingleSegmentConversionExecutor {
  public static final String RECORD_PURGER_KEY = "recordPurger";
  public static final String RECORD_MODIFIER_KEY = "recordModifier";
  public static final String NUM_RECORDS_PURGED_KEY = "numRecordsPurged";
  public static final String NUM_RECORDS_MODIFIED_KEY = "numRecordsModified";

  @Override
  protected SegmentConversionResult convert(@Nonnull PinotTaskConfig pinotTaskConfig, @Nonnull File originalIndexDir,
      @Nonnull File workingDir) throws Exception {
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    String tableNameWithType = configs.get(MinionConstants.TABLE_NAME_KEY);
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);

    SegmentPurger.RecordPurgerFactory recordPurgerFactory = MINION_CONTEXT.getRecordPurgerFactory();
    SegmentPurger.RecordPurger recordPurger =
        recordPurgerFactory != null ? recordPurgerFactory.getRecordPurger(rawTableName) : null;
    SegmentPurger.RecordModifierFactory recordModifierFactory = MINION_CONTEXT.getRecordModifierFactory();
    SegmentPurger.RecordModifier recordModifier =
        recordModifierFactory != null ? recordModifierFactory.getRecordModifier(rawTableName) : null;
    SegmentPurger segmentPurger = new SegmentPurger(originalIndexDir, workingDir, recordPurger, recordModifier);

    File purgedSegmentFile = segmentPurger.purgeSegment();
    if (purgedSegmentFile == null) {
      purgedSegmentFile = originalIndexDir;
    }

    return new SegmentConversionResult.Builder().setFile(purgedSegmentFile)
        .setTableNameWithType(tableNameWithType)
        .setSegmentName(configs.get(MinionConstants.SEGMENT_NAME_KEY))
        .setCustomProperty(RECORD_PURGER_KEY, segmentPurger.getRecordPurger())
        .setCustomProperty(RECORD_MODIFIER_KEY, segmentPurger.getRecordModifier())
        .setCustomProperty(NUM_RECORDS_PURGED_KEY, segmentPurger.getNumRecordsPurged())
        .setCustomProperty(NUM_RECORDS_MODIFIED_KEY, segmentPurger.getNumRecordsModified())
        .build();
  }

  @Override
  protected SegmentZKMetadataCustomMapModifier getSegmentZKMetadataCustomMapModifier() {
    return new SegmentZKMetadataCustomMapModifier(SegmentZKMetadataCustomMapModifier.ModifyMode.REPLACE,
        Collections.singletonMap(MinionConstants.PurgeTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX,
            String.valueOf(System.currentTimeMillis())));
  }
}
