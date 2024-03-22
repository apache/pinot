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
package org.apache.pinot.plugin.minion.tasks.purge;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.common.metrics.MinionMeter;
import org.apache.pinot.common.metrics.MinionMetrics;
import org.apache.pinot.common.metrics.MinionTimer;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.core.minion.SegmentPurger;
import org.apache.pinot.plugin.minion.tasks.BaseSingleSegmentConversionExecutor;
import org.apache.pinot.plugin.minion.tasks.BaseTaskExecutor;
import org.apache.pinot.plugin.minion.tasks.SegmentConversionResult;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PurgeTaskExecutor extends BaseSingleSegmentConversionExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(PurgeTaskExecutor.class);
  protected final MinionMetrics _minionMetrics = MinionMetrics.get();
  public static final String RECORD_PURGER_KEY = "recordPurger";
  public static final String RECORD_MODIFIER_KEY = "recordModifier";
  public static final String NUM_RECORDS_PURGED_KEY = "numRecordsPurged";
  public static final String NUM_RECORDS_MODIFIED_KEY = "numRecordsModified";
  private static final ThreadMXBean MX_BEAN = ManagementFactory.getThreadMXBean();

  @Override
  protected SegmentConversionResult convert(PinotTaskConfig pinotTaskConfig, File indexDir, File workingDir)
      throws Exception {
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    String tableNameWithType = configs.get(MinionConstants.TABLE_NAME_KEY);
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    String taskType = pinotTaskConfig.getTaskType();

    SegmentPurger.RecordPurgerFactory recordPurgerFactory = MINION_CONTEXT.getRecordPurgerFactory();
    TableConfig tableConfig = getTableConfig(tableNameWithType);
    Schema schema = getSchema(tableNameWithType);
    SegmentPurger.RecordPurger recordPurger =
        recordPurgerFactory != null ? recordPurgerFactory.getRecordPurger(pinotTaskConfig, tableConfig, schema) : null;
    SegmentPurger.RecordModifierFactory recordModifierFactory = MINION_CONTEXT.getRecordModifierFactory();
    SegmentPurger.RecordModifier recordModifier =
        recordModifierFactory != null ? recordModifierFactory.getRecordModifier(rawTableName) : null;

    _eventObserver.notifyProgress(pinotTaskConfig, "Purging segment: " + indexDir);
    SegmentPurger segmentPurger =
        new SegmentPurger(indexDir, workingDir, tableConfig, schema, recordPurger, recordModifier);
    long purgeTaskStartTimeNs = MX_BEAN.getCurrentThreadCpuTime();
    File purgedSegmentFile = segmentPurger.purgeSegment();
    long purgeTaskEndTimeNs = MX_BEAN.getCurrentThreadCpuTime();
    _minionMetrics.addTimedTableValue(tableNameWithType, taskType, MinionTimer.TASK_THREAD_CPU_TIME_NS,
        purgeTaskEndTimeNs - purgeTaskStartTimeNs, TimeUnit.NANOSECONDS);
    if (purgedSegmentFile == null) {
      purgedSegmentFile = indexDir;
    }

    BaseTaskExecutor.addTaskMeterMetrics(MinionMeter.RECORDS_PER_SEGMENT, segmentPurger.getTotalRecordsProcessed(),
        tableNameWithType, taskType);
    BaseTaskExecutor.addTaskMeterMetrics(MinionMeter.RECORDS_PURGED_PER_SEGMENT,
        segmentPurger.getNumRecordsPurged(), tableNameWithType, taskType);

    return new SegmentConversionResult.Builder().setFile(purgedSegmentFile).setTableNameWithType(tableNameWithType)
        .setSegmentName(configs.get(MinionConstants.SEGMENT_NAME_KEY))
        .setCustomProperty(RECORD_PURGER_KEY, segmentPurger.getRecordPurger())
        .setCustomProperty(RECORD_MODIFIER_KEY, segmentPurger.getRecordModifier())
        .setCustomProperty(NUM_RECORDS_PURGED_KEY, segmentPurger.getNumRecordsPurged())
        .setCustomProperty(NUM_RECORDS_MODIFIED_KEY, segmentPurger.getNumRecordsModified()).build();
  }

  @Override
  protected SegmentZKMetadataCustomMapModifier getSegmentZKMetadataCustomMapModifier(PinotTaskConfig pinotTaskConfig,
      SegmentConversionResult segmentConversionResult) {
    return new SegmentZKMetadataCustomMapModifier(SegmentZKMetadataCustomMapModifier.ModifyMode.UPDATE,
        Collections.singletonMap(MinionConstants.PurgeTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX,
            String.valueOf(System.currentTimeMillis())));
  }
}
