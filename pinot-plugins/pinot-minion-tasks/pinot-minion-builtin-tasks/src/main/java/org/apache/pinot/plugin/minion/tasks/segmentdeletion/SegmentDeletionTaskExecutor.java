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
package org.apache.pinot.plugin.minion.tasks.segmentdeletion;

import java.util.Collections;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.minion.MinionConf;
import org.apache.pinot.plugin.minion.tasks.BaseTaskExecutor;
import org.apache.pinot.plugin.minion.tasks.SegmentConversionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentDeletionTaskExecutor extends BaseTaskExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentDeletionTaskExecutor.class);
  private MinionConf _minionConf;

  public SegmentDeletionTaskExecutor(MinionConf minionConf) {
    _minionConf = minionConf;
  }

  @Override
  protected SegmentZKMetadataCustomMapModifier getSegmentZKMetadataCustomMapModifier(PinotTaskConfig pinotTaskConfig,
      SegmentConversionResult segmentConversionResult) {
    return new SegmentZKMetadataCustomMapModifier(SegmentZKMetadataCustomMapModifier.ModifyMode.UPDATE,
        Collections.singletonMap(MinionConstants.SegmentDeletionTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX,
            String.valueOf(System.currentTimeMillis())));
  }

  @Override
  public Object executeTask(PinotTaskConfig pinotTaskConfig)
      throws Exception {

    SegmentDeletionResult.Builder resultBuilder = new SegmentDeletionResult.Builder();

    String tableName = pinotTaskConfig.getTableName();
    String segmentName = pinotTaskConfig.getConfigs().get("segmentName");
    String operator = pinotTaskConfig.getConfigs().get("operator");

    boolean success = false;

    if (tableName == null) {
      LOGGER.warn("tableName is missing");
    }

    if (segmentName == null) {
      LOGGER.warn("segmentName is missing");
    }

    if (operator == null) {
      LOGGER.warn("operator is missing");
    }

    if ((segmentName != null) && (operator != null) && (tableName != null)) {
      /*

            TODO need to implement "delete segment" behavior

            what is the correct mechanism for deleting segments?

            should we use the pinotHelixResourceManager to delete segments?

                pinotHelixResourceManager.deleteSegments(tableName, List.of(segmentName));

            We do not have a reference to pinotHelixResourceManager

          TODO need to handle two specific use cases:
              1)  where $segmentName = 'foo'
              1)  where $segmentName LIKE 'segNamePattern'

       */
      success = true;
    }

    resultBuilder.setSucceed(success);
    resultBuilder.setSegmentName(segmentName);
    return resultBuilder.build();
  }
}
