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
package org.apache.pinot.plugin.minion.tasks;

import com.google.common.base.Preconditions;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.common.metrics.MinionMeter;
import org.apache.pinot.common.metrics.MinionMetrics;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.minion.MinionContext;
import org.apache.pinot.minion.executor.PinotTaskExecutor;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


public abstract class BaseTaskExecutor implements PinotTaskExecutor {
  protected static final MinionContext MINION_CONTEXT = MinionContext.getInstance();

  protected boolean _cancelled = false;
  protected final MinionMetrics _minionMetrics = MinionMetrics.get();

  @Override
  public void cancel() {
    _cancelled = true;
  }

  /**
   * Returns the segment ZK metadata custom map modifier.
   */
  protected abstract SegmentZKMetadataCustomMapModifier getSegmentZKMetadataCustomMapModifier(
      PinotTaskConfig pinotTaskConfig, SegmentConversionResult segmentConversionResult);

  protected TableConfig getTableConfig(String tableNameWithType) {
    TableConfig tableConfig =
        ZKMetadataProvider.getTableConfig(MINION_CONTEXT.getHelixPropertyStore(), tableNameWithType);
    Preconditions.checkState(tableConfig != null, "Failed to find table config for table: %s", tableNameWithType);
    return tableConfig;
  }

  protected Schema getSchema(String tableName) {
    Schema schema = ZKMetadataProvider.getTableSchema(MINION_CONTEXT.getHelixPropertyStore(), tableName);
    Preconditions.checkState(schema != null, "Failed to find schema for table: %s", tableName);
    return schema;
  }

  protected long getSegmentCrc(String tableNameWithType, String segmentName) {
    SegmentZKMetadata segmentZKMetadata =
        ZKMetadataProvider.getSegmentZKMetadata(MINION_CONTEXT.getHelixPropertyStore(), tableNameWithType, segmentName);
    /*
     * If the segmentZKMetadata is null, it is likely that the segment has been deleted, return -1 as CRC in this case,
     * so that task can terminate early when verify CRC. If we throw exception, helix will keep retrying this forever
     * and task status would be left unchanged without proper cleanup.
     */
    return segmentZKMetadata == null ? -1 : segmentZKMetadata.getCrc();
  }

  public static void addTaskMeterMetrics(MinionMeter meter, long unitCount, String tableName, String taskType) {
    MinionMetrics.get().addMeteredGlobalValue(meter, unitCount);
    MinionMetrics.get().addMeteredTableValue(tableName, meter, unitCount);
    MinionMetrics.get().addMeteredTableValue(tableName, taskType, meter, unitCount);
  }
}
