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
package org.apache.pinot.plugin.minion.tasks.materializedview;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.controller.helix.core.minion.generator.BaseTaskGenerator;
import org.apache.pinot.controller.helix.core.minion.generator.TaskGeneratorUtils;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.materializedview.context.MaterializedViewTaskGeneratorContext;
import org.apache.pinot.materializedview.scheduler.MaterializedViewTaskScheduler;
import org.apache.pinot.spi.annotations.minion.TaskGenerator;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;


/// Pinot minion plugin wiring for the materialized-view task scheduler.
@TaskGenerator
public class MaterializedViewTaskGenerator extends BaseTaskGenerator {

  @Override
  public String getTaskType() {
    return CommonConstants.MaterializedViewTask.TASK_TYPE;
  }

  @Override
  public List<PinotTaskConfig> generateTasks(List<TableConfig> tableConfigs) {
    return scheduler().generateTasks(tableConfigs);
  }

  @Override
  public void validateTaskConfigs(TableConfig tableConfig, Schema schema, Map<String, String> taskConfigs) {
    scheduler().validateTaskConfigs(tableConfig, schema, taskConfigs);
  }

  private MaterializedViewTaskScheduler scheduler() {
    return new MaterializedViewTaskScheduler(new ControllerTaskGeneratorContext(_clusterInfoAccessor));
  }

  private static final class ControllerTaskGeneratorContext implements MaterializedViewTaskGeneratorContext {
    private final ClusterInfoAccessor _clusterInfoAccessor;

    private ControllerTaskGeneratorContext(ClusterInfoAccessor clusterInfoAccessor) {
      _clusterInfoAccessor = clusterInfoAccessor;
    }

    @Override
    public HelixPropertyStore<ZNRecord> getPropertyStore() {
      return _clusterInfoAccessor.getPinotHelixResourceManager().getPropertyStore();
    }

    @Override
    public List<SegmentZKMetadata> getSegmentsZKMetadata(String tableNameWithType) {
      return _clusterInfoAccessor.getSegmentsZKMetadata(tableNameWithType);
    }

    @Override
    public String getVipUrl() {
      return _clusterInfoAccessor.getVipUrl();
    }

    @Override
    public void forRunningTasks(String tableNameWithType, String taskType,
        Consumer<Map<String, String>> taskConfigConsumer) {
      TaskGeneratorUtils.forRunningTasks(tableNameWithType, taskType, _clusterInfoAccessor, taskConfigConsumer);
    }

    @Override
    public boolean tableExists(String tableNameWithType) {
      return _clusterInfoAccessor.getTableConfig(tableNameWithType) != null;
    }

    @Override
    public TableConfig getTableConfig(String tableNameWithType) {
      TableConfig tableConfig = _clusterInfoAccessor.getTableConfig(tableNameWithType);
      if (tableConfig == null) {
        throw new IllegalStateException("Table config not found for: " + tableNameWithType
            + " (use tableExists() to probe; this method requires the table to exist)");
      }
      return tableConfig;
    }

    @Override
    public Schema getTableSchema(String tableName) {
      Schema schema = _clusterInfoAccessor.getTableSchema(tableName);
      if (schema == null) {
        throw new IllegalStateException("Schema not found for table: " + tableName
            + " (the table may exist without a registered schema — fix the cluster state)");
      }
      return schema;
    }

    @Override
    public String getClusterConfig(String configName) {
      return _clusterInfoAccessor.getClusterConfig(configName);
    }
  }
}
