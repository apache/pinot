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
package org.apache.pinot.minion.executor;

import com.google.common.base.Preconditions;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.minion.MinionContext;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


public abstract class BaseTaskExecutor implements PinotTaskExecutor {
  protected static final MinionContext MINION_CONTEXT = MinionContext.getInstance();

  protected boolean _cancelled = false;

  @Override
  public void cancel() {
    _cancelled = true;
  }

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

  /**
   * Pre processing operations to be done at the beginning of task execution
   */
  protected void preProcess(PinotTaskConfig pinotTaskConfig) {
  }

  /**
   * Post processing operations to be done before exiting a successful task execution
   */
  protected void postProcess(PinotTaskConfig pinotTaskConfig) {
  }
}
