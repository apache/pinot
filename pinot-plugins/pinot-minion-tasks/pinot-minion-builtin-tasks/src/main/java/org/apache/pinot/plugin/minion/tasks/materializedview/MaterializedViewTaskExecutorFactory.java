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

import java.util.Collections;
import org.apache.pinot.common.config.GrpcConfig;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.minion.MinionConf;
import org.apache.pinot.minion.MinionContext;
import org.apache.pinot.minion.executor.MinionTaskZkMetadataManager;
import org.apache.pinot.minion.executor.PinotTaskExecutor;
import org.apache.pinot.minion.executor.PinotTaskExecutorFactory;
import org.apache.pinot.spi.annotations.minion.TaskExecutorFactory;


/**
 * Factory for creating {@link MaterializedViewTaskExecutor} instances.
 *
 * <p>Creates a long-lived {@link GrpcMvQueryExecutor} lazily on the first
 * {@link #create()} call (not during {@link #init}) because the
 * {@link MinionContext#getHelixManager()} is only available after the minion
 * has connected to Helix, which happens after factory initialization.
 * The executor is then shared across all executor instances for connection
 * reuse and load balancing.
 */
@TaskExecutorFactory
public class MaterializedViewTaskExecutorFactory implements PinotTaskExecutorFactory {
  private MinionTaskZkMetadataManager _zkMetadataManager;
  private MinionConf _minionConf;
  private volatile MvQueryExecutor _queryExecutor;

  @Override
  public void init(MinionTaskZkMetadataManager zkMetadataManager) {
    _zkMetadataManager = zkMetadataManager;
  }

  @Override
  public void init(MinionTaskZkMetadataManager zkMetadataManager, MinionConf minionConf) {
    _zkMetadataManager = zkMetadataManager;
    _minionConf = minionConf;
  }

  @Override
  public String getTaskType() {
    return MinionConstants.MaterializedViewTask.TASK_TYPE;
  }

  @Override
  public PinotTaskExecutor create() {
    if (_queryExecutor == null) {
      synchronized (this) {
        if (_queryExecutor == null) {
          _queryExecutor = new GrpcMvQueryExecutor(
              MinionContext.getInstance().getHelixManager(),
              new GrpcConfig(Collections.emptyMap()));
        }
      }
    }
    return new MaterializedViewTaskExecutor(_zkMetadataManager, _minionConf, _queryExecutor);
  }
}
