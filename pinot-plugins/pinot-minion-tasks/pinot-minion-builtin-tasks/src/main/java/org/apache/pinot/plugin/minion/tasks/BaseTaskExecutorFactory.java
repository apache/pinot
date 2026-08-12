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

import org.apache.pinot.minion.MinionConf;
import org.apache.pinot.minion.executor.MinionTaskZkMetadataManager;
import org.apache.pinot.minion.executor.PinotTaskExecutorFactory;
import org.apache.pinot.spi.ingestion.IngestionGroovyPolicy;


/// Base factory that retains initialized minion state for task-executor creation.
///
/// The framework calls [#init(MinionTaskZkMetadataManager, MinionConf)] exactly once before invoking `create()` on a
/// factory. After initialization, subclasses may safely read the retained references from concurrent `create()` calls;
/// neither this class nor subclasses should mutate or replace them.
public abstract class BaseTaskExecutorFactory implements PinotTaskExecutorFactory {
  protected MinionTaskZkMetadataManager _zkMetadataManager;
  protected MinionConf _minionConf;

  @Override
  public final void init(MinionTaskZkMetadataManager zkMetadataManager, MinionConf minionConf) {
    _zkMetadataManager = zkMetadataManager;
    _minionConf = minionConf;
  }

  protected final IngestionGroovyPolicy getIngestionGroovyPolicy() {
    return IngestionGroovyPolicy.fromDisabled(_minionConf == null || _minionConf.isIngestionGroovyDisabled());
  }
}
