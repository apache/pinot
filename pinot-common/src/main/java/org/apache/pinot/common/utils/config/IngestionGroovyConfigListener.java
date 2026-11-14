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
package org.apache.pinot.common.utils.config;

import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.evaluator.GroovyFunctionEvaluator;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.utils.CommonConstants.Groovy;


/// Applies the cluster ingestion Groovy policy in server and minion processes.
///
/// Missing, removed, and invalid values disable new ingestion compilation; only `false` enables it. Query Groovy
/// and previously compiled scripts retain their own behavior. Thread-safe: updates publish through a volatile flag.
public class IngestionGroovyConfigListener implements PinotClusterConfigChangeListener {
  @Override
  public void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    applyClusterConfig(clusterConfigs);
  }

  /// Applies a raw cluster snapshot before a server or minion starts ingestion or query processing.
  public static void applyClusterConfig(Map<String, String> clusterConfigs) {
    GroovyFunctionEvaluator.setIngestionGroovyDisabled(
        Groovy.isIngestionGroovyDisabled(clusterConfigs.get(Groovy.DISABLE_INGESTION_GROOVY)));
  }
}
