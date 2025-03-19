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
package org.apache.pinot.controller.helix.core.rebalance;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nullable;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.TableSizeReader;
import org.apache.pinot.spi.config.table.TableConfig;


public interface RebalancePreChecker {
  void init(PinotHelixResourceManager pinotHelixResourceManager, @Nullable ExecutorService executorService,
      double diskUtilizationThreshold);

  class PreCheckContext {
    protected final String _rebalanceJobId;
    protected final String _tableNameWithType;
    protected final TableConfig _tableConfig;
    protected final Map<String, Map<String, String>> _currentAssignment;
    protected final Map<String, Map<String, String>> _targetAssignment;
    protected final TableSizeReader.TableSubTypeSizeDetails _tableSubTypeSizeDetails;

    public PreCheckContext(String rebalanceJobId, String tableNameWithType, TableConfig tableConfig,
        Map<String, Map<String, String>> currentAssignment, Map<String, Map<String, String>> targetAssignment,
        @Nullable TableSizeReader.TableSubTypeSizeDetails tableSubTypeSizeDetails) {
      _rebalanceJobId = rebalanceJobId;
      _tableNameWithType = tableNameWithType;
      _tableConfig = tableConfig;
      _currentAssignment = currentAssignment;
      _targetAssignment = targetAssignment;
      _tableSubTypeSizeDetails = tableSubTypeSizeDetails;
    }
  }

  Map<String, RebalancePreCheckerResult> check(PreCheckContext preCheckContext);
}
