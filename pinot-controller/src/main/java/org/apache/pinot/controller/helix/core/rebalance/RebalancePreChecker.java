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
import java.util.Set;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nullable;
import org.apache.pinot.common.restlet.resources.RebalanceConfig;
import org.apache.pinot.common.restlet.resources.RebalancePreCheckerResult;
import org.apache.pinot.common.restlet.resources.RebalanceSummaryResult;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.TableSizeReader;
import org.apache.pinot.spi.config.table.TableConfig;


public interface RebalancePreChecker {
  void init(PinotHelixResourceManager pinotHelixResourceManager, @Nullable ExecutorService executorService,
      double diskUtilizationThreshold);

  class PreCheckContext {
    private final String _rebalanceJobId;
    private final String _tableNameWithType;
    private final TableConfig _tableConfig;
    private final Map<String, Map<String, String>> _currentAssignment;
    private final Map<String, Map<String, String>> _targetAssignment;
    private final TableSizeReader.TableSubTypeSizeDetails _tableSubTypeSizeDetails;
    private final RebalanceConfig _rebalanceConfig;
    private final RebalanceSummaryResult _rebalanceSummaryResult;
    private final Map<String, Set<String>> _providedTierToSegmentsMap;

    public PreCheckContext(String rebalanceJobId, String tableNameWithType, TableConfig tableConfig,
        Map<String, Map<String, String>> currentAssignment, Map<String, Map<String, String>> targetAssignment,
        @Nullable TableSizeReader.TableSubTypeSizeDetails tableSubTypeSizeDetails, RebalanceConfig rebalanceConfig,
        @Nullable RebalanceSummaryResult rebalanceSummaryResult,
        @Nullable Map<String, Set<String>> providedTierToSegmentsMap) {
      _rebalanceJobId = rebalanceJobId;
      _tableNameWithType = tableNameWithType;
      _tableConfig = tableConfig;
      _currentAssignment = currentAssignment;
      _targetAssignment = targetAssignment;
      _tableSubTypeSizeDetails = tableSubTypeSizeDetails;
      _rebalanceConfig = rebalanceConfig;
      _rebalanceSummaryResult = rebalanceSummaryResult;
      _providedTierToSegmentsMap = providedTierToSegmentsMap;
    }

    public String getRebalanceJobId() {
      return _rebalanceJobId;
    }

    public String getTableNameWithType() {
      return _tableNameWithType;
    }

    public TableConfig getTableConfig() {
      return _tableConfig;
    }

    public Map<String, Map<String, String>> getCurrentAssignment() {
      return _currentAssignment;
    }

    public Map<String, Map<String, String>> getTargetAssignment() {
      return _targetAssignment;
    }

    public TableSizeReader.TableSubTypeSizeDetails getTableSubTypeSizeDetails() {
      return _tableSubTypeSizeDetails;
    }

    public RebalanceConfig getRebalanceConfig() {
      return _rebalanceConfig;
    }

    public RebalanceSummaryResult getRebalanceSummaryResult() {
      return _rebalanceSummaryResult;
    }

    /// Returns the tier name to segments map computed while updating the target tiers of this rebalance, or
    /// `null` if the target tiers were not updated (i.e. updateTargetTier is disabled).
    @Nullable
    public Map<String, Set<String>> getProvidedTierToSegmentsMap() {
      return _providedTierToSegmentsMap;
    }
  }

  Map<String, RebalancePreCheckerResult> check(PreCheckContext preCheckContext);
}
