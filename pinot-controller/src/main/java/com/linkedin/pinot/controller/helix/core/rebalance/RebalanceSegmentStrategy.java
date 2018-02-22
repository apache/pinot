/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.controller.helix.core.rebalance;

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.controller.helix.PartitionAssignment;
import org.apache.helix.model.IdealState;


/**
 * Interface for rebalance segment strategies
 */
public interface RebalanceSegmentStrategy {

  /**
   * Rebalances and writes partition assignments involved in the rebalance of the table
   * @param idealState old ideal state
   * @param tableConfig table config of table tor rebalance
   * @param rebalanceUserParams custom user configs for specific rebalance strategies
   * @return rebalanced partition assignments
   */
  PartitionAssignment rebalancePartitionAssignment(IdealState idealState, TableConfig tableConfig,
      RebalanceUserParams rebalanceUserParams);

  /**
   * Rebalances segments and writes ideal state of table
   * @param idealState old ideal state
   * @param tableConfig table config of table tor rebalance
   * @param rebalanceUserParams custom user configs for specific rebalance strategies
   * @param newPartitionAssignment new rebalaned partition assignments as part of the resource rebalance
   * @return rebalanced ideal state
   */
  IdealState rebalanceIdealState(IdealState idealState, TableConfig tableConfig, RebalanceUserParams rebalanceUserParams,
      PartitionAssignment newPartitionAssignment);
}