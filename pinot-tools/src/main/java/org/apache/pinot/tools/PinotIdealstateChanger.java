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
package org.apache.pinot.tools;

import com.google.common.base.Function;
import javax.annotation.Nullable;
import org.apache.helix.HelixManager;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.spi.utils.retry.RetryPolicies;


public class PinotIdealstateChanger extends PinotZKChanger {

  private final String _tableNameWithType;
  private final boolean _dryRun;

  public PinotIdealstateChanger(String zkAddress, String clusterName, String tableNameWithType, boolean dryRun) {
    super(zkAddress, clusterName);
    _tableNameWithType = tableNameWithType;
    _dryRun = dryRun;
  }

  public void updateIdealState() {
    HelixManager helixManager = getHelixManager();
    HelixHelper.updateIdealState(helixManager, _tableNameWithType, new Function<IdealState, IdealState>() {
      @Nullable
      @Override
      public IdealState apply(@Nullable IdealState input) {
        return applyChangesToIdealState(input);
      }
    }, RetryPolicies.exponentialBackoffRetryPolicy(5, 100, 1.2));
  }

  private IdealState applyChangesToIdealState(IdealState input) {
    // Add code here as needed to change idealstate
    System.out.println("No change applied to idealstate.");
    return input;
  }

  public static void main(String[] args) {
    final boolean dryRun = true;
    final String zkAddress = "localhost:2191";
    final String clusterName = "LLCRealtimeClusterIntegrationTest";
    final String tableName = "mytable_REALTIME";

    PinotIdealstateChanger changer = new PinotIdealstateChanger(zkAddress, clusterName, tableName, dryRun);
    changer.updateIdealState();
  }
}
