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
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotNumReplicaChanger extends PinotZKChanger {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotNumReplicaChanger.class);

  private boolean _dryRun;

  public PinotNumReplicaChanger(String zkAddress, String clusterName, boolean dryRun) {
    super(zkAddress, clusterName);
    _dryRun = dryRun;
  }

  private static void usage() {
    System.out.println("Usage: PinotNumReplicaChanger <zkAddress> <clusterName> <tableName> <numReplicas>");
    System.out.println("Example: localhost:2181 PinotCluster myTable_OFFLINE 5");
    System.exit(1);
  }

  public void changeNumReplicas(final String tableName)
      throws Exception {
    // Get the number of replicas in the tableconfig.
    final String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    final TableConfig offlineTableConfig = ZKMetadataProvider.getOfflineTableConfig(_propertyStore, offlineTableName);
    final int newNumReplicas = Integer.parseInt(offlineTableConfig.getValidationConfig().getReplication());

    // Now get the idealstate, and get the number of replicas in it.
    IdealState currentIdealState = _helixAdmin.getResourceIdealState(_clusterName, offlineTableName);
    int currentNumReplicas = Integer.parseInt(currentIdealState.getReplicas());

    if (newNumReplicas > currentNumReplicas) {
      LOGGER.info("Increasing replicas not yet supported");
    } else if (newNumReplicas == currentNumReplicas) {
      LOGGER.info("Number of replicas ({}) match in table definition and Idealstate. Nothing to do for {}",
          newNumReplicas, offlineTableName);
    } else if (newNumReplicas < currentNumReplicas) {
      if (_dryRun) {
        IdealState newIdealState = updateIdealState(currentIdealState, newNumReplicas);
        LOGGER.info("Final segment Assignment:");
        printSegmentAssignment(newIdealState.getRecord().getMapFields());
      } else {
        HelixHelper.updateIdealState(_helixManager, offlineTableName, new Function<IdealState, IdealState>() {
          @Nullable
          @Override
          public IdealState apply(IdealState idealState) {
            return updateIdealState(idealState, newNumReplicas);
          }
        }, RetryPolicies.exponentialBackoffRetryPolicy(5, 500L, 2.0f));
        waitForStable(offlineTableName);
        LOGGER.info("Successfully changed numReplicas to {} for table {}", newNumReplicas, offlineTableName);
        LOGGER.warn("*** You need to rebalance table {} ***", offlineTableName);
      }
    }
  }

  private IdealState updateIdealState(IdealState idealState, int newNumReplicas) {
    idealState.setReplicas(Integer.toString(newNumReplicas));
    Set<String> segmentIds = idealState.getPartitionSet();
    for (String segmentId : segmentIds) {
      Map<String, String> instanceStateMap = idealState.getInstanceStateMap(segmentId);
      if (instanceStateMap.size() > newNumReplicas) {
        Set<String> keys = instanceStateMap.keySet();
        while (instanceStateMap.size() > newNumReplicas) {
          instanceStateMap.remove(keys.iterator().next());
        }
      } else if (instanceStateMap.size() < newNumReplicas) {
        throw new RuntimeException(
            "Segment " + segmentId + " has " + instanceStateMap.size() + " replicas but want changed to "
                + newNumReplicas);
      }
    }
    return idealState;
  }

  public static void main(String[] args)
      throws Exception {
    final boolean dryRun = true;
    if (args.length != 3) {
      usage();
    }
    final String zkAddress = args[0];
    final String clusterName = args[1];
    final String tableName = args[2];

    PinotNumReplicaChanger replicaChanger = new PinotNumReplicaChanger(zkAddress, clusterName, dryRun);

    replicaChanger.changeNumReplicas(tableName);

    if (dryRun) {
      System.out.println("That was a dryrun");
    }
  }
}
