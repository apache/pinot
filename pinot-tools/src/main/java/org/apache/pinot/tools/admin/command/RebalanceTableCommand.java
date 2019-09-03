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
package org.apache.pinot.tools.admin.command;

import org.apache.pinot.common.utils.JsonUtils;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceResult;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.PinotTableRebalancer;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A sub-command for pinot-admin tool to rebalance a specific table
 */
@SuppressWarnings({"FieldCanBeLocal", "unused"})
public class RebalanceTableCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(RebalanceTableCommand.class);

  @Option(name = "-zkAddress", required = true, metaVar = "<http>", usage = "HTTP address of Zookeeper")
  private String _zkAddress;

  @Option(name = "-clusterName", required = true, metaVar = "<String>", usage = "Name of the Pinot cluster")
  private String _clusterName;

  @Option(name = "-tableName", required = true, metaVar = "<String>", usage = "Name of the table to rebalance (with type suffix, e.g. myTable_OFFLINE)")
  private String _tableNameWithType;

  @Option(name = "-dryRun", metaVar = "<boolean>", usage = "Whether to execute in dry-run mode (just log the target assignment without applying changes to the cluster, false by default)")
  private boolean _dryRun = false;

  @Option(name = "-reassignInstances", metaVar = "<boolean>", usage = "Whether to reassign instances (false by default)")
  private boolean _reassignInstances = false;

  @Option(name = "-includeConsuming", metaVar = "<boolean>", usage = "Whether to rebalance CONSUMING segments (false by default)")
  private boolean _includeConsuming = false;

  @Option(name = "-downtime", metaVar = "<boolean>", usage = "Whether to allow downtime for the rebalance (false by default)")
  private boolean _downtime = false;

  @Option(name = "-minAvailableReplicas", metaVar = "<int>", usage = "Minimum number of replicas that will be available, or maximum number of replicas that are allowed to be unavailable if negative number is provided for no-downtime mode (1 by default)")
  private int _minAvailableReplicas = 1;

  @Option(name = "-help", help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message")
  private boolean _help = false;

  public boolean getHelp() {
    return _help;
  }

  @Override
  public String getName() {
    return "RebalanceTable";
  }

  @Override
  public boolean execute()
      throws Exception {
    PinotTableRebalancer tableRebalancer =
        new PinotTableRebalancer(_zkAddress, _clusterName, _dryRun, _reassignInstances, _includeConsuming, _downtime,
            _minAvailableReplicas);
    RebalanceResult rebalanceResult = tableRebalancer.rebalance(_tableNameWithType);
    LOGGER
        .info("Got rebalance result: {} for table: {}", JsonUtils.objectToString(rebalanceResult), _tableNameWithType);
    return rebalanceResult.getStatus() == RebalanceResult.Status.DONE;
  }

  @Override
  public String description() {
    return "Reassign instances and rebalances segments of a table.";
  }

  @Override
  public void printExamples() {
    System.out.println("Usage examples:");
    System.out.println();
    System.out.println("Help for RebalanceTable command");
    System.out.println("sh pinot-admin.sh RebalanceTable -h");
    System.out.println();
    System.out.println("Running rebalancer in dry-run mode");
    System.out.println(
        "sh pinot-admin.sh RebalanceTable -zkAddress localhost:2191 -clusterName PinotCluster -tableName myTable_OFFLINE -dryRun");
    System.out.println();
    System.out.println("Running rebalancer with instances reassigned");
    System.out.println(
        "sh pinot-admin.sh RebalanceTable -zkAddress localhost:2191 -clusterName PinotCluster -tableName myTable_OFFLINE -reassignInstances");
    System.out.println();
    System.out.println("Running rebalancer with downtime");
    System.out.println(
        "sh pinot-admin.sh RebalanceTable -zkAddress localhost:2191 -clusterName PinotCluster -tableName myTable_OFFLINE -downtime");
    System.out.println();
    System.out.println(
        "Running rebalancer without downtime (default). Rebalancer will try to keep at least one replica up for each segment while rebalancing");
    System.out.println(
        "sh pinot-admin.sh RebalanceTable -zkAddress localhost:2191 -clusterName PinotCluster -tableName myTable_OFFLINE");
    System.out.println();
    System.out.println(
        "Running rebalancer with configured min replicas up. Rebalancer will keep the given number of min replicas up for each segment while rebalancing");
    System.out.println(
        "sh pinot-admin.sh RebalanceTable -zkAddress localhost:2191 -clusterName PinotCluster -tableName myTable_OFFLINE -minAvailableReplicas 2");
    System.out.println();
    System.out.println(
        "Running rebalancer with configured max replicas down. Rebalancer will keep the given number of max replicas down for each segment while rebalancing");
    System.out.println(
        "sh pinot-admin.sh RebalanceTable -zkAddress localhost:2191 -clusterName PinotCluster -tableName myTable_OFFLINE -minAvailableReplicas -1");
  }
}
