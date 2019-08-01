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

import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.PinotTableRebalancer;
import org.kohsuke.args4j.Option;


/**
 * A sub-command for pinot-admin tool to rebalance a specific table
 */
public class RebalanceCommand extends AbstractBaseAdminCommand implements Command {

  @Option(name = "-zkAddress", required = true, metaVar = "<http>", usage = "HTTP address of Zookeeper.")
  private String _zkAddress;

  @Option(name = "-clusterName", required = true, metaVar = "<String>", usage = "Pinot cluster name.")
  private String _clusterName;

  @Option(name = "-tableName", required = true, metaVar = "<String>", usage = "Name of the table to rebalance (e.g. myTable_OFFLINE)")
  private String _tableName;

  @Option(name = "-tableType", required = true, metaVar = "<String>", usage = "Type of table (OFFLINE or REALTIME)")
  private String _tableType;

  @Option(name = "-dryRun", required = false, metaVar = "<boolean>", usage = "Dry Run (just get the target ideal state without rebalancing)")
  private boolean _dryRun = false;

  @Option(name = "-includeConsuming", required = false, metaVar = "<boolean>", usage = "Consider consuming segments while rebalancing realtime tables")
  private boolean _includeConsuming = false;

  @Option(name = "-downtime", required = false, metaVar = "<boolean>", usage = "Rebalance with downtime")
  private boolean _downtime = false;

  @Option(name = "-minAvailableReplicas", required = false, metaVar = "<int>", usage = "Minimum number of replicas that will be available for no downtime rebalancing")
  private int _minAvailableReplicas = 1;

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
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
    final PinotTableRebalancer tableRebalancer =
        new PinotTableRebalancer(_zkAddress, _clusterName, _dryRun, !_downtime, _includeConsuming,
            _minAvailableReplicas);
    return tableRebalancer.rebalance(_tableName, _tableType);
  }

  @Override
  public String description() {
    final String description =
        "Rebalances segments of a table. " + "By default, rebalancing is done without downtime where the "
            + "the rebalancer ensures at least one replica per segment is available to serve queries "
            + "while segments get rebalanced. For better QPS with no downtime "
            + "use the -minAvailableReplicas <val> option to ensure that rebalancer keeps at least "
            + "the required number of replicas alive for each segment while segments are being moved "
            + "between hosts in no downtime mode. Use the -downtime option to rebalance with downtime "
            + "where there is no guarantee on the availability of replicas for serving queries as "
            + "segments are moved around ";
    return description;
  }

  @Override
  public void printExamples() {
    System.out.println("Usage examples:\n\n");

    System.out.println("Help for RebalanceTable command");
    System.out.println("sh pinot-admin.sh RebalanceTable -h");

    System.out.println("Running rebalancer in dry run mode");
    System.out
        .println("sh pinot-admin.sh RebalanceTable -zkAddress localhost:2191 -clusterName PinotCluster -tableName myTable -tableType offline -dryRun");

    System.out.println("\nRunning rebalancer in downtime mode");
    System.out.println("sh pinot-admin.sh RebalanceTable -zkAddress localhost:2191 -clusterName PinotCluster -tableName myTable -tableType offline -downtime");

    System.out.println("\n\nExample usage for running rebalancer in no-downtime (default) mode. "
        + "Rebalancer will try to keep at least one replica up for each segment while rebalancing");
    System.out.println(
        "sh pinot-admin.sh RebalanceTable -zkAddress localhost:2191 -clusterName PinotCluster -tableName myTable -tableType offline");

    System.out.println("\n\nExample usage for running rebalancer in no-downtime (default) mode. "
        + "Rebalancer will keep the given number of min replicas up for each segment while rebalancing");
    System.out.println(
        "sh pinot-admin.sh RebalanceTable -zkAddress localhost:2191 -clusterName PinotCluster -tableName myTable -tableType offline -minAvailableReplicas 2");
  }
}
