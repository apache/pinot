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

import org.apache.pinot.controller.helix.core.rebalance.RebalanceResult;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.RebalanceConfigConstants;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.PinotTableRebalancer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


/**
 * A sub-command for pinot-admin tool to rebalance a specific table
 */
@SuppressWarnings({"FieldCanBeLocal", "unused"})
@CommandLine.Command(name = "RebalanceTable", description = "Reassign instances and segments for a table.",
    mixinStandardHelpOptions = true)
public class RebalanceTableCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(RebalanceTableCommand.class);

  @CommandLine.Option(names = {"-zkAddress"}, required = true, description = "HTTP address of Zookeeper")
  private String _zkAddress;

  @CommandLine.Option(names = {"-clusterName"}, required = true, description = "Name of the Pinot cluster")
  private String _clusterName;

  @CommandLine.Option(names = {"-tableName"}, required = true,
      description = "Name of the table to rebalance (with type suffix, e.g. myTable_OFFLINE)")
  private String _tableNameWithType;

  @CommandLine.Option(names = {"-dryRun"},
      description = "Whether to rebalance table in dry-run mode (just log the target assignment without applying"
          + " changes to the cluster, false by default)")
  private boolean _dryRun = false;

  @CommandLine.Option(names = {"-reassignInstances"},
      description = "Whether to reassign instances before reassigning segments (false by default)")
  private boolean _reassignInstances = false;

  @CommandLine.Option(names = {"-includeConsuming"},
      description = "Whether to reassign CONSUMING segments for real-time table (false by default)")
  private boolean _includeConsuming = false;

  @CommandLine.Option(names = {"-bootstrap"},
      description = "Whether to rebalance table in bootstrap mode (regardless of minimum segment movement, reassign"
          + " all segments in a round-robin fashion as if adding new segments to an empty table, false by default)")
  private boolean _bootstrap = false;

  @CommandLine.Option(names = {"-downtime"},
      description = "Whether to allow downtime for the rebalance (false by default)")
  private boolean _downtime = false;

  @CommandLine.Option(names = {"-minAvailableReplicas"},
      description = "For no-downtime rebalance, minimum number of replicas to keep alive during rebalance, or maximum "
          + "number of replicas allowed to be unavailable if value is negative (1 by default)")
  private int _minAvailableReplicas = 1;

  @CommandLine.Option(names = {"-bestEfforts"},
      description = "Whether to use best-efforts to rebalance (not fail the rebalance when the no-downtime contract"
          + " cannot be achieved, false by default)")
  private boolean _bestEfforts = false;

  @CommandLine.Option(names = {"-externalViewCheckIntervalInMs"},
      description = "How often to check if external view converges with ideal view")
  private long _externalViewCheckIntervalInMs = RebalanceConfigConstants.DEFAULT_EXTERNAL_VIEW_CHECK_INTERVAL_IN_MS;

  @CommandLine.Option(names = {"-externalViewStabilizationTimeoutInMs"},
      description = "How long to wait till external view converges with ideal view")
  private long _externalViewStabilizationTimeoutInMs =
      RebalanceConfigConstants.DEFAULT_EXTERNAL_VIEW_STABILIZATION_TIMEOUT_IN_MS;

  @Override
  public String getName() {
    return "RebalanceTable";
  }

  @Override
  public boolean execute()
      throws Exception {
    PinotTableRebalancer tableRebalancer =
        new PinotTableRebalancer(_zkAddress, _clusterName, _dryRun, _reassignInstances, _includeConsuming, _bootstrap,
            _downtime, _minAvailableReplicas, _bestEfforts, _externalViewCheckIntervalInMs,
            _externalViewStabilizationTimeoutInMs);
    RebalanceResult rebalanceResult = tableRebalancer.rebalance(_tableNameWithType);
    LOGGER
        .info("Got rebalance result: {} for table: {}", JsonUtils.objectToString(rebalanceResult), _tableNameWithType);
    return rebalanceResult.getStatus() == RebalanceResult.Status.DONE;
  }
}
