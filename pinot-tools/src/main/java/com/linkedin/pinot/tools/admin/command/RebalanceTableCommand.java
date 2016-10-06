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
package com.linkedin.pinot.tools.admin.command;

import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.tools.Command;
import com.linkedin.pinot.tools.PinotSegmentRebalancer;


public class RebalanceTableCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(StartBrokerCommand.class);

  @Option(name = "-zkAddress", required = false, metaVar = "<http>", usage = "HTTP address of Zookeeper.")
  private String _zkAddress = DEFAULT_ZK_ADDRESS;

  @Option(name = "-clusterName", required = false, metaVar = "<String>", usage = "Pinot cluster name.")
  private String _clusterName = "PinotCluster";

  @Option(name = "-tableName", required = false, metaVar = "<String>", usage = "Table name to rebalance (e.g. myTable_OFFLINE)", forbids ={"-tenantName"})
  private String _tableName;

  @Option(name = "-tenantName", required = false, metaVar = "<string>", usage = "Name of the tenant. Note All offline tables belonging this tenant will be rebalanced", forbids ={"-tableName"})
  private String _tenantName;

  @Option(name = "-exec", required = false, metaVar = "<boolean>", usage = "Execute command (Run the rebalancer)")
  private boolean _exec;

  @Option(name = "-help", required = false, help = true, aliases = { "-h", "--h", "--help" },
      usage = "Print this message.")
  private boolean _help = false;

  public boolean getHelp() {
    return _help;
  }

  @Override
  public String getName() {
    return "RebalanceTable";
  }
  @Override
  public boolean execute() throws Exception {
    boolean _dryRun = !_exec;
    PinotSegmentRebalancer rebalancer = new PinotSegmentRebalancer(_zkAddress, _clusterName, _dryRun);
    if (_tenantName == null && _tableName == null) {
      System.err.println("One of tenantName or tableName must be specified");
      return false;
    }
    if (_tenantName != null) {
      rebalancer.rebalanceTenantTables(_tenantName);
    } else {
      rebalancer.rebalanceTable(_tableName);
    }
    if (_dryRun) {
      LOGGER.info("That was a dryrun");
      LOGGER.info("Use the -exec option to actually execute the command");
    }
    return true;
  }

  @Override
  public String description() {
    return "Rebalance segments for a single table or all tables belonging to a tenant. This is run after adding new nodes to rebalance the segments";
  }
}
