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

import com.linkedin.pinot.tools.ClusterStateVerifier;
import com.linkedin.pinot.tools.Command;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class VerifyClusterStateCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(VerifyClusterStateCommand.class);

  @Option(name = "-zkAddress", required = false, metaVar = "<http>", usage = "HTTP address of Zookeeper.")
  private String _zkAddress = DEFAULT_ZK_ADDRESS;

  @Option(name = "-clusterName", required = false, metaVar = "<String>", usage = "Pinot cluster name.")
  private String _clusterName = "PinotCluster";

  @Option(name = "-tableName", required = false, metaVar = "<String>", usage = "Table name to check the state (e.g. myTable_OFFLINE).")
  private String _tableName;

  @Option(name = "-timeoutSec", required = false, metaVar = "<long>", usage = "Maximum timeout in second.")
  private long _timeoutSec = 300L;

  @Option(name = "-help", required = false, help = true, aliases = { "-h", "--h", "--help" },
      usage = "Print this message.")
  private boolean _help = false;

  @Override
  public String getName() {
    return "VerifyClusterState";
  }

  @Override
  public boolean execute() throws Exception {
    if (_timeoutSec <= 0) {
      String message = "Error: timeoutMs must be greater than 0";
      LOGGER.error(message);
      System.exit(1);
    }

    boolean isStable = false;
    try {
      ClusterStateVerifier clusterStateVerifier = new ClusterStateVerifier(_zkAddress, _clusterName);
      isStable = clusterStateVerifier.verifyClusterState(_tableName, _timeoutSec);
    } catch (Exception ex) {
      String message = "Caught exception! The cluster is not stable.";
      LOGGER.error(message);
      System.exit(1);
    }

    if (!isStable) {
      String message = "The cluster is not stable.";
      LOGGER.error(message);
      System.exit(1);
    } else {
      LOGGER.info("All Tables are stable. The cluster is stable.");
    }
    return true;
  }

  @Override
  public String description() {
    return "Verify cluster's state after shutting down several nodes randomly.";
  }

  @Override
  public boolean getHelp() {
    return _help;
  }
}
