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

import org.apache.pinot.tools.ClusterStateVerifier;
import org.apache.pinot.tools.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


@CommandLine.Command(name = "VerifyClusterState", description = "Verify cluster's state after shutting down several "
                                                                + "nodes randomly.", mixinStandardHelpOptions = true)
public class VerifyClusterStateCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(VerifyClusterStateCommand.class);

  @CommandLine.Option(names = {"-zkAddress"}, required = false, description = "HTTP address of Zookeeper.")
  private String _zkAddress = DEFAULT_ZK_ADDRESS;

  @CommandLine.Option(names = {"-clusterName"}, required = false, description = "Pinot cluster name.")
  private String _clusterName = "PinotCluster";

  @CommandLine.Option(names = {"-tableName"}, required = false,
      description = "Table name to check the state (e.g. myTable_OFFLINE).")
  private String _tableName;

  @CommandLine.Option(names = {"-timeoutSec"}, required = false, description = "Maximum timeout in second.")
  private long _timeoutSec = 300L;

  @Override
  public String getName() {
    return "VerifyClusterState";
  }

  @Override
  public boolean execute()
      throws Exception {
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
}
