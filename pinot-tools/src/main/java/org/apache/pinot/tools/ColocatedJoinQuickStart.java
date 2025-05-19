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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.admin.command.QuickstartRunner;


public class ColocatedJoinQuickStart extends MultistageEngineQuickStart {
  private static final String QUICKSTART_IDENTIFIER = "COLOCATED_JOIN";
  private static final String[] COLOCATED_JOIN_DIRECTORIES = new String[]{
      "examples/batch/colocated/userAttributes", "examples/batch/colocated/userGroups",
      "examples/batch/colocated/userFactEvents"
  };

  private static final String EXPLICIT = "SELECT COUNT(*) "
      + "FROM userAttributes /*+ tableOptions(partition_key='userUUID', partition_size='2') */ ua "
      + "JOIN userGroups /*+ tableOptions(partition_key='userUUID', partition_size='2') */ ug "
      + "ON ua.userUUID = ug.userUUID";

  private static final String IMPLICIT = "SET inferPartitionHint = true; "
      + "SELECT COUNT(*) "
      + "FROM userAttributes ua "
      + "JOIN userGroups ug "
      + "ON ua.userUUID = ug.userUUID";

  private static final String PARTITION_PARALLELISM = "SET inferPartitionHint = true; "
      + "SELECT COUNT(*) "
      + "FROM userAttributes /*+ tableOptions(partition_parallelism='2') */ ua "
      + "JOIN userGroups /*+ tableOptions(partition_parallelism='2') */ ug "
      + "ON ua.userUUID = ug.userUUID";

  private static final String MULTIPLE_PARTITIONS_PER_WORKER = "SET inferPartitionHint = true; "
      + "SELECT COUNT(*) "
      + "FROM userAttributes /*+ tableOptions(partition_size='2') */ ua "
      + "JOIN userGroups /*+ tableOptions(partition_size='2') */ ug "
      + "ON ua.userUUID = ug.userUUID";

  private static final String MULTIPLE_PARTITIONS_PER_WORKER_AND_PARALLELISM = "SET inferPartitionHint = true; "
      + "SELECT COUNT(*) "
      + "FROM userAttributes /*+ tableOptions(partition_size='2', partition_parallelism='2') */ ua "
      + "JOIN userGroups /*+ tableOptions(partition_size='2', partition_parallelism='2') */ ug "
      + "ON ua.userUUID = ug.userUUID";

  // This one falls back to hash exchange with data shuffle
  private static final String WORKERS_NOT_MATCH = "SET inferPartitionHint = true; "
      + "SELECT COUNT(*) "
      + "FROM userAttributes /*+ tableOptions(partition_size='2') */ ua "
      + "JOIN userGroups ug "
      + "ON ua.userUUID = ug.userUUID";

  // Physical optimizer based queries.
  private static final String DIFFERENT_PARTITION_BUT_COLOCATED_QUERY = "SET usePhysicalOptimizer = true; "
      + "SELECT COUNT(*) "
      + "  FROM userFactEvents WHERE userUUID NOT IN ("
      + "    SELECT userUUID FROM userGroups WHERE groupUUID = 'group-1'"
      + "  )";

  @Override
  public List<String> types() {
    return Collections.singletonList(QUICKSTART_IDENTIFIER);
  }

  @Override
  public Map<String, Object> getConfigOverrides() {
    Map<String, Object> overrides = new HashMap<>(super.getConfigOverrides());
    // This is actually not required anymore, but we are keeping it as reference
    overrides.put(CommonConstants.Broker.CONFIG_OF_ENABLE_PARTITION_METADATA_MANAGER, "true");
    return overrides;
  }

  @Override
  public String[] getDefaultBatchTableDirectories() {
    return COLOCATED_JOIN_DIRECTORIES;
  }

  @Override
  protected int getNumQuickstartRunnerServers() {
    return 4;
  }

  @Override
  public void runSampleQueries(QuickstartRunner runner)
      throws Exception {
    printStatus(Color.YELLOW, "***** Colocated join quickstart setup complete *****");

    printStatus(Color.YELLOW, "***** With explicit hints *****");
    printStatus(Color.CYAN, "Query : " + EXPLICIT);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(EXPLICIT, OPTIONS_TO_USE_MSE)));
    printStatus(Color.GREEN, "***************************************************");

    printStatus(Color.YELLOW, "***** With implicit hints *****");
    printStatus(Color.CYAN, "Query : " + IMPLICIT);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(IMPLICIT, OPTIONS_TO_USE_MSE)));
    printStatus(Color.GREEN, "***************************************************");

    printStatus(Color.YELLOW, "***** With partition parallelism *****");
    printStatus(Color.CYAN, "Query : " + PARTITION_PARALLELISM);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(PARTITION_PARALLELISM, OPTIONS_TO_USE_MSE)));
    printStatus(Color.GREEN, "***************************************************");

    printStatus(Color.YELLOW, "***** With multiple partitions per worker *****");
    printStatus(Color.CYAN, "Query : " + MULTIPLE_PARTITIONS_PER_WORKER);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(MULTIPLE_PARTITIONS_PER_WORKER, OPTIONS_TO_USE_MSE)));
    printStatus(Color.GREEN, "***************************************************");

    printStatus(Color.YELLOW, "***** With multiple partitions per worker and parallelism *****");
    printStatus(Color.CYAN, "Query : " + MULTIPLE_PARTITIONS_PER_WORKER_AND_PARALLELISM);
    printStatus(Color.YELLOW,
        prettyPrintResponse(runner.runQuery(MULTIPLE_PARTITIONS_PER_WORKER_AND_PARALLELISM, OPTIONS_TO_USE_MSE)));
    printStatus(Color.GREEN, "***************************************************");

    printStatus(Color.YELLOW, "***** With workers not matching (not using colocated join) *****");
    printStatus(Color.CYAN, "Query : " + WORKERS_NOT_MATCH);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(WORKERS_NOT_MATCH, OPTIONS_TO_USE_MSE)));
    printStatus(Color.GREEN, "***************************************************");

    printStatus(Color.YELLOW, "***** With workers not matching (not using colocated join) *****");
    printStatus(Color.CYAN, "Query : " + DIFFERENT_PARTITION_BUT_COLOCATED_QUERY);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(DIFFERENT_PARTITION_BUT_COLOCATED_QUERY,
        OPTIONS_TO_USE_MSE)));
    printStatus(Color.GREEN, "***************************************************");

    printStatus(Color.GREEN, "***************************************************");
    printStatus(Color.YELLOW, "Example query run completed.");
    printStatus(Color.GREEN, "***************************************************");
  }

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", QUICKSTART_IDENTIFIER));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }
}
