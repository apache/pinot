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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.admin.command.QuickstartRunner;

import static org.apache.pinot.tools.Quickstart.prettyPrintResponse;


public class MultistageEngineQuickStart extends QuickStartBase {

  @Override
  public List<String> types() {
    return Collections.singletonList("MULTI_STAGE");
  }

  @Override
  public Map<String, Object> getConfigOverrides() {
    Map<String, Object> overrides = new HashMap<>(super.getConfigOverrides());
    overrides.put("pinot.multistage.engine.enabled", "true");
    overrides.put("pinot.server.instance.currentDataTableVersion", 4);
    return overrides;
  }

  public void execute()
      throws Exception {
    File quickstartTmpDir = new File(_dataDir, String.valueOf(System.currentTimeMillis()));

    // Baseball stat table
    File baseBallStatsBaseDir = new File(quickstartTmpDir, "baseballStats");
    File schemaFile = new File(baseBallStatsBaseDir, "baseballStats_schema.json");
    File tableConfigFile = new File(baseBallStatsBaseDir, "baseballStats_offline_table_config.json");
    File ingestionJobSpecFile = new File(baseBallStatsBaseDir, "ingestionJobSpec.yaml");
    ClassLoader classLoader = Quickstart.class.getClassLoader();
    URL resource = classLoader.getResource("examples/batch/baseballStats/baseballStats_schema.json");
    Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, schemaFile);
    resource = classLoader.getResource("examples/batch/baseballStats/baseballStats_offline_table_config.json");
    Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, tableConfigFile);
    resource = classLoader.getResource("examples/batch/baseballStats/ingestionJobSpec.yaml");
    Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, ingestionJobSpecFile);
    QuickstartTableRequest request = new QuickstartTableRequest(baseBallStatsBaseDir.getAbsolutePath());

    File tempDir = new File(quickstartTmpDir, "tmp");
    FileUtils.forceMkdir(tempDir);
    QuickstartRunner runner =
        new QuickstartRunner(Lists.newArrayList(request), 1, 1, 1, 1, tempDir, getConfigOverrides());

    printStatus(Quickstart.Color.CYAN, "***** Starting Zookeeper, controller, broker and server *****");
    runner.startAll();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        printStatus(Quickstart.Color.GREEN, "***** Shutting down offline quick start *****");
        runner.stop();
        FileUtils.deleteDirectory(quickstartTmpDir);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }));
    printStatus(Quickstart.Color.CYAN, "***** Bootstrap baseballStats table *****");
    runner.bootstrapTable();

    waitForBootstrapToComplete(null);

    Map<String, String> queryOptions = Collections.singletonMap(
        CommonConstants.Broker.Request.QueryOptionKey.USE_MULTISTAGE_ENGINE, "true");

    printStatus(Quickstart.Color.YELLOW, "***** Multi-stage engine quickstart setup complete *****");
    String q1 = "SELECT count(*) FROM baseballStats_OFFLINE limit 1";
    printStatus(Quickstart.Color.YELLOW, "Total number of documents in the table");
    printStatus(Quickstart.Color.CYAN, "Query : " + q1);
    printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(q1, queryOptions)));
    printStatus(Quickstart.Color.GREEN, "***************************************************");

    String q2 = "SELECT a.playerID, a.runs, a.yearID, b.runs, b.yearID"
        + " FROM baseballStats_OFFLINE AS a JOIN baseballStats_OFFLINE AS b ON a.playerID = b.playerID"
        + " WHERE a.runs > 160 AND b.runs < 2";
    printStatus(Quickstart.Color.YELLOW, "Correlate the same player(s) with more than 160-run some year(s) and"
        + " with less than 2-run some other year(s)");
    printStatus(Quickstart.Color.CYAN, "Query : " + q2);
    printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(q2, queryOptions)));
    printStatus(Quickstart.Color.GREEN, "***************************************************");

    printStatus(Quickstart.Color.GREEN, "***************************************************");
    printStatus(Quickstart.Color.YELLOW, "Example query run completed.");
    printStatus(Quickstart.Color.GREEN, "***************************************************");
    printStatus(Quickstart.Color.YELLOW, "Please use broker port for executing multistage queries.");
    printStatus(Quickstart.Color.GREEN, "***************************************************");
  }

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", "MULTI_STAGE"));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }
}
