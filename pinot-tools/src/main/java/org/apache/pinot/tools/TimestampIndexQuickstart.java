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
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.tools.Quickstart.Color;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.admin.command.QuickstartRunner;

import static org.apache.pinot.tools.Quickstart.prettyPrintResponse;


public class TimestampIndexQuickstart extends QuickStartBase {
  @Override
  public List<String> types() {
    return Collections.singletonList("TIMESTAMP");
  }

  private File _schemaFile;
  private File _ingestionJobSpecFile;

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", "TIMESTAMP"));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }

  private QuickstartTableRequest prepareTableRequest(File baseDir)
      throws IOException {
    _schemaFile = new File(baseDir, "airlineStats_schema.json");
    _ingestionJobSpecFile = new File(baseDir, "ingestionJobSpec.yaml");
    File tableConfigFile = new File(baseDir, "airlineStats_offline_table_config.json");

    ClassLoader classLoader = Quickstart.class.getClassLoader();
    URL resource = classLoader.getResource("examples/batch/airlineStats/airlineStats_schema.json");
    Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, _schemaFile);
    resource = classLoader.getResource("examples/batch/airlineStats/ingestionJobSpec.yaml");
    Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, _ingestionJobSpecFile);
    resource = classLoader.getResource("examples/batch/airlineStats/airlineStats_offline_table_config.json");
    Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, tableConfigFile);
    return new QuickstartTableRequest(baseDir.getAbsolutePath());
  }

  public void execute()
      throws Exception {
    File quickstartTmpDir = new File(_dataDir, String.valueOf(System.currentTimeMillis()));
    File baseDir = new File(quickstartTmpDir, "airlineStats");
    File dataDir = new File(baseDir, "data");
    Preconditions.checkState(dataDir.mkdirs());
    QuickstartTableRequest bootstrapTableRequest = prepareTableRequest(baseDir);
    final QuickstartRunner runner =
        new QuickstartRunner(Lists.newArrayList(bootstrapTableRequest), 1, 1, 1, 0, dataDir, getConfigOverrides());
    printStatus(Color.YELLOW, "***** Starting Zookeeper, 1 servers, 1 brokers and 1 controller *****");
    runner.startAll();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        printStatus(Color.GREEN, "***** Shutting down timestamp quick start *****");
        runner.stop();
        FileUtils.deleteDirectory(quickstartTmpDir);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }));
    printStatus(Color.YELLOW, "***** Bootstrap airlineStats offline and realtime table *****");
    runner.bootstrapTable();

    printStatus(Color.YELLOW, "***** Pinot Timestamp with timestamp table setup is complete *****");
    printStatus(Color.YELLOW, "***** Sequence of operations *****");
    printStatus(Color.YELLOW, "*****    1. Started 1 controller instance where tenant creation is enabled *****");
    printStatus(Color.YELLOW, "*****    2. Started 1 servers and 1 brokers *****");
    printStatus(Color.YELLOW, "*****    3. Created a server tenant with 1 offline and 1 realtime instance *****");
    printStatus(Color.YELLOW, "*****    4. Created a broker tenant with 2 instances *****");
    printStatus(Color.YELLOW, "*****    5. Added a schema *****");
    printStatus(Color.YELLOW,
        "*****    6. Created an offline and a realtime table with the tenant names created above *****");
    printStatus(Color.YELLOW, "*****    7. Built and pushed an offline segment *****");
    printStatus(Color.YELLOW,
        "*****    8. Started publishing a Kafka stream for the realtime instance to start consuming *****");
    printStatus(Color.YELLOW, "*****    9. Sleep 5 Seconds to wait for all components brought up *****");
    Thread.sleep(5000);

    String q1 = "select ts, $ts$DAY, $ts$WEEK, $ts$MONTH from airlineStats limit 1";
    printStatus(Color.YELLOW, "Pick one row with timestamp and different granularity using generated column name ");
    printStatus(Color.CYAN, "Query : " + q1);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q1)));
    printStatus(Color.GREEN, "***************************************************");

    String q2 =
        "select ts, dateTrunc('DAY', ts), dateTrunc('WEEK', ts), dateTrunc('MONTH', ts) from airlineStats limit 1";
    printStatus(Color.YELLOW, "Pick one row with timestamp and different granularity using dateTrunc function");
    printStatus(Color.CYAN, "Query : " + q2);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q2)));
    printStatus(Color.GREEN, "***************************************************");

    String q3 =
        "select count(*), toTimestamp(dateTrunc('WEEK', ts)) as tsWeek from airlineStats GROUP BY tsWeek limit 1";
    printStatus(Color.YELLOW, "Count events in week basis ");
    printStatus(Color.CYAN, "Query : " + q3);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q3)));
    printStatus(Color.GREEN, "***************************************************");
    printStatus(Color.GREEN, "You can always go to http://localhost:9000 to play around in the query console");
  }
}
