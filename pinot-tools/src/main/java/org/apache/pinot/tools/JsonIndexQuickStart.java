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
import java.io.File;
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
import static org.apache.pinot.tools.Quickstart.printStatus;


public class JsonIndexQuickStart extends QuickStartBase {

  public void execute()
      throws Exception {
    File quickstartTmpDir = new File(_tmpDir, String.valueOf(System.currentTimeMillis()));
    File baseDir = new File(quickstartTmpDir, "githubEvents");
    File dataDir = new File(quickstartTmpDir, "rawdata");
    Preconditions.checkState(dataDir.mkdirs());

    File schemaFile = new File(baseDir, "githubEvents_schema.json");
    File tableConfigFile = new File(baseDir, "githubEvents_offline_table_config.json");
    File ingestionJobSpecFile = new File(baseDir, "ingestionJobSpec.yaml");

    ClassLoader classLoader = JsonIndexQuickStart.class.getClassLoader();
    URL resource = classLoader.getResource("examples/batch/githubEvents/githubEvents_offline_table_config.json");
    Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, tableConfigFile);
    resource = classLoader.getResource("examples/batch/githubEvents/githubEvents_schema.json");
    Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, schemaFile);
    resource = classLoader.getResource("examples/batch/githubEvents/ingestionJobSpec.yaml");
    Preconditions.checkNotNull(resource);
    FileUtils.copyURLToFile(resource, ingestionJobSpecFile);

    QuickstartTableRequest request = new QuickstartTableRequest(baseDir.getAbsolutePath());
    final QuickstartRunner runner = new QuickstartRunner(Collections.singletonList(request), 1, 1, 1, dataDir);

    printStatus(Color.CYAN, "***** Starting Zookeeper, controller, broker and server *****");
    runner.startAll();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        printStatus(Color.GREEN, "***** Shutting down offline quick start *****");
        runner.stop();
        FileUtils.deleteDirectory(quickstartTmpDir);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }));
    printStatus(Color.CYAN, "***** Bootstrap githubEvents table *****");
    runner.bootstrapTable();

    printStatus(Color.CYAN, "***** Waiting for 5 seconds for the server to fetch the assigned segment *****");
    Thread.sleep(5000);

    printStatus(Color.YELLOW, "***** Offline json-index quickstart setup complete *****");

    String q1 =
        "select json_extract_scalar(repo, '$.name', 'STRING'), count(*) from githubEvents where json_match(actor, '\"$.login\"=''LombiqBot''') group by 1 order by 2 desc limit 10";
    printStatus(Color.YELLOW, "Most contributed repos by 'LombiqBot'");
    printStatus(Color.CYAN, "Query : " + q1);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q1)));

    printStatus(Color.GREEN, "***************************************************");
    printStatus(Color.GREEN, "You can always go to http://localhost:9000 to play around in the query console");
  }

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", "BATCH-JSON-INDEX"));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }
}
