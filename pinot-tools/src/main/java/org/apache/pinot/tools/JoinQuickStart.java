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
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.tools.admin.command.QuickstartRunner;

import java.io.File;
import java.net.URL;

import static org.apache.pinot.tools.Quickstart.prettyPrintResponse;
import static org.apache.pinot.tools.Quickstart.printStatus;

public class JoinQuickStart
{
    public static void main(String[] args)
            throws Exception {
        PluginManager.get().init();
        new JoinQuickStart().execute();
    }

    private void execute() throws Exception  {
        File quickstartTmpDir = new File(FileUtils.getTempDirectory(), String.valueOf(System.currentTimeMillis()));

        // Baseball stat table
        File baseBallStatsBaseDir = new File(quickstartTmpDir, "baseballStats");
        File baseBallStatsDataDir = new File(baseBallStatsBaseDir, "rawdata");
        Preconditions.checkState(baseBallStatsDataDir.mkdirs());
        File schemaFile = new File(baseBallStatsBaseDir, "baseballStats_schema.json");
        File tableConfigFile = new File(baseBallStatsBaseDir, "baseballStats_offline_table_config.json");
        File ingestionJobSpecFile = new File(baseBallStatsBaseDir, "ingestionJobSpec.yaml");
        File dataFile = new File(baseBallStatsDataDir, "baseballStats_data.csv");
        ClassLoader classLoader = Quickstart.class.getClassLoader();
        URL resource = classLoader.getResource("examples/batch/baseballStats/baseballStats_schema.json");
        com.google.common.base.Preconditions.checkNotNull(resource);
        FileUtils.copyURLToFile(resource, schemaFile);
        resource = classLoader.getResource("examples/batch/baseballStats/rawdata/baseballStats_data.csv");
        com.google.common.base.Preconditions.checkNotNull(resource);
        FileUtils.copyURLToFile(resource, dataFile);
        resource = classLoader.getResource("examples/batch/baseballStats/ingestionJobSpec.yaml");
        com.google.common.base.Preconditions.checkNotNull(resource);
        FileUtils.copyURLToFile(resource, ingestionJobSpecFile);
        resource = classLoader.getResource("examples/batch/baseballStats/baseballStats_offline_table_config.json");
        com.google.common.base.Preconditions.checkNotNull(resource);
        FileUtils.copyURLToFile(resource, tableConfigFile);
        QuickstartTableRequest request = new QuickstartTableRequest(baseBallStatsBaseDir.getAbsolutePath());

        // Baseball teams dim table
        File dimBaseballTeamsBaseDir = new File(quickstartTmpDir, "dimBaseballTeams");
        schemaFile = new File(dimBaseballTeamsBaseDir, "dimBaseballTeams_schema.json");
        tableConfigFile = new File(dimBaseballTeamsBaseDir, "dimBaseballTeams_offline_table_config.json");
        ingestionJobSpecFile = new File(dimBaseballTeamsBaseDir, "ingestionJobSpec.yaml");
        dataFile = new File(dimBaseballTeamsBaseDir, "dimBaseballTeams_data.csv");
        classLoader = Quickstart.class.getClassLoader();
        resource = classLoader.getResource("examples/batch/dimBaseballTeams/dimBaseballTeams_schema.json");
        com.google.common.base.Preconditions.checkNotNull(resource);
        FileUtils.copyURLToFile(resource, schemaFile);
        resource = classLoader.getResource("examples/batch/dimBaseballTeams/rawdata/dimBaseballTeams_data.csv");
        com.google.common.base.Preconditions.checkNotNull(resource);
        FileUtils.copyURLToFile(resource, dataFile);
        resource = classLoader.getResource("examples/batch/dimBaseballTeams/ingestionJobSpec.yaml");
        com.google.common.base.Preconditions.checkNotNull(resource);
        FileUtils.copyURLToFile(resource, ingestionJobSpecFile);
        resource = classLoader.getResource("examples/batch/dimBaseballTeams/dimBaseballTeams_offline_table_config.json");
        com.google.common.base.Preconditions.checkNotNull(resource);
        FileUtils.copyURLToFile(resource, tableConfigFile);
        QuickstartTableRequest dimTableRequest = new QuickstartTableRequest(dimBaseballTeamsBaseDir.getAbsolutePath());

        final QuickstartRunner runner = new QuickstartRunner(Lists.newArrayList(request, dimTableRequest), 3, 1, 1, baseBallStatsDataDir);

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

        printStatus(Quickstart.Color.CYAN, "***** Waiting for 5 seconds for the server to fetch the assigned segment *****");
        Thread.sleep(5000);

        printStatus(Quickstart.Color.YELLOW, "***** Offline quickstart setup complete *****");

        String q1 = "select count(*) from baseballStats limit 1";
        printStatus(Quickstart.Color.YELLOW, "Total number of documents in the table");
        printStatus(Quickstart.Color.CYAN, "Query : " + q1);
        printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(q1)));
        printStatus(Quickstart.Color.GREEN, "***************************************************");

        String q2 = "select count(*) from dimBaseballTeams limit 1";
        printStatus(Quickstart.Color.YELLOW, "Baseball Teams");
        printStatus(Quickstart.Color.CYAN, "Query : " + q2);
        printStatus(Quickstart.Color.YELLOW, prettyPrintResponse(runner.runQuery(q2)));
        printStatus(Quickstart.Color.GREEN, "***************************************************");

        printStatus(Quickstart.Color.GREEN, "You can always go to http://localhost:9000 to play around in the query console");

    }
}
