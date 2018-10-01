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

import com.google.common.base.Charsets;
import com.linkedin.pinot.controller.helix.ControllerRequestURLBuilder;
import com.linkedin.pinot.tools.Command;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONArray;
import org.json.JSONObject;
import org.kohsuke.args4j.Option;


/**
 * Command to back up table configurations.
 */
public class BackupTableConfigsCommand extends AbstractBaseAdminCommand implements Command {
  @Option(name = "-controllerUrl", required = true, metaVar = "<String>", usage = "Controller URL")
  private String _controllerUrl;

  @Option(name = "-tableName", required = false, usage = "Table name to back up. Optional, if not specified, defaults to backing up all tables")
  private String _tableName = null;

  @Option(name = "-outputDir", required = false, usage = "Directory in which table configs are placed. Optional, defaults to the current working directory. Directory is created if it does not exist.")
  private String _outputDir = ".";

  @Option(name = "-help", required = false, help = true, aliases = { "-h", "--h", "--help" },
      usage = "Print this message.")
  private boolean _help = false;

  @Override
  public boolean execute() throws Exception {
    // Create the output directory if it does not exist
    File outputDirectory = new File(_outputDir);
    if (!outputDirectory.exists()) {
      outputDirectory.mkdirs();
    }

    // Populate the list of tables to fetch, if applicable
    ControllerRequestURLBuilder requestURLBuilder = ControllerRequestURLBuilder.baseUrl(_controllerUrl);
    HttpClient client = HttpClients.createDefault();
    List<String> tablesToFetch;
    if (_tableName != null) {
      tablesToFetch = Collections.singletonList(_tableName);
    } else {
      HttpResponse response = client.execute(new HttpGet(requestURLBuilder.forTableList()));
      int statusCode = response.getStatusLine().getStatusCode();
      String responseEntity = IOUtils.toString(response.getEntity().getContent());
      if (statusCode / 100 == 2) {
        JSONArray tableNameArray = new JSONObject(responseEntity).getJSONArray("tables");
        tablesToFetch = new ArrayList<>(tableNameArray.length());
        for (int i = 0; i < tableNameArray.length(); i++) {
          tablesToFetch.add(tableNameArray.getString(i));
        }
      } else {
        throw new RuntimeException("Failed to fetch the table list, got HTTP status " + statusCode + ": " + responseEntity);
      }
    }

    System.out.println("Will fetch " + tablesToFetch.size() + " table(s)");

    boolean allTablesSuccessful = true;
    for (String tableName : tablesToFetch) {
      HttpResponse response = client.execute(new HttpGet(requestURLBuilder.forNewUpdateTableConfig(tableName)));
      int statusCode = response.getStatusLine().getStatusCode();
      String responseEntity = IOUtils.toString(response.getEntity().getContent());
      if (statusCode / 100 == 2) {
        try {
          File outputFile = new File(outputDirectory, tableName + ".conf");
          FileUtils.writeStringToFile(outputFile, responseEntity, Charsets.UTF_8);
          System.out.println("Saved table configuration for table " +  tableName);
        } catch (IOException e) {
          System.out.println("Failed to write table configuration for table " + tableName + ", got exception.");
          e.printStackTrace();
          allTablesSuccessful = false;
        }
      } else {
        System.out.println("Failed to fetch table configuration for table " + tableName + ", got HTTP status " + statusCode + ": " + responseEntity);
        allTablesSuccessful = false;
      }
    }

    if (allTablesSuccessful) {
      System.out.println("Successfully backed up " + tablesToFetch.size() + " tables");
    } else {
      System.out.println("Some tables failed to back up, see messages above for details.");
    }

    return allTablesSuccessful;
  }

  @Override
  public String description() {
    return "Backs up table configurations.";
  }

  @Override
  public boolean getHelp() {
    return _help;
  }
}
