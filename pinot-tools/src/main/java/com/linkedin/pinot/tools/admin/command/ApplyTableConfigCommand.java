/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
import com.google.common.base.Splitter;
import com.linkedin.pinot.common.config.CombinedConfig;
import com.linkedin.pinot.common.config.CombinedConfigLoader;
import com.linkedin.pinot.common.config.Serializer;
import com.linkedin.pinot.controller.helix.ControllerRequestURLBuilder;
import com.linkedin.pinot.tools.Command;
import java.io.File;
import java.io.InputStream;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.kohsuke.args4j.Option;


/**
 * Command that applies a given table configuration to a cluster.
 */
public class ApplyTableConfigCommand extends AbstractBaseAdminCommand implements Command {

  @Option(name = "-controllerUrl", required = true, metaVar = "<String>", usage = "Controller URL")
  private String _controllerUrl;

  @Option(name = "-tableConfigFile", required = true, metaVar = "<string>", usage = "Table configuration file to use.")
  private String _tableConfigFile = null;

  @Option(name = "-profile", required = false, metaVar = "<string>", usage = "Comma separated list of configuration profiles to use.")
  private String _profileList = null;

  @Option(name = "-showComputedConfig", required = false, usage = "Only display the computed configuration, without attempting to upload it")
  private boolean _showComputedConfig = false;

  @Option(name = "-help", required = false, help = true, aliases = { "-h", "--h", "--help" },
      usage = "Print this message.")
  private boolean _help = false;

  @Override
  public boolean execute() throws Exception {
    // Split the profile list, if present
    String[] configurationProfiles;

    if (_profileList != null) {
      configurationProfiles = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(_profileList).toArray(new String[0]);
    } else {
      configurationProfiles = new String[0];
    }

    // Load the config
    CombinedConfig combinedConfig = CombinedConfigLoader.loadCombinedConfig(new File(_tableConfigFile), configurationProfiles);
    String tableName;
    if (combinedConfig.getOfflineTableConfig() != null) {
      tableName = combinedConfig.getOfflineTableConfig().getTableName();
    } else if (combinedConfig.getRealtimeTableConfig() != null) {
      tableName = combinedConfig.getRealtimeTableConfig().getTableName();
    } else {
      throw new RuntimeException("Table config does not contain a valid offline or realtime table definition.");
    }

    String computedConfig = Serializer.serializeToString(combinedConfig);

    if (_showComputedConfig) {
      System.out.println(computedConfig);
      return true;
    }

    // Build the upload URL
    ControllerRequestURLBuilder urlBuilder = ControllerRequestURLBuilder.baseUrl(_controllerUrl);
    String uploadUrl = urlBuilder.forNewUpdateTableConfig(tableName);
    String createUrl = urlBuilder.forNewTableCreate();

    // Check if the table already exists
    boolean tableExists;

    HttpClient client = HttpClients.createDefault();
    HttpResponse response = client.execute(new HttpGet(uploadUrl));
    int statusCode = response.getStatusLine().getStatusCode();
    if (statusCode == 404) {
      tableExists = false;
    } else if (statusCode / 100 == 2) {
      tableExists = true;
    } else {
      throw new RuntimeException("Unable to determine whether table " + tableName + " exists from HTTP status code " + statusCode);
    }

    // Post or put the table configuration depending on whether the table exists
    HttpEntityEnclosingRequestBase request;
    if (tableExists) {
      request = new HttpPut(uploadUrl);
      System.out.println("Table " + tableName + " exists, will update its configuration.");
    } else {
      request = new HttpPost(createUrl);
      System.out.println("Table " + tableName + " does not exist, creating.");
    }

    request.setEntity(new StringEntity(computedConfig, Charsets.UTF_8));
    response = client.execute(request);
    statusCode = response.getStatusLine().getStatusCode();

    if (statusCode / 100 == 2) {
      System.out.println("Successfully applied table configuration for table " + tableName);
      return true;
    } else {
      InputStream contentStream = response.getEntity().getContent();
      String content = IOUtils.toString(contentStream);
      System.out.println("Failed to apply table configuration for table " + tableName + ", received HTTP status code " + statusCode + " " + content);
      return false;
    }
  }

  @Override
  public String description() {
    return "Applies the given table configuration to a cluster.";
  }

  @Override
  public boolean getHelp() {
    return _help;
  }
}
