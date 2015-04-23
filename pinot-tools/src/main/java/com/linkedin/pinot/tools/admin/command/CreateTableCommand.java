/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;

import org.json.JSONException;
import org.json.JSONObject;
import org.kohsuke.args4j.Option;

import com.linkedin.pinot.controller.helix.ControllerRequestBuilderUtil;
import com.linkedin.pinot.controller.helix.ControllerRequestURLBuilder;

/**
* Class to implement the CreateTable command.
*
* @author Mayank Shrivastava <mshrivastava@linkedin.com>
*/
public class CreateTableCommand extends AbstractBaseCommand implements Command {
  @Option(name="-tableName", required=true, metaVar="<Name of table to create")
  String _tableName;

  @Option(name="-resourceName", required=true, metaVar="<Name of resource for the table.")
  String _resourceName;

  @Option(name="-controllerAddress", required=true, metaVar="<http>", usage="Http address of Controller (including port.")
  private String _controllerAddress = null;

  @Option(name="-help", required=false, help=true, usage="Print this message.")
  private boolean _help = false;

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public String getName() {
    return "CreateTable";
  }

  @Override
  public String toString() {
    return ("CreateTable " + " -tableName " + _tableName + " -resourceName " + _resourceName +
        " -controllerAddress " + _controllerAddress);
  }
  public void init(String tableName, String resourceName, String controllerAddress) {
    _tableName = tableName;
    _resourceName = resourceName;
    _controllerAddress = controllerAddress;
  }

  @Override
  public void cleanup() {

  }

  @Override
  public boolean execute() throws JSONException, IOException {
    JSONObject payload =
        ControllerRequestBuilderUtil.createOfflineClusterAddTableToResource(_resourceName, _tableName).toJSON();

    String res =
        sendPutRequest(ControllerRequestURLBuilder.baseUrl(_controllerAddress).forResourceCreate(),
            payload.toString());

    String status = new JSONObject(res).getString("status");
    return (status == "success");
  }

  public static String sendPutRequest(String urlString, String payload) throws IOException {
    final URL url = new URL(urlString);
    final HttpURLConnection conn = (HttpURLConnection) url.openConnection();

    conn.setDoOutput(true);
    conn.setRequestMethod("PUT");
    final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(conn.getOutputStream(), "UTF-8"));

    writer.write(payload, 0, payload.length());
    writer.flush();

    final BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
    final StringBuilder sb = new StringBuilder();

    String line = null;
    while ((line = reader.readLine()) != null) {
      sb.append(line);
    }

    return sb.toString();
  }
}
