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
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;

import org.json.JSONException;
import org.json.JSONObject;
import org.kohsuke.args4j.Option;

import com.linkedin.pinot.controller.helix.ControllerRequestURLBuilder;
import com.linkedin.pinot.controller.helix.ControllerRequestBuilderUtil;

/**
 * Class to implement CreateResource command.
 *
 * @author Mayank Shrivastava <mshrivastava@linkedin.com>
 */
public class CreateResourceCommand extends AbstractBaseCommand implements Command {
  @Option(name="-resourceName", required=true, metaVar="<string>", usage="Name of the resource to create.")
  private String _resourceName;

  @Option(name="-controllerAddress", required=true, metaVar="<http>", usage="Http address of Controller (with port).")
  private String _controllerAddress = null;

  @Option(name="-numInstances", required=false, metaVar = "<int>", usage="Number of instances.")
  int _numInstances=1;

  @Option(name="-numReplicas", required=false, metaVar = "<int>", usage="Number of replicas.")
  private int _numReplicas=1;

  @Option(name="-help", required=false, help=true, usage="Print this message.")
  private boolean _help = false;

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public String getName() {
    return "CreateResource";
  }

  @Override
  public String toString() {
    return ("CreateResource " + _resourceName + " -controllerAddress " + _controllerAddress +
        " -numInstances " + _numInstances + " -numReplicase " + _numReplicas);
  }

  @Override
  public void cleanup() {

  }

  public void init(String resourceName, String controllerAddress, int numInstances, int numReplicas) {
    _resourceName = resourceName;
    _controllerAddress = controllerAddress;
    _numInstances = numInstances;
    _numReplicas = numReplicas;
  }

  @Override
  public boolean execute() throws Exception {
    JSONObject payload =
        ControllerRequestBuilderUtil.buildCreateOfflineResourceJSON(_resourceName, _numInstances, _numReplicas);

    String res =
        sendPostRequest(ControllerRequestURLBuilder.baseUrl(_controllerAddress).forResourceCreate(),
            payload.toString().replaceAll("}$", ", \"metadata\":{}}"));

    String status = new JSONObject(res).getString("status");
    return status.equals("success");
  }

  public static String sendPostRequest(String urlString, String payload) throws UnsupportedEncodingException,
  IOException, JSONException {
    final URL url = new URL(urlString);
    final URLConnection conn = (HttpURLConnection) url.openConnection();

    conn.setDoOutput(true);
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
