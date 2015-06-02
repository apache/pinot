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
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;

import org.json.JSONObject;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PostQueryCommand extends AbstractBaseCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(PostQueryCommand.class.getName());

  @Option(name="-brokerPort", required=true, metaVar="<int>", usage="http port for broker.")
  private String _brokerPort;

  @Option(name="-query", required=true, metaVar="<string>", usage="Query string to perform.")
  private String _query;

  @Option(name="-help", required=false, help=true, usage="Print this message.")
  private boolean _help = false;

  public boolean getHelp() {
    return _help;
  }

  @Override
  public String getName() {
    return "PostQuery";
  }

  private String _brokerUrl = "http://localhost:" + _brokerPort;

  @Override
  public String toString() {
    return ("PostQueryCommand -brokerUrl " + _brokerUrl + " -query " + _query);
  }

  @Override
  public void cleanup() {

  }

  public PostQueryCommand setBrokerUrl(String brokerUrl) {
    _brokerUrl = brokerUrl;
    return this;
  }

  public PostQueryCommand setQuery(String query) {
    _query = query;
    return this;
  }

  public String run() throws Exception {
    final JSONObject json = new JSONObject();
    json.put("pql",  _query);

    final URLConnection conn = new URL(_brokerUrl + "/query").openConnection();
    conn.setDoOutput(true);

    final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(conn.getOutputStream(), "UTF-8"));
    String request = json.toString();

    writer.write(request, 0, request.length());
    writer.flush();

    final BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
    final StringBuilder sb = new StringBuilder();

    String line = null;
    while ((line = reader.readLine()) != null) {
      sb.append(line);
    }

    return sb.toString();
  }

  public boolean execute() throws Exception {
    String result = run();
    LOGGER.info("Result: " + result);
    return true;
  }
}
