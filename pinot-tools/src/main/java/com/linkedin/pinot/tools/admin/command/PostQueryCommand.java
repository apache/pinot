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

public class PostQueryCommand implements Command {
  @Option(name="-brokerUrl", required=true, metaVar="<BrokerBaseApiUrl>")
  String _brokerUrl;

  @Option(name="-query", required=true, metaVar="<queryString>")
  String _query;

  public void init(String brokerUrl, String query) {
    _brokerUrl = brokerUrl;
    _query = query;
  }

  public boolean execute() throws Exception {
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
    System.out.println("Result: " + sb.toString());
    return true;
  }
}
