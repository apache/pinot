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
package org.apache.pinot.tools.admin.command;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.pinot.tools.AbstractBaseCommand;


/**
 * Super class for all the commands.
 * Implements the common printUsage method.
 *
 */
public class AbstractBaseAdminCommand extends AbstractBaseCommand {
  static final String DEFAULT_CONTROLLER_PORT = "9000";
  static final String URI_TABLES_PATH = "/tables/";

  static final String TMP_DIR = System.getProperty("java.io.tmpdir") + File.separator;

  public AbstractBaseAdminCommand(boolean addShutdownHook) {
    super(addShutdownHook);
  }

  public AbstractBaseAdminCommand() {
    super(true);
  }

  public static int getPID() {
    String processName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
    return Integer.parseInt(processName.split("@")[0]);
  }

  protected void savePID(String fileName)
      throws IOException {
    FileWriter pidFile = new FileWriter(fileName);
    pidFile.write(getPID());
    pidFile.close();
  }

  public static String sendPostRequest(String urlString, String payload)
      throws IOException {
    return sendRequest("POST", urlString, payload);
  }

  public static String sendDeleteRequest(String urlString, String payload)
      throws IOException {
    return sendRequest("DELETE", urlString, payload);
  }

  public static String sendRequest(String requestMethod, String urlString, String payload)
      throws IOException {
    final URL url = new URL(urlString);
    final HttpURLConnection conn = (HttpURLConnection) url.openConnection();

    conn.setDoOutput(true);
    conn.setRequestMethod(requestMethod);
    if (payload != null) {
      final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(conn.getOutputStream(), "UTF-8"));
      writer.write(payload, 0, payload.length());
      writer.flush();
    }

    try {
      return readInputStream(conn.getInputStream());
    } catch (Exception e) {
      return readInputStream(conn.getErrorStream());
    }
  }

  private static String readInputStream(InputStream inputStream) throws IOException {
    final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
    final StringBuilder sb = new StringBuilder();
    String line;
    while ((line = reader.readLine()) != null) {
      sb.append(line);
    }
    return sb.toString();
  }

  PropertiesConfiguration readConfigFromFile(String configFileName)
      throws ConfigurationException {
    if (configFileName != null) {
      File configFile = new File(configFileName);

      if (configFile.exists()) {
        return new PropertiesConfiguration(configFile);
      } else {
        return null;
      }
    } else {
      return null;
    }
  }
}
