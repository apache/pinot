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
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.auth.NullAuthProvider;
import org.apache.pinot.common.auth.StaticTokenAuthProvider;
import org.apache.pinot.common.auth.UrlAuthProvider;
import org.apache.pinot.core.auth.BasicAuthUtils;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.tools.AbstractBaseCommand;
import org.apache.pinot.tools.utils.PinotConfigUtils;


/**
 * Super class for all the commands.
 * Implements the common getDescription method.
 *
 */
public class AbstractBaseAdminCommand extends AbstractBaseCommand {
  static final String DEFAULT_CONTROLLER_PORT = "9000";
  static final String URI_TABLES_PATH = "/tables/";

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
    return sendRequest(requestMethod, urlString, payload, Collections.emptyList());
  }

  public static String sendRequest(String requestMethod, String urlString, String payload, List<Header> headers)
      throws IOException {
    final URL url = new URL(urlString);
    final HttpURLConnection conn = (HttpURLConnection) url.openConnection();

    headers.forEach(header -> conn.setRequestProperty(header.getName(), header.getValue()));
    conn.setRequestMethod(requestMethod);
    conn.setDoOutput(true);
    if (payload != null) {
      try (final BufferedWriter writer = new BufferedWriter(
          new OutputStreamWriter(conn.getOutputStream(), StandardCharsets.UTF_8))) {
        writer.write(payload, 0, payload.length());
        writer.flush();
      }
    }

    try {
      return readInputStream(conn.getInputStream());
    } catch (Exception e) {
      if (conn.getErrorStream() != null) {
        return readInputStream(conn.getErrorStream());
      }
      throw new IOException("Request failed due to connection error", e);
    }
  }

  private static String readInputStream(InputStream inputStream)
      throws IOException {
    final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
    final StringBuilder sb = new StringBuilder();
    String line;
    while ((line = reader.readLine()) != null) {
      sb.append(line);
    }
    return sb.toString();
  }

  protected void savePID(String fileName)
      throws IOException {
    FileWriter pidFile = new FileWriter(fileName);
    pidFile.write(getPID());
    pidFile.close();
  }

  Map<String, Object> readConfigFromFile(String configFileName)
      throws ConfigurationException {
    return PinotConfigUtils.readConfigFromFile(configFileName);
  }

  /**
   * Generate an (optional) HTTP Authorization header given an auth config
   *
   * @param authProvider auth provider
   * @return list of headers
   */
  static List<Header> makeAuthHeaders(AuthProvider authProvider) {
    return AuthProviderUtils.toRequestHeaders(authProvider);
  }

  /**
   * Generate auth token from pass-thru token or generate basic auth from user/password pair
   *
   * @param provider optional provider
   * @param tokenUrl optional token url
   * @param authToken optional pass-thru token
   * @param user optional username
   * @param password optional password
   * @return auth provider, or NullauthProvider if neither pass-thru token nor user info available
   */
  @Nullable
  static AuthProvider makeAuthProvider(AuthProvider provider, String tokenUrl, String authToken, String user,
      String password) {
    if (provider != null) {
      return provider;
    }

    if (StringUtils.isNotBlank(tokenUrl)) {
      return new UrlAuthProvider(tokenUrl);
    }

    if (StringUtils.isNotBlank(authToken)) {
      return new StaticTokenAuthProvider(authToken);
    }

    if (StringUtils.isNotBlank(user)) {
      return new StaticTokenAuthProvider(BasicAuthUtils.toBasicAuthToken(user, password));
    }

    return new NullAuthProvider();
  }
}
