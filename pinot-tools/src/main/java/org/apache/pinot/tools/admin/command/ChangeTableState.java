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

import java.net.URI;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.tools.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


@CommandLine.Command(name = "ChangeTableState")
public class ChangeTableState extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(ChangeTableState.class);

  @CommandLine.Option(names = {"-controllerHost"}, required = false, description = "host name for controller")
  private String _controllerHost;

  @CommandLine.Option(names = {"-controllerPort"}, required = false, description = "Port number for controller.")
  private String _controllerPort = DEFAULT_CONTROLLER_PORT;

  @CommandLine.Option(names = {"-controllerProtocol"}, required = false, description = "protocol for controller.")
  private String _controllerProtocol = CommonConstants.HTTP_PROTOCOL;

  @CommandLine.Option(names = {"-tableName"}, required = true, description = "Table name to disable")
  private String _tableName;

  @CommandLine.Option(names = {"-state"}, required = true, description = "Change Table State(enable|disable|drop)")
  private String _state;

  @CommandLine.Option(names = {"-user"}, required = false, description = "Username for basic auth.")
  private String _user;

  @CommandLine.Option(names = {"-password"}, required = false, description = "Password for basic auth.")
  private String _password;

  @CommandLine.Option(names = {"-authToken"}, required = false, description = "Http auth token.")
  private String _authToken;

  @CommandLine.Option(names = {"-authTokenUrl"}, required = false, description = "Http auth token url.")
  private String _authTokenUrl;

  @CommandLine.Option(names = {"-help", "-h", "--h", "--help"}, required = false, help = true,
      description = "Print this message.")
  private boolean _help = false;

  private AuthProvider _authProvider;

  public ChangeTableState setAuthProvider(AuthProvider authProvider) {
    _authProvider = authProvider;
    return this;
  }

  @Override
  public boolean execute()
      throws Exception {
    if (_controllerHost == null) {
      _controllerHost = NetUtils.getHostAddress();
    }

    String stateValue = _state.toLowerCase();
    if (!stateValue.equals("enable") && !stateValue.equals("disable") && !stateValue.equals("drop")) {
      throw new IllegalArgumentException(
          "Invalid value for state: " + _state + "\n Value must be one of enable|disable|drop");
    }
    try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
      URI uri = new URI(_controllerProtocol, null, _controllerHost, Integer.parseInt(_controllerPort),
          URI_TABLES_PATH + _tableName, "state=" + stateValue, null);

      HttpGet httpGet = new HttpGet(uri);
      AuthProviderUtils.makeAuthHeaders(
              AuthProviderUtils.makeAuthProvider(_authProvider, _authTokenUrl, _authToken, _user, _password))
          .forEach(header -> httpGet.addHeader(header.getName(), header.getValue()));

      HttpResponse response = httpClient.execute(httpGet);
      int status = response.getStatusLine().getStatusCode();
      if (status != 200) {
        String responseString = EntityUtils.toString(response.getEntity());
        throw new HttpException("Failed to change table state, error: " + responseString);
      }
    }
    return true;
  }

  @Override
  public String description() {
    return "Change the state (enable|disable|drop) of Pinot table";
  }

  public boolean getHelp() {
    return _help;
  }

  @Override
  public String getName() {
    return "ChangeTableState";
  }

  @Override
  public String toString() {
    return ("ChangeTableState -controllerProtocol " + _controllerProtocol + " -controllerHost " + _controllerHost
        + " -controllerPort " + _controllerPort + " -tableName" + _tableName + " -state" + _state + " -user " + _user
        + " -password " + "[hidden]");
  }

  @Override
  public void cleanup() {
  }
}
