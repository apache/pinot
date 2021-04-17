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
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.tools.Command;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ChangeTableState extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(ChangeTableState.class);

  @Option(name = "-controllerHost", required = false, metaVar = "<String>", usage = "host name for controller")
  private String _controllerHost;

  @Option(name = "-controllerPort", required = false, metaVar = "<int>", usage = "Port number for controller.")
  private String _controllerPort = DEFAULT_CONTROLLER_PORT;

  @Option(name = "-controllerProtocol", required = false, metaVar = "<String>", usage = "protocol for controller.")
  private String _controllerProtocol = CommonConstants.HTTP_PROTOCOL;

  @Option(name = "-tableName", required = true, metaVar = "<String>", usage = "Table name to disable")
  private String _tableName;

  @Option(name = "-state", required = true, metaVar = "<String>", usage = "Change Table State(enable|disable|drop)")
  private String _state;

  @Option(name = "-user", required = false, metaVar = "<String>", usage = "Username for basic auth.")
  private String _user;

  @Option(name = "-password", required = false, metaVar = "<String>", usage = "Password for basic auth.")
  private String _password;

  @Option(name = "-authToken", required = false, metaVar = "<String>", usage = "Http auth token.")
  private String _authToken;

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"},
      usage = "Print this message.")
  private boolean _help = false;

  @Override
  public boolean execute() throws Exception {
    if (_controllerHost == null) {
      _controllerHost = NetUtils.getHostAddress();
    }

    String stateValue = _state.toLowerCase();
    if (!stateValue.equals("enable") && !stateValue.equals("disable") && !stateValue.equals("drop")) {
      throw new IllegalArgumentException(
          "Invalid value for state: " + _state + "\n Value must be one of enable|disable|drop");
    }
    HttpClient httpClient = new HttpClient();
    URI uri = new URI(_controllerProtocol, null, _controllerHost, Integer.parseInt(_controllerPort),
        URI_TABLES_PATH + _tableName, "state=" + stateValue, null);

    String token = makeAuthToken(_authToken, _user, _password);

    GetMethod httpGet = new GetMethod(uri.toString());
    if (StringUtils.isNotBlank(token)) {
      httpGet.setRequestHeader("Authorization", token);
    }
    int status = httpClient.executeMethod(httpGet);
    if (status != 200) {
      throw new RuntimeException("Failed to change table state, error: " + httpGet.getResponseBodyAsString());
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
