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

import com.linkedin.pinot.common.utils.NetUtil;
import com.linkedin.pinot.tools.Command;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpURL;
import org.apache.commons.httpclient.methods.GetMethod;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ChangeTableState extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(ChangeTableState.class);

  @Option (name = "-controllerHost", required = false, metaVar = "<String>", usage = "host name for controller")
  private String _controllerHost;

  @Option (name = "-controllerPort", required = false, metaVar = "<int>", usage = "Port number for controller.")
  private String _controllerPort = DEFAULT_CONTROLLER_PORT;

  @Option (name = "-tableName", required = true, metaVar = "<String>", usage = "Table name to disable")
  private String _tableName;

  @Option (name = "-state", required = true, metaVar = "<String>", usage = "Change Table State(enable|disable|drop)")
  private String _state;

  @Option (name = "-help", required = false, help = true, aliases = { "-h", "--h", "--help" },
      usage = "Print this message.")
  private boolean _help = false;

  @Override
  public boolean execute() throws Exception {
    if (_controllerHost == null) {
      _controllerHost = NetUtil.getHostAddress();
    }

    String stateValue = _state.toLowerCase();
    if (!stateValue.equals("enable")
        && !stateValue.equals("disable")
        && !stateValue.equals("drop")) {
      throw new IllegalArgumentException("Invalid value for state: " + _state
          + "\n Value must be one of enable|disable|drop");
    }
    HttpClient httpClient = new HttpClient();
    HttpURL url = new HttpURL(_controllerHost,
        Integer.parseInt(_controllerPort),
        URI_TABLES_PATH + _tableName);
    url.setQuery("state", stateValue);
    GetMethod httpGet = new GetMethod(url.getEscapedURI());
    int status = httpClient.executeMethod(httpGet);
    if (status != 200) {
      throw new RuntimeException(
          "Failed to change table state, error: " + httpGet.getResponseBodyAsString());
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
    return ("ChangeTableState -controllerHost " + _controllerHost
        + " -controllerPort " + _controllerPort
        + " -tableName" + _tableName
        + " -state" + _state);
  }

  @Override
  public void cleanup() {

  }

}
