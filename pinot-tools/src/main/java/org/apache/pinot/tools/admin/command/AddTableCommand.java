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

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.io.IOException;
import org.apache.pinot.common.utils.JsonUtils;
import org.apache.pinot.common.utils.NetUtil;
import org.apache.pinot.controller.helix.ControllerRequestURLBuilder;
import org.apache.pinot.tools.Command;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class to implement CreateResource command.
 *
 */
public class AddTableCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(AddTableCommand.class);

  @Option(name = "-filePath", required = true, metaVar = "<string>", usage = "Path to the request.json file")
  private String _filePath;

  @Option(name = "-controllerHost", required = false, metaVar = "<String>", usage = "host name for controller.")
  private String _controllerHost;

  @Option(name = "-controllerPort", required = false, metaVar = "<int>", usage = "Port number to start the controller at.")
  private String _controllerPort = DEFAULT_CONTROLLER_PORT;

  @Option(name = "-exec", required = false, metaVar = "<boolean>", usage = "Execute the command.")
  private boolean _exec;

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help = false;

  private String _controllerAddress;

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public String getName() {
    return "AddTable";
  }

  @Override
  public String description() {
    return "Add table specified in the json file, to the controller.";
  }

  @Override
  public String toString() {
    String retString = ("AddTable -filePath " + _filePath + " -controllerHost " + _controllerHost + " -controllerPort "
        + _controllerPort);

    return ((_exec) ? (retString + " -exec") : retString);
  }

  @Override
  public void cleanup() {

  }

  public AddTableCommand setFilePath(String filePath) {
    _filePath = filePath;
    return this;
  }

  public AddTableCommand setControllerHost(String controllerHost) {
    _controllerHost = controllerHost;
    return this;
  }

  public AddTableCommand setControllerPort(String controllerPort) {
    _controllerPort = controllerPort;
    return this;
  }

  public AddTableCommand setExecute(boolean exec) {
    _exec = exec;
    return this;
  }

  public boolean execute(JsonNode node)
      throws IOException {
    if (_controllerHost == null) {
      _controllerHost = NetUtil.getHostAddress();
    }
    _controllerAddress = "http://" + _controllerHost + ":" + _controllerPort;

    if (!_exec) {
      LOGGER.warn("Dry Running Command: " + toString());
      LOGGER.warn("Use the -exec option to actually execute the command.");
      return true;
    }

    LOGGER.info("Executing command: " + toString());
    String res = AbstractBaseAdminCommand
        .sendPostRequest(ControllerRequestURLBuilder.baseUrl(_controllerAddress).forTableCreate(), node.toString());

    LOGGER.info(res);
    return true;
  }

  @Override
  public boolean execute()
      throws Exception {
    return execute(JsonUtils.fileToJsonNode(new File(_filePath)));
  }
}
