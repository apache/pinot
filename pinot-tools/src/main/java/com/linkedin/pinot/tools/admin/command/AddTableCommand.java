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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONException;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.controller.helix.ControllerRequestURLBuilder;


/**
 * Class to implement CreateResource command.
 *
 */
public class AddTableCommand extends AbstractBaseCommand implements Command {
  private static final Logger _logger = LoggerFactory.getLogger(AddTableCommand.class);

  @Option(name = "-filePath", required = true, metaVar = "<string>", usage = "Path to the request.json file")
  private String _filePath;

  @Option(name="-controllerPort", required=false, metaVar="<int>", usage="Port number to start the controller at.")
  private String _controllerPort = DEFAULT_CONTROLLER_PORT;

  @Option(name = "-help", required = false, help = true, aliases={"-h", "--h", "--help"}, usage = "Print this message.")
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
    return ("AddTable -filePath " + _filePath  + " -controllerPort " + _controllerPort);
  }

  @Override
  public void cleanup() {

  }

  public AddTableCommand setFilePath(String filePath) {
    _filePath = filePath;
    return this;
  }

  public AddTableCommand setControllerPort(String controllerPort) {
    _controllerPort = controllerPort;
    return this;
  }

  public boolean execute(JsonNode node) throws UnsupportedEncodingException, IOException, JSONException {
    _controllerAddress = "http://localhost:" + _controllerPort;
    String res =
        AbstractBaseCommand.sendPostRequest(ControllerRequestURLBuilder.baseUrl(_controllerAddress).forTableCreate(),
            node.toString());

    _logger.info(res);
    return true;
  }

  @Override
  public boolean execute() throws Exception {
    JsonNode node = new ObjectMapper().readTree(new FileInputStream (_filePath));
    return execute(node);
  }
}
