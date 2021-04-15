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
import java.util.Collections;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.controller.helix.ControllerRequestURLBuilder;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.NetUtils;
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

  @Option(name = "-tableConfigFile", required = true, metaVar = "<string>", aliases = {"-tableConf", "-tableConfig", "-filePath"}, usage = "Path to table config file.")
  private String _tableConfigFile;

  @Option(name = "-schemaFile", required = false, metaVar = "<string>", aliases = {"-schema"}, usage = "Path to table schema file.")
  private String _schemaFile = null;

  @Option(name = "-controllerHost", required = false, metaVar = "<String>", usage = "host name for controller.")
  private String _controllerHost;

  @Option(name = "-controllerPort", required = false, metaVar = "<int>", usage = "Port number to start the controller at.")
  private String _controllerPort = DEFAULT_CONTROLLER_PORT;

  @Option(name = "-controllerProtocol", required = false, metaVar = "<String>", usage = "protocol for controller.")
  private String _controllerProtocol = CommonConstants.HTTP_PROTOCOL;

  @Option(name = "-exec", required = false, metaVar = "<boolean>", usage = "Execute the command.")
  private boolean _exec;

  @Option(name = "-user", required = false, metaVar = "<String>", usage = "Username for basic auth.")
  private String _user;

  @Option(name = "-password", required = false, metaVar = "<String>", usage = "Password for basic auth.")
  private String _password;

  @Option(name = "-authToken", required = false, metaVar = "<String>", usage = "Http auth token.")
  private String _authToken;

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
    return "Create a Pinot table";
  }

  @Override
  public String toString() {
    String retString =
        ("AddTable -tableConfigFile " + _tableConfigFile + " -schemaFile " + _schemaFile + " -controllerProtocol "
            + _controllerProtocol + " -controllerHost " + _controllerHost + " -controllerPort " + _controllerPort
            + " -user " + _user + " -password " + "[hidden]");
    return ((_exec) ? (retString + " -exec") : retString);
  }

  @Override
  public void cleanup() {
  }

  public AddTableCommand setTableConfigFile(String tableConfigFile) {
    _tableConfigFile = tableConfigFile;
    return this;
  }

  public AddTableCommand setSchemaFile(String schemaFile) {
    _schemaFile = schemaFile;
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

  public AddTableCommand setControllerProtocol(String controllerProtocol) {
    _controllerProtocol = controllerProtocol;
    return this;
  }

  public AddTableCommand setUser(String user) {
    _user = user;
    return this;
  }

  public AddTableCommand setPassword(String password) {
    _password = password;
    return this;
  }

  public AddTableCommand setAuthToken(String authToken) {
    _authToken = authToken;
    return this;
  }

  public AddTableCommand setExecute(boolean exec) {
    _exec = exec;
    return this;
  }

  public void uploadSchema()
      throws Exception {
    File schemaFile;
    Schema schema;
    try {
      schemaFile = new File(_schemaFile);
      schema = Schema.fromFile(schemaFile);
    } catch (Exception e) {
      LOGGER.error("Got exception while reading Pinot schema from file: [" + _schemaFile + "]");
      throw e;
    }
    try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient()) {
      fileUploadDownloadClient.addSchema(FileUploadDownloadClient
              .getUploadSchemaURI(_controllerProtocol, _controllerHost, Integer.parseInt(_controllerPort)),
          schema.getSchemaName(), schemaFile, makeAuthHeader(makeAuthToken(_authToken, _user, _password)),
          Collections.emptyList());
    } catch (Exception e) {
      LOGGER.error("Got Exception to upload Pinot Schema: " + schema.getSchemaName(), e);
      throw e;
    }
  }

  public boolean sendTableCreationRequest(JsonNode node)
      throws IOException {
    String res = AbstractBaseAdminCommand
        .sendRequest("POST", ControllerRequestURLBuilder.baseUrl(_controllerAddress).forTableCreate(), node.toString(),
            makeAuthHeader(makeAuthToken(_authToken, _user, _password)));
    LOGGER.info(res);
    return res.contains("succesfully added");
  }

  @Override
  public boolean execute()
      throws Exception {
    if (!_exec) {
      LOGGER.warn("Dry Running Command: " + toString());
      LOGGER.warn("Use the -exec option to actually execute the command.");
      return true;
    }

    if (_controllerHost == null) {
      _controllerHost = NetUtils.getHostAddress();
    }
    _controllerAddress = _controllerProtocol + "://" + _controllerHost + ":" + _controllerPort;

    LOGGER.info("Executing command: " + toString());

    // Backward compatible
    if (_schemaFile != null) {
      uploadSchema();
    }
    return sendTableCreationRequest(JsonUtils.fileToJsonNode(new File(_tableConfigFile)));
  }
}
