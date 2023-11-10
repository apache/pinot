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

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URI;
import java.util.Collections;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.tools.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


@CommandLine.Command(name = "AddSchema")
public class AddSchemaCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(AddSchemaCommand.class);

  @CommandLine.Option(names = {"-controllerHost"}, required = false, description = "host name for controller.")
  private String _controllerHost;

  @CommandLine.Option(names = {"-controllerPort"}, required = false, description = "port name for controller.")
  private String _controllerPort = DEFAULT_CONTROLLER_PORT;

  @CommandLine.Option(names = {"-controllerProtocol"}, required = false, description = "protocol for controller.")
  private String _controllerProtocol = CommonConstants.HTTP_PROTOCOL;

  @CommandLine.Option(names = {"-schemaFile"}, required = true, description = "Path to schema file.")
  private String _schemaFile = null;

  @CommandLine.Option(names = {"-override"}, required = false, description = "Whether to override the schema if the "
      + "schema exists.")
  private boolean _override = false;

  @CommandLine.Option(names = {"-force"}, required = false, description = "Whether to force overriding the schema if "
      + "the schema exists.")
  private boolean _force = false;

  @CommandLine.Option(names = {"-exec"}, required = false, description = "Execute the command.")
  private boolean _exec;

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

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public String description() {
    return "Add schema specified in the schema file to the controller";
  }

  @Override
  public String getName() {
    return "AddSchema";
  }

  @Override
  public String toString() {
    String retString = ("AddSchema -controllerProtocol " + _controllerProtocol + " -controllerHost " + _controllerHost
        + " -controllerPort " + _controllerPort + " -schemaFile " + _schemaFile + " -override " + _override + " _force "
        + _force + " -user " + _user + " -password " + "[hidden]");

    return ((_exec) ? (retString + " -exec") : retString);
  }

  @Override
  public void cleanup() {
  }

  public AddSchemaCommand setControllerHost(String controllerHost) {
    _controllerHost = controllerHost;
    return this;
  }

  public AddSchemaCommand setControllerPort(String controllerPort) {
    _controllerPort = controllerPort;
    return this;
  }

  public AddSchemaCommand setControllerProtocol(String controllerProtocol) {
    _controllerProtocol = controllerProtocol;
    return this;
  }

  public AddSchemaCommand setSchemaFilePath(String schemaFilePath) {
    _schemaFile = schemaFilePath;
    return this;
  }

  public AddSchemaCommand setOverride(boolean override) {
    _override = override;
    return this;
  }

  public AddSchemaCommand setForce(boolean force) {
    _force = force;
    return this;
  }

  public void setUser(String user) {
    _user = user;
  }

  public void setPassword(String password) {
    _password = password;
  }

  public void setAuthProvider(AuthProvider authProvider) {
    _authProvider = authProvider;
  }

  public AddSchemaCommand setExecute(boolean exec) {
    _exec = exec;
    return this;
  }

  @Override
  public boolean execute()
      throws Exception {
    if (_controllerHost == null) {
      _controllerHost = NetUtils.getHostAddress();
    }

    if (!_exec) {
      LOGGER.warn("Dry Running Command: " + toString());
      LOGGER.warn("Use the -exec option to actually execute the command.");
      return true;
    }

    File schemaFile = new File(_schemaFile);
    LOGGER.info("Executing command: " + toString());
    if (!schemaFile.exists()) {
      throw new FileNotFoundException("file does not exist, + " + _schemaFile);
    }

    Schema schema = Schema.fromFile(schemaFile);
    try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient()) {
      URI schemaURI = FileUploadDownloadClient
          .getUploadSchemaURI(_controllerProtocol, _controllerHost, Integer.parseInt(_controllerPort));
      schemaURI = new URI(schemaURI + "?override=" + _override + "?force=" + _force);
      fileUploadDownloadClient.addSchema(schemaURI,
          schema.getSchemaName(), schemaFile, AuthProviderUtils.makeAuthHeaders(
              AuthProviderUtils.makeAuthProvider(_authProvider, _authTokenUrl, _authToken,
              _user, _password)), Collections.emptyList());
    } catch (Exception e) {
      LOGGER.error("Got Exception to upload Pinot Schema: " + schema.getSchemaName(), e);
      return false;
    }
    return true;
  }
}
