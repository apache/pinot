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

import java.util.Collections;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.tools.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


@CommandLine.Command(name = "DeleteSchema")
public class DeleteSchemaCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(DeleteSchemaCommand.class);

  @CommandLine.Option(names = {"-controllerHost"}, required = false, description = "host name for controller.")
  private String _controllerHost;

  @CommandLine.Option(names = {"-controllerPort"}, required = false, description = "port name for controller.")
  private String _controllerPort = DEFAULT_CONTROLLER_PORT;

  @CommandLine.Option(names = {"-controllerProtocol"}, required = false, description = "protocol for controller.")
  private String _controllerProtocol = CommonConstants.HTTP_PROTOCOL;

  @CommandLine.Option(names = {"-schemaName"}, required = true, description = "Schema name.")
  private String _schemaName = null;

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

  @CommandLine.Option(names = {"-help", "-h", "--h", "--help"}, required = false, help = true, description = "Print "
      + "this message.")
  private boolean _help = false;

  private AuthProvider _authProvider;

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public String description() {
    return "Delete schema specified via name";
  }

  @Override
  public String getName() {
    return "DeleteSchema";
  }

  @Override
  public String toString() {
    String retString =
        ("DeleteSchema -controllerProtocol " + _controllerProtocol + " -controllerHost " + _controllerHost
            + " -controllerPort " + _controllerPort + " -schemaName " + _schemaName + " -user " + _user + " -password "
            + "[hidden]");

    return ((_exec) ? (retString + " -exec") : retString);
  }

  @Override
  public void cleanup() {
  }

  public DeleteSchemaCommand setControllerHost(String controllerHost) {
    _controllerHost = controllerHost;
    return this;
  }

  public DeleteSchemaCommand setControllerPort(String controllerPort) {
    _controllerPort = controllerPort;
    return this;
  }

  public DeleteSchemaCommand setControllerProtocol(String controllerProtocol) {
    _controllerProtocol = controllerProtocol;
    return this;
  }

  public DeleteSchemaCommand setSchemaName(String schemaName) {
    _schemaName = schemaName;
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

  public DeleteSchemaCommand setExecute(boolean exec) {
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

    LOGGER.info("Executing command: " + toString());
    try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient()) {
      fileUploadDownloadClient.getHttpClient().sendDeleteRequest(
          FileUploadDownloadClient.getDeleteSchemaURI(_controllerProtocol, _controllerHost,
              Integer.parseInt(_controllerPort), _schemaName), Collections.emptyMap(),
          AuthProviderUtils.makeAuthProvider(_authProvider, _authTokenUrl, _authToken, _user, _password));
    } catch (Exception e) {
      LOGGER.error("Got Exception while deleting Pinot Schema: " + _schemaName, e);
      return false;
    }
    return true;
  }
}
