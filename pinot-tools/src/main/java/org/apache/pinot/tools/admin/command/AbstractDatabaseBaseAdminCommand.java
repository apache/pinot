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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.net.ssl.SSLContext;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.utils.ClientSSLContextGenerator;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.tools.Command;
import picocli.CommandLine;


public abstract class AbstractDatabaseBaseAdminCommand extends AbstractBaseAdminCommand implements Command {
  @CommandLine.Option(names = {"-controllerHost"}, required = false, description = "Host name for controller.")
  protected String _controllerHost;

  @CommandLine.Option(names = {"-controllerPort"}, required = false, description = "Port number for controller.")
  protected String _controllerPort = DEFAULT_CONTROLLER_PORT;

  @CommandLine.Option(names = {"-controllerProtocol"}, required = false, description = "Protocol for controller.")
  protected String _controllerProtocol = CommonConstants.HTTP_PROTOCOL;

  @CommandLine.Option(names = {"-user"}, required = false, description = "Username for basic auth.")
  protected String _user;

  @CommandLine.Option(names = {"-password"}, required = false, description = "Password for basic auth.")
  protected String _password;

  @CommandLine.Option(names = {"-authToken"}, required = false, description = "Http auth token.")
  protected String _authToken;

  @CommandLine.Option(names = {"-authTokenUrl"}, required = false, description = "Http auth token url.")
  protected String _authTokenUrl;

  protected AuthProvider _authProvider;

  @CommandLine.Option(names = {"-exec"}, required = false, description = "Execute the command.")
  protected boolean _exec;

  @CommandLine.Option(names = {"-skipControllerCertValidation"}, required = false, description = "Whether to skip"
      + " controller certification validation.")
  private boolean _skipControllerCertValidation = false;

  @CommandLine.Option(names = {"-database"}, required = false, description = "Corresponding database.")
  protected String _database;

  public AbstractDatabaseBaseAdminCommand setControllerHost(String controllerHost) {
    _controllerHost = controllerHost;
    return this;
  }

  public AbstractDatabaseBaseAdminCommand setControllerPort(String controllerPort) {
    _controllerPort = controllerPort;
    return this;
  }

  public AbstractDatabaseBaseAdminCommand setControllerProtocol(String controllerProtocol) {
    _controllerProtocol = controllerProtocol;
    return this;
  }

  public AbstractDatabaseBaseAdminCommand setUser(String user) {
    _user = user;
    return this;
  }

  public AbstractDatabaseBaseAdminCommand setPassword(String password) {
    _password = password;
    return this;
  }

  public AbstractDatabaseBaseAdminCommand setAuthProvider(AuthProvider authProvider) {
    _authProvider = authProvider;
    return this;
  }

  public AbstractDatabaseBaseAdminCommand setDatabase(String database) {
    _database = database;
    return this;
  }

  protected SSLContext makeTrustAllSSLContext() {
    if (_skipControllerCertValidation) {
      PinotConfiguration trustAllSslConfig = new PinotConfiguration();
      return new ClientSSLContextGenerator(trustAllSslConfig).generate();
    } else {
      return null;
    }
  }

  protected List<Header> getHeaders() {
    List<Header> headers = AuthProviderUtils.makeAuthHeaders(
        AuthProviderUtils.makeAuthProvider(_authProvider, _authTokenUrl, _authToken, _user, _password));
    headers.add(new BasicHeader("database", _database));
    return headers;
  }

  protected Map<String, String> getHeadersAsMap() {
    return getHeaders().stream().collect(Collectors.toMap(Header::getName, Header::getValue));
  }

  public AbstractDatabaseBaseAdminCommand setExecute(boolean exec) {
    _exec = exec;
    return this;
  }

  @Override
  public String toString() {
    String retString =
        (" -controllerProtocol " + _controllerProtocol + " -controllerHost " + _controllerHost + " -controllerPort "
            + _controllerPort + " -controllerProtocol " + _controllerProtocol + " -database " + _database + " -user "
            + _user + " -password " + "[hidden]");

    return ((_exec) ? (retString + " -exec") : retString);
  }
}
