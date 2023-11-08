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

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.tools.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


@CommandLine.Command(name = "PostQuery")
public class PostQueryCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(PostQueryCommand.class.getName());

  @CommandLine.Option(names = {"-brokerHost"}, required = false, description = "host name for broker.")
  private String _brokerHost;

  @CommandLine.Option(names = {"-brokerPort"}, required = false, description = "http port for broker.")
  private String _brokerPort = Integer.toString(CommonConstants.Helix.DEFAULT_BROKER_QUERY_PORT);

  @CommandLine.Option(names = {"-brokerProtocol"}, required = false, description = "protocol for broker.")
  private String _brokerProtocol = "http";

  @CommandLine.Option(names = {"-query"}, required = true, description = "Query string to perform.")
  private String _query;

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

  @CommandLine.Option(names = {"-o", "-option"}, required = false, description = "Additional options '-o key=value'")
  private Map<String, String> _additionalOptions = new HashMap<>();

  private AuthProvider _authProvider;

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public String getName() {
    return "PostQuery";
  }

  @Override
  public String toString() {
    return ("PostQuery -brokerProtocol " + _brokerProtocol + " -brokerHost " + _brokerHost + " -brokerPort "
        + _brokerPort + " -query " + _query);
  }

  @Override
  public void cleanup() {
  }

  @Override
  public String description() {
    return "Query the uploaded Pinot segments.";
  }

  public PostQueryCommand setBrokerHost(String host) {
    _brokerHost = host;
    return this;
  }

  public PostQueryCommand setBrokerPort(String port) {
    _brokerPort = port;
    return this;
  }

  public PostQueryCommand setBrokerProtocol(String protocol) {
    _brokerProtocol = protocol;
    return this;
  }

  public PostQueryCommand setUser(String user) {
    _user = user;
    return this;
  }

  public PostQueryCommand setPassword(String password) {
    _password = password;
    return this;
  }

  public PostQueryCommand setQuery(String query) {
    _query = query;
    return this;
  }

  public PostQueryCommand setAuthProvider(AuthProvider authProvider) {
    _authProvider = authProvider;
    return this;
  }

  public PostQueryCommand setAdditionalOptions(Map<String, String> additionalOptions) {
    _additionalOptions.putAll(additionalOptions);
    return this;
  }

  public String run()
      throws Exception {
    if (_brokerHost == null) {
      _brokerHost = NetUtils.getHostAddress();
    }
    LOGGER.info("Executing command: " + this);
    String url = _brokerProtocol + "://" + _brokerHost + ":" + _brokerPort + "/query/sql";
    Map<String, String> payload = new HashMap<>();
    payload.put(Request.SQL, _query);
    if (_additionalOptions != null) {
      payload.putAll(_additionalOptions);
    }
    String request = JsonUtils.objectToString(payload);
    return sendRequest("POST", url, request, AuthProviderUtils.makeAuthHeaders(
        AuthProviderUtils.makeAuthProvider(_authProvider, _authTokenUrl, _authToken, _user, _password)));
  }

  @Override
  public boolean execute()
      throws Exception {
    String result = run();
    LOGGER.info("Result: " + result);
    return true;
  }
}
