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
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.CommonConstants.Broker.Request;
import org.apache.pinot.common.utils.NetUtil;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.tools.Command;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PostQueryCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(PostQueryCommand.class.getName());

  @Option(name = "-brokerHost", required = false, metaVar = "<String>", usage = "host name for broker.")
  private String _brokerHost;

  @Option(name = "-brokerPort", required = false, metaVar = "<int>", usage = "http port for broker.")
  private String _brokerPort = Integer.toString(CommonConstants.Helix.DEFAULT_BROKER_QUERY_PORT);

  @Option(name = "-brokerProtocol", required = false, metaVar = "<String>", usage = "protocol for broker.")
  private String _brokerProtocol = "http";

  @Option(name = "-queryType", required = false, metaVar = "<string>", usage = "Query use sql or pql.")
  private String _queryType = Request.PQL;

  @Option(name = "-query", required = true, metaVar = "<string>", usage = "Query string to perform.")
  private String _query;

  @Option(name = "-user", required = false, metaVar = "<String>", usage = "Username for basic auth.")
  private String _user;

  @Option(name = "-password", required = false, metaVar = "<String>", usage = "Password for basic auth.")
  private String _password;

  @Option(name = "-authToken", required = false, metaVar = "<String>", usage = "Http auth token.")
  private String _authToken;

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help = false;

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
        + _brokerPort + " -queryType " + _queryType + " -query " + _query);
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

  public PostQueryCommand setAuthToken(String authToken) {
    _authToken = authToken;
    return this;
  }

  public PostQueryCommand setQueryType(String queryType) {
    _queryType = queryType;
    return this;
  }

  public PostQueryCommand setQuery(String query) {
    _query = query;
    return this;
  }

  public String run()
      throws Exception {
    if (_brokerHost == null) {
      _brokerHost = NetUtil.getHostAddress();
    }
    LOGGER.info("Executing command: " + toString());

    String request;
    String urlString = _brokerProtocol + "://" + _brokerHost + ":" + _brokerPort + "/query";
    if (_queryType.toLowerCase().equals(Request.SQL)) {
      urlString += "/sql";
      request = JsonUtils.objectToString(Collections.singletonMap(Request.SQL, _query));
    } else {
      request = JsonUtils.objectToString(Collections.singletonMap(Request.PQL, _query));
    }

    return sendRequest("POST", urlString, request, makeAuthHeader(makeAuthToken(_authToken, _user, _password)));
  }

  @Override
  public boolean execute()
      throws Exception {
    String result = run();
    LOGGER.info("Result: " + result);
    return true;
  }
}
