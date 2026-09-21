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
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
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


@CommandLine.Command(name = "PostQuery", mixinStandardHelpOptions = true)
public class PostQueryCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(PostQueryCommand.class.getName());

  /// Supported output formats for the query response: `JSON` (default, raw broker response) or
  /// `CSV` (rendered from `resultTable`; see `formatResponse` for fallback behavior).
  public enum OutputFormat {
    JSON, CSV
  }

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

  @CommandLine.Option(names = {"-o", "-option"}, required = false, description = "Additional options '-o key=value'")
  private Map<String, String> _additionalOptions = new HashMap<>();

  @CommandLine.Option(names = {"-outputFormat"}, required = false,
      description = "Output format for the query response: JSON (default) or CSV (upper-case).")
  private OutputFormat _outputFormat = OutputFormat.JSON;

  @CommandLine.Option(names = {"-outputFile"}, required = false,
      description = "File path to write the query response to. If omitted, the response is only "
          + "logged (pre-existing behavior).")
  private String _outputFile;

  private AuthProvider _authProvider;

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

  public PostQueryCommand setOutputFormat(OutputFormat outputFormat) {
    _outputFormat = outputFormat;
    return this;
  }

  public PostQueryCommand setOutputFile(String outputFile) {
    _outputFile = outputFile;
    return this;
  }

  public String run()
      throws Exception {
    if (_brokerHost == null) {
      _brokerHost = NetUtils.getHostAddress();
    }
    LOGGER.info("Executing command: {}", this);
    String url = _brokerProtocol + "://" + _brokerHost + ":" + _brokerPort + "/query/sql";
    Map<String, String> payload = new HashMap<>();
    payload.put(Request.SQL, _query);
    if (_additionalOptions != null) {
      payload.putAll(_additionalOptions);
    }
    String request = JsonUtils.objectToString(payload);
    String response = sendRequest("POST", url, request, AuthProviderUtils.makeAuthHeaders(
        AuthProviderUtils.makeAuthProvider(_authProvider, _authTokenUrl, _authToken, _user, _password)));
    return formatResponse(response);
  }

  /// Renders the raw broker response according to `_outputFormat`. Never throws for a
  /// CSV-incompatible response: falls back to the raw response whenever the response isn't valid
  /// JSON, or has no `resultTable` (e.g. the query errored out), so no error detail is ever lost.
  String formatResponse(String rawResponse) {
    if (_outputFormat != OutputFormat.CSV) {
      return rawResponse;
    }
    JsonNode root;
    try {
      root = JsonUtils.stringToJsonNode(rawResponse);
    } catch (IOException e) {
      LOGGER.warn("Response is not valid JSON (e.g. a broker/proxy error page); "
          + "falling back to the raw response instead of CSV.", e);
      return rawResponse;
    }
    JsonNode resultTable = root.get("resultTable");
    if (resultTable == null || resultTable.isNull()) {
      LOGGER.warn("Response has no 'resultTable' (e.g. the query may have errored out); "
          + "falling back to JSON output instead of CSV.");
      return rawResponse;
    }
    JsonNode exceptions = root.path("exceptions");
    if (exceptions.size() > 0 || root.path("partialResult").asBoolean(false)) {
      LOGGER.warn("Response has a 'resultTable' but also reports exceptions and/or a partial "
          + "result; CSV output only renders resultTable rows, so this detail is not reflected "
          + "in the CSV. Use -outputFormat JSON to inspect the full response.");
    }
    JsonNode columnNames = resultTable.path("dataSchema").path("columnNames");
    JsonNode rows = resultTable.path("rows");
    StringWriter stringWriter = new StringWriter();
    try (CSVPrinter csvPrinter = new CSVPrinter(stringWriter, CSVFormat.DEFAULT)) {
      for (JsonNode columnName : columnNames) {
        csvPrinter.print(columnName.asText());
      }
      csvPrinter.println();
      for (JsonNode row : rows) {
        Iterator<JsonNode> cells = row.elements();
        while (cells.hasNext()) {
          JsonNode cell = cells.next();
          csvPrinter.print(cell.isTextual() ? cell.asText() : cell.toString());
        }
        csvPrinter.println();
      }
    } catch (IOException e) {
      // CSVPrinter only throws IOException for the underlying Appendable; a StringWriter never
      // throws, so this is unreachable in practice. Fall back to JSON rather than propagate.
      LOGGER.warn("Unexpected error rendering CSV; falling back to JSON output.", e);
      return rawResponse;
    }
    return stringWriter.toString();
  }

  @Override
  public boolean execute()
      throws Exception {
    String result = run();
    LOGGER.info("Result: {}", result);
    if (_outputFile != null) {
      Files.write(new File(_outputFile).toPath(), result.getBytes(StandardCharsets.UTF_8));
    }
    return true;
  }
}
