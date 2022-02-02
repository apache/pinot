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
import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.tools.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


@CommandLine.Command(name = "EstimateHeapSize")
public class HeapUsageEstimateCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(HeapUsageEstimateCommand.class.getName());
  private static final String TAB = "\t\t";
  private static final String NEW_LINE = "\n";

  @CommandLine.Option(names = {"-brokerHost"}, required = false, description = "host name for broker.")
  private String _brokerHost;

  @CommandLine.Option(names = {"-brokerPort"}, required = false, description = "http port for broker.")
  private String _brokerPort = Integer.toString(CommonConstants.Helix.DEFAULT_BROKER_QUERY_PORT);

  @CommandLine.Option(names = {"-brokerProtocol"}, required = false, description = "protocol for broker.")
  private String _brokerProtocol = "http";

  @CommandLine.Option(names = {"-tableName"}, required = true, description = "table name.")
  private String _tableName;

  @CommandLine.Option(names = {"-schemaFile"}, required = true, description = "Path to schema file.")
  private String _schemaFile;

  @CommandLine.Option(names = {"-tableConfigFile"}, required = true, description = "Path to tableConfig file.")
  private String _tableConfigFile;

  @CommandLine.Option(names = {"-primaryKeySize"}, required = false, description = "Primary key columns bytes")
  private int _primaryKeySize = 8;

  @CommandLine.Option(names = {"-comparisonColSize"}, required = false, description = "Comparison columns bytes")
  private int _comparisonColSize = 8;

  @CommandLine.Option(names = {"-messageRate"}, required = true, description = "Message rate per second")
  private float _messageRate;

  @CommandLine.Option(names = {"-user"}, required = false, description = "Username for basic auth.")
  private String _user;

  @CommandLine.Option(names = {"-password"}, required = false, description = "Password for basic auth.")
  private String _password;

  @CommandLine.Option(names = {"-authToken"}, required = false, description = "Http auth token.")
  private String _authToken;

  @CommandLine.Option(names = {"-help", "-h", "--h", "--help"}, required = false, help = true,
      description = "Print this message.")
  private boolean _help = false;

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public String getName() {
    return "EstimateHeapSize";
  }

  @Override
  public String toString() {
    return ("EstimateHeapSize -brokerProtocol " + _brokerProtocol + " -brokerHost " + _brokerHost + " -brokerPort "
        + _brokerPort + " -tableName " + _tableName + " -schemaFile " + _schemaFile + " -tableConfigFile "
        + _tableConfigFile + " -messageRate" + _messageRate + " -primaryKeySize" + _primaryKeySize
        + " -comparisonColSize " + _comparisonColSize);
  }

  @Override
  public void cleanup() {

  }

  @Override
  public String description() {
    return "Query the uploaded Pinot segments.";
  }

  public HeapUsageEstimateCommand setBrokerHost(String host) {
    _brokerHost = host;
    return this;
  }

  public HeapUsageEstimateCommand setBrokerPort(String port) {
    _brokerPort = port;
    return this;
  }

  public HeapUsageEstimateCommand setBrokerProtocol(String protocol) {
    _brokerProtocol = protocol;
    return this;
  }

  public HeapUsageEstimateCommand setUser(String user) {
    _user = user;
    return this;
  }

  public HeapUsageEstimateCommand setPassword(String password) {
    _password = password;
    return this;
  }

  public HeapUsageEstimateCommand setAuthToken(String authToken) {
    _authToken = authToken;
    return this;
  }

  public HeapUsageEstimateCommand setTableName(String tableName) {
    _tableName = tableName;
    return this;
  }

  public HeapUsageEstimateCommand setSchemaFile(String schemaFile) {
    _schemaFile = schemaFile;
    return this;
  }

  public HeapUsageEstimateCommand setTableConfigFile(String tableConfigFile) {
    _tableConfigFile = tableConfigFile;
    return this;
  }

  public HeapUsageEstimateCommand setPrimaryKeySize(int primaryKeySize) {
    _primaryKeySize = primaryKeySize;
    return this;
  }

  public HeapUsageEstimateCommand setComparisonColSize(int comparisonColSize) {
    _comparisonColSize = comparisonColSize;
    return this;
  }

  public HeapUsageEstimateCommand setMessageRate(float messageRate) {
    _messageRate = messageRate;
    return this;
  }

  public String run()
      throws Exception {
    if (_brokerHost == null) {
      _brokerHost = NetUtils.getHostAddress();
    }
    LOGGER.info("Executing command: " + toString());

    StringBuilder responseBuilder = new StringBuilder();
    responseBuilder.append("Schema file").append(TAB).append(_schemaFile).append(NEW_LINE);
    responseBuilder.append("Table config file").append(TAB).append(_tableConfigFile).append(NEW_LINE);

    File schemaFile = new File(_schemaFile);
    Schema schema;
    LOGGER.info("Executing command: " + toString());
    if (!schemaFile.exists()) {
      throw new FileNotFoundException("file does not exist, + " + _schemaFile);
    }
    try {
      schema = Schema.fromFile(schemaFile);
    } catch (Exception e) {
      LOGGER.error("Got exception while reading Pinot schema from file: [" + _schemaFile + "]");
      throw e;
    }

    TableConfig tableConfig;
    try {
      tableConfig = JsonUtils.fileToObject(new File(_tableConfigFile), TableConfig.class);
    } catch (Exception e) {
      throw new IllegalStateException("Caught exception while reading table config from file: " + _tableConfigFile, e);
    }
    LOGGER.info("Using table config: {}", tableConfig.toJsonString());

    List<String> primaryKeys = schema.getPrimaryKeyColumns();
    // Estimated key space, it contains primary key columns
    int bytesPerKey = 0;
    for (String primaryKey : primaryKeys) {
      FieldSpec.DataType dt = schema.getFieldSpecFor(primaryKey).getDataType();
      if (dt == FieldSpec.DataType.JSON || dt == FieldSpec.DataType.LIST || dt == FieldSpec.DataType.MAP) {
        throw new Exception("Not support data types for primary key columns");
      } else if (dt == FieldSpec.DataType.STRING) {
        bytesPerKey += _primaryKeySize;
      } else {
        bytesPerKey += dt.size();
      }
    }
    // Estimated value space, it contains <segmentName, DocId, ComparisonValue(timestamp)>
    int bytesPerValue = tableConfig.getUpsertConfig().getComparisonColumn() != null ? 52 + _comparisonColSize : 64;

    String retentionTimeValue = tableConfig.getValidationConfig().getRetentionTimeValue();
    String retentionTimeUnit = tableConfig.getValidationConfig().getRetentionTimeUnit();

    responseBuilder.append("Bytes per key").append(TAB).append(bytesPerKey).append(NEW_LINE);
    responseBuilder.append("Bytes per value").append(TAB).append(bytesPerValue).append(NEW_LINE);
    responseBuilder.append("Message rate per second").append(TAB).append(_messageRate).append(NEW_LINE);
    responseBuilder.append("Retention").append(TAB).append(retentionTimeValue).append(retentionTimeUnit)
        .append(NEW_LINE);

    // Calculate upsert frequency.
    // UpsertFrequency = 1 - nums of records / nums of skipUpsert records.
    String request;
    String urlString = _brokerProtocol + "://" + _brokerHost + ":" + _brokerPort + "/query";
    urlString += "/sql";
    String query = "select count(*) from " + _tableName;
    request = JsonUtils.objectToString(Collections.singletonMap(Request.SQL, query));
    JsonNode recordNums = JsonUtils.stringToJsonNode(
        sendRequest("POST", urlString, request, makeAuthHeader(makeAuthToken(_authToken, _user, _password))))
        .get("resultTable").get("rows").get(0).get(0);

    responseBuilder.append("Nums of records").append(TAB).append(recordNums.asText()).append(NEW_LINE);

    request = JsonUtils.objectToString(Collections.singletonMap(Request.SQL, query + " option(skipUpsert=True)"));
    JsonNode recordNumsSkipUpsert = JsonUtils.stringToJsonNode(
        sendRequest("POST", urlString, request, makeAuthHeader(makeAuthToken(_authToken, _user, _password))))
        .get("resultTable").get("rows").get(0).get(0);

    responseBuilder.append("Nums of records (skipUpsert)").append(TAB).append(recordNumsSkipUpsert.asText())
        .append(NEW_LINE);

    // Estimate total Key/Value space based on unique key combinations.
    // Keycombination = messageRate * retentionValue * (1 - upsertFrequency).
    // KeySpace = bytesPerKey * uniqueCombinations
    // ValueSpace = bytesPerValue * uniqueCombinations
    if (recordNumsSkipUpsert.asLong() != 0) {
      float appendFrequency = recordNums.asLong() / recordNumsSkipUpsert.asLong();
      float totalKeySpace =
          bytesPerKey * _messageRate * Integer.valueOf(retentionTimeValue) * 24 * 3600 * appendFrequency / (1024 * 1024
              * 1024);
      float totalValueSpace =
          bytesPerValue * _messageRate * Integer.valueOf(retentionTimeValue) * 24 * 3600 * appendFrequency / (1024
              * 1024 * 1024);

      responseBuilder.append("Upsert frequency").append(TAB).append(1 - appendFrequency).append(NEW_LINE);
      responseBuilder.append("Estimated total key space (GB)").append(TAB).append(totalKeySpace).append(NEW_LINE);
      responseBuilder.append("Estimated total value space (GB)").append(TAB).append(totalValueSpace).append(NEW_LINE);
    }

    return responseBuilder.toString();
  }

  @Override
  public boolean execute()
      throws Exception {
    String result = run();
    LOGGER.info("Result: " + result);
    return true;
  }
}
