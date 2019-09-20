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
package org.apache.pinot.benchmark.api;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.core.Response;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.pinot.benchmark.PinotBenchConf;
import org.apache.pinot.benchmark.api.resources.CreatePerfTableRequest;
import org.apache.pinot.benchmark.api.resources.PinotBenchException;
import org.apache.pinot.benchmark.api.resources.SuccessResponse;
import org.apache.pinot.benchmark.common.utils.PerfTableMode;
import org.apache.pinot.benchmark.common.utils.PinotBenchConstants;
import org.apache.pinot.benchmark.common.utils.PinotBenchConstants.PerfTableCreationParameters;
import org.apache.pinot.benchmark.common.utils.PinotBenchConstants.PinotServerType;
import org.apache.pinot.benchmark.common.utils.PinotBenchConstants.TenantTags;
import org.apache.pinot.benchmark.common.utils.PinotClusterClient;
import org.apache.pinot.benchmark.common.utils.PinotClusterLocator;
import org.apache.pinot.common.config.IndexingConfig;
import org.apache.pinot.common.config.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.config.TableCustomConfig;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.config.TenantConfig;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.JsonUtils;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.pql.parsers.utils.Pair;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotClusterManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotClusterManager.class);

  private PinotBenchConf _conf;
  private PinotClusterClient _pinotClusterClient;
  private PinotClusterLocator _pinotClusterLocator;
  private HelixManager _helixZkManager;
  private HelixAdmin _helixAdmin;

  public PinotClusterManager(PinotBenchConf conf, PinotClusterClient pinotClusterClient, PinotClusterLocator pinotClusterLocator) {
    _conf = conf;
    _pinotClusterClient = pinotClusterClient;
    _pinotClusterLocator = pinotClusterLocator;
  }

  public void start(HelixManager helixManager) {
    _helixZkManager = helixManager;
    _helixAdmin = _helixZkManager.getClusterManagmentTool();
  }

  public List<String> listTables(String clusterType) {
    Pair<String, Integer> pair = _pinotClusterLocator.getPinotClusterFromType(clusterType);
    List<String> tableNames = new ArrayList<>();
    try {
      SimpleHttpResponse response = _pinotClusterClient
          .sendGetRequest(PinotClusterClient.getListTablesHttpURI(pair.getFirst(), pair.getSecond()));
      JsonNode jsonArray = JsonUtils.stringToJsonNode(response.getResponse()).get(PinotBenchConstants.TABLES_PATH);

      Iterator<JsonNode> elements = jsonArray.elements();
      while (elements.hasNext()) {
        tableNames.add(elements.next().asText());
      }
      return tableNames;
    } catch (HttpErrorStatusException e) {
      throw new PinotBenchException(LOGGER, e.getMessage(), e.getStatusCode(), e);
    } catch (Exception e) {
      throw new PinotBenchException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  public String getTableConfig(String clusterType, String tableName) {
    Pair<String, Integer> pair = _pinotClusterLocator.getPinotClusterFromType(clusterType);
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    try {
      SimpleHttpResponse response = _pinotClusterClient.sendGetRequest(
          PinotClusterClient.getRetrieveTableConfigHttpURI(pair.getFirst(), pair.getSecond(), rawTableName));
      return response.getResponse();
    } catch (HttpErrorStatusException e) {
      throw new PinotBenchException(LOGGER, e.getMessage(), e.getStatusCode(), e);
    } catch (Exception e) {
      throw new PinotBenchException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  public String addTableConfig(FormDataMultiPart multiPart) {
    FormDataBodyPart bodyPart = multiPart.getFields().values().iterator().next().get(0);
    String tableConfigStr = bodyPart.getValueAs(String.class);
    return addTableConfig(tableConfigStr);
  }

  private String addTableConfig(TableConfig tableConfig) {
    if (tableConfig == null) {
      return null;
    }
    return addTableConfig(tableConfig.toJsonConfigString());
  }

  private String addTableConfig(String tableConfigStr) {
    if (tableConfigStr == null) {
      return null;
    }
    Pair<String, Integer> pair = _pinotClusterLocator.getPerfClusterPair();
    try {
      SimpleHttpResponse response = _pinotClusterClient.sendRequest(PinotClusterClient
          .getAddTableConfigRequest(PinotClusterClient.getListTablesHttpURI(pair.getFirst(), pair.getSecond()), "table",
              tableConfigStr));
      LOGGER.info("Add table successfully: " + response.getResponse());
      return response.getResponse();
    } catch (HttpErrorStatusException e) {
      throw new PinotBenchException(LOGGER, e.getMessage(), e.getStatusCode(), e);
    } catch (Exception e) {
      throw new PinotBenchException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  public String updateTableConfig(String tableName, String tableConfigStr) {
    Pair<String, Integer> pair = _pinotClusterLocator.getPerfClusterPair();
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    try {
      SimpleHttpResponse response = _pinotClusterClient.sendRequest(PinotClusterClient.getUpdateTableConfigRequest(
          PinotClusterClient.getRetrieveTableConfigHttpURI(pair.getFirst(), pair.getSecond(), rawTableName),
          rawTableName, tableConfigStr));
      LOGGER.info("Add table successfully: " + response.getResponse());
      return response.getResponse();
    } catch (HttpErrorStatusException e) {
      throw new PinotBenchException(LOGGER, e.getMessage(), e.getStatusCode(), e);
    } catch (Exception e) {
      throw new PinotBenchException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  public String deleteTableConfig(String tableName, String tableTypeStr) {
    Pair<String, Integer> pair = _pinotClusterLocator.getPerfClusterPair();
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    if (tableTypeStr == null) {
      CommonConstants.Helix.TableType type = TableNameBuilder.getTableTypeFromTableName(tableName);
      if (type != null) {
        tableTypeStr = type.name();
      }
    }
    try {
      SimpleHttpResponse response = _pinotClusterClient.sendRequest(PinotClusterClient.getDeleteTableConfigHttpURI(
          PinotClusterClient.getRetrieveTableConfigHttpURI(pair.getFirst(), pair.getSecond(), rawTableName),
          rawTableName, tableTypeStr));
      return response.getResponse();
    } catch (HttpErrorStatusException e) {
      throw new PinotBenchException(LOGGER, e.getMessage(), e.getStatusCode(), e);
    } catch (Exception e) {
      throw new PinotBenchException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  public String listSchemas(String clusterType) {
    Pair<String, Integer> pair = _pinotClusterLocator.getPinotClusterFromType(clusterType);
    try {
      SimpleHttpResponse response = _pinotClusterClient
          .sendGetRequest(PinotClusterClient.getListSchemasHttpURI(pair.getFirst(), pair.getSecond()));
      return response.getResponse();
    } catch (HttpErrorStatusException e) {
      throw new PinotBenchException(LOGGER, e.getMessage(), e.getStatusCode(), e);
    } catch (Exception e) {
      throw new PinotBenchException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  public String getSchema(String clusterType, String schemaName) {
    Pair<String, Integer> pair = _pinotClusterLocator.getPinotClusterFromType(clusterType);
    try {
      SimpleHttpResponse response = _pinotClusterClient
          .sendGetRequest(PinotClusterClient.getSchemaHTTPURI(pair.getFirst(), pair.getSecond(), schemaName));
      return response.getResponse();
    } catch (HttpErrorStatusException e) {
      throw new PinotBenchException(LOGGER, e.getMessage(), e.getStatusCode(), e);
    } catch (Exception e) {
      throw new PinotBenchException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  public String addSchema(Schema schema) {
    String schemaStr = schema.toPrettyJsonString();
    InputStream schemaInputStream = new ByteArrayInputStream(schemaStr.getBytes());
    return addSchema(schemaInputStream);
  }

  public String addSchema(FormDataMultiPart multiPart) {
    FormDataBodyPart bodyPart = multiPart.getFields().values().iterator().next().get(0);
    InputStream schemaInputStream = bodyPart.getValueAs(InputStream.class);
    return addSchema(schemaInputStream);
  }

  public String addSchema(InputStream schemaInputStream) {
    Pair<String, Integer> pair = _pinotClusterLocator.getPerfClusterPair();
    try {
      SimpleHttpResponse response = _pinotClusterClient.sendRequest(PinotClusterClient
          .getAddSchemaRequest(PinotClusterClient.getListSchemasHttpURI(pair.getFirst(), pair.getSecond()), "schema",
              schemaInputStream));
      LOGGER.info("Add schema successfully: " + response.getResponse());
      return response.getResponse();
    } catch (HttpErrorStatusException e) {
      throw new PinotBenchException(LOGGER, e.getMessage(), e.getStatusCode(), e);
    } catch (Exception e) {
      throw new PinotBenchException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  public String updateSchema(String schemaName, FormDataMultiPart multiPart) {
    Pair<String, Integer> pair = _pinotClusterLocator.getPerfClusterPair();
    FormDataBodyPart bodyPart = multiPart.getFields().values().iterator().next().get(0);
    InputStream schemaInputStream = bodyPart.getValueAs(InputStream.class);

    try {
      SimpleHttpResponse response = _pinotClusterClient.sendRequest(PinotClusterClient
          .getUpdateSchemaRequest(PinotClusterClient.getSchemaHTTPURI(pair.getFirst(), pair.getSecond(), schemaName),
              schemaName, schemaInputStream));
      return response.getResponse();
    } catch (HttpErrorStatusException e) {
      throw new PinotBenchException(LOGGER, e.getMessage(), e.getStatusCode(), e);
    } catch (Exception e) {
      throw new PinotBenchException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  public String deleteSchema(String schemaName) {
    Pair<String, Integer> pair = _pinotClusterLocator.getPerfClusterPair();
    try {
      SimpleHttpResponse response = _pinotClusterClient
          .sendDeleteRequest(PinotClusterClient.getSchemaHTTPURI(pair.getFirst(), pair.getSecond(), schemaName));
      return response.getResponse();
    } catch (HttpErrorStatusException e) {
      throw new PinotBenchException(LOGGER, e.getMessage(), e.getStatusCode(), e);
    } catch (Exception e) {
      throw new PinotBenchException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  public SuccessResponse createBenchmarkTable(FormDataMultiPart multiPart) {
    CreatePerfTableRequest createPerfTableRequest = parseRequest(multiPart);

    List<String> tableNames = listTables(PinotClusterLocator.PinotClusterType.PERF.getClusterType());
    if (tableNames.contains(createPerfTableRequest.getRawTableName())) {
      throw new PinotBenchException(LOGGER, "Table " + createPerfTableRequest.getRawTableName() + " already exists",
          Response.Status.NOT_ACCEPTABLE);
    }

    Schema schema = validateSchema(createPerfTableRequest);
    TableConfig offlineTableConfig = validateOfflineTableConfig(createPerfTableRequest);
    TableConfig realtimeTableConfig = validateRealtimeTableConfig(createPerfTableRequest);

    // Assign brokers and servers here before adding schema and table configs.
    prepareInstances(createPerfTableRequest);

    addSchema(schema);

    try {
      // puts sleeps between two calls.
      Thread.sleep(100L);
      addTableConfig(offlineTableConfig);
      Thread.sleep(100L);
      addTableConfig(realtimeTableConfig);
      return new SuccessResponse("Create Table " + schema.getSchemaName() + " successfully");
    } catch (Exception e) {
      throw new PinotBenchException(LOGGER,
          "Exception when adding table config for Table: " + createPerfTableRequest.getRawTableName(),
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  private CreatePerfTableRequest parseRequest(FormDataMultiPart multiPart) {
    String tableName = null;
    String tableType = null;
    String mode = null;
    String ldap = null;
    String versionId = null;
    int tableRetention = -1;
    String tableConfig = null;
    String schema = null;
    int numBrokers = 0;
    int numOfflineServers = 0;
    int numRealtimeServers = 0;

    for (Map.Entry<String, List<FormDataBodyPart>> entry : multiPart.getFields().entrySet()) {
      String entryName = entry.getKey();
      List<FormDataBodyPart> dataBodyPartList = entry.getValue();
      throwExceptionIfViolated(dataBodyPartList.size() == 1, "The data body size should be 1");
      String bodyPartValue = dataBodyPartList.get(0).getValueAs(String.class);

      switch (entryName) {
        case PerfTableCreationParameters.TABLE_CONFIG_KEY:
          tableConfig = bodyPartValue;
          break;
        case PerfTableCreationParameters.TABLE_SCHEMA_KEY:
          schema = bodyPartValue;
          break;
        case PerfTableCreationParameters.TABLE_NAME_KEY:
          tableName = bodyPartValue;
          break;
        case PerfTableCreationParameters.TABLE_TYPE_KEY:
          tableType = bodyPartValue;
          break;
        case PerfTableCreationParameters.MODE_KEY:
          mode = bodyPartValue;
          break;
        case PerfTableCreationParameters.LDAP_KEY:
          ldap = bodyPartValue;
          break;
        case PerfTableCreationParameters.TABLE_RETENTION_KEY:
          tableRetention = Integer.parseInt(bodyPartValue);
          break;
        case PerfTableCreationParameters.VERSION_ID_KEY:
          versionId = bodyPartValue;
          break;
        case PerfTableCreationParameters.NUM_BROKERS_KEY:
          numBrokers = Integer.parseInt(bodyPartValue);
          break;
        case PerfTableCreationParameters.NUM_OFFLINE_SERVERS_KEY:
          numOfflineServers = Integer.parseInt(bodyPartValue);
          break;
        case PerfTableCreationParameters.NUM_REALTIME_SERVERS_KEY:
          numRealtimeServers = Integer.parseInt(bodyPartValue);
          break;
        default:
          throw new PinotBenchException(LOGGER, "Invalid name:" + entryName, Response.Status.BAD_REQUEST);
      }
    }

    PerfTableMode perfTableMode = PerfTableMode.getPerfTableMode(mode);
    throwExceptionIfViolated(perfTableMode != null, "Perf table mode should not be null. Current value: " + mode);
    throwExceptionIfViolated(ldap != null, "Ldap should not be null");
    throwExceptionIfViolated(tableRetention != -1,
        "Table retention should not be null. Current value: " + tableRetention);
    throwExceptionIfViolated(tableType != null, "Perf table type should not be null");
    throwExceptionIfViolated(tableName != null, "Table name should not be null");
    throwExceptionIfViolated(numBrokers >= 0, "Number of brokers should be >= 0");
    throwExceptionIfViolated(numOfflineServers >= 0, "Number of offline servers should be >= 0");
    throwExceptionIfViolated(numRealtimeServers >= 0, "Number of realtime servers should be >= 0");

    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    if (!rawTableName.endsWith(PinotBenchConstants.TABLE_TEST_SUFFIX)) {
      rawTableName = rawTableName + PinotBenchConstants.TABLE_TEST_SUFFIX;
    }
    if (versionId != null) {
      rawTableName = rawTableName + "_" + versionId;
    }

    if (perfTableMode == PerfTableMode.NON_MIRROR) {
      throwExceptionIfViolated(tableConfig != null, "Table config should not be null for table in non-mirror mode");
      throwExceptionIfViolated(schema != null, "Schema should not be null for table in non-mirror mode");
    } else {
      // Creates table in mirror mode.
      if (schema == null) {
        // Assume the schema name in prod is the same as the table name.
        schema = getSchema(PinotClusterLocator.PinotClusterType.PROD.getClusterType(), tableName);
      }
      if (tableConfig == null) {
        tableConfig = getTableConfig(PinotClusterLocator.PinotClusterType.PROD.getClusterType(), tableName);
      }
    }

    String offlineTableConfigStr = null;
    String realtimeTableConfigStr = null;
    JsonNode jsonNode;
    try {
      jsonNode = JsonUtils.stringToJsonNode(tableConfig);
    } catch (IOException e) {
      throw new PinotBenchException(LOGGER, "IOException when parsing string to json node.",
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    if (tableType.equalsIgnoreCase(PinotServerType.HYBRID) || tableType.equalsIgnoreCase(PinotServerType.OFFLINE)) {
      offlineTableConfigStr = jsonNode.get(PinotServerType.OFFLINE).toString();
    }
    if (tableType.equalsIgnoreCase(PinotServerType.HYBRID) || tableType.equalsIgnoreCase(PinotServerType.REALTIME)) {
      realtimeTableConfigStr = jsonNode.get(PinotServerType.REALTIME).toString();
    }

    String tenantTag = TenantTags.PINOT_BENCH_TENANT_TAG_PREFIX + ldap + "-" + rawTableName;
    return new CreatePerfTableRequest(rawTableName, tableName, tableType, perfTableMode, ldap, versionId,
        tableRetention, schema, offlineTableConfigStr, realtimeTableConfigStr, tenantTag, numBrokers, numOfflineServers,
        numRealtimeServers);
  }

  private Schema validateSchema(CreatePerfTableRequest createPerfTableRequest) {
    String rawTableName = TableNameBuilder.extractRawTableName(createPerfTableRequest.getRawTableName());
    String schemaStr = createPerfTableRequest.getSchema();
    Schema schema;
    try {
      schema = Schema.fromString(schemaStr);
    } catch (IOException e) {
      throw new PinotBenchException(LOGGER, "IOException when parsing schema for Table: " + rawTableName,
          Response.Status.BAD_REQUEST, e);
    }
    schema.setSchemaName(rawTableName);
    return schema;
  }

  private TableConfig validateOfflineTableConfig(CreatePerfTableRequest createPerfTableRequest) {
    return validateTableConfig(createPerfTableRequest, createPerfTableRequest.getOfflineTableConfig());
  }

  private TableConfig validateRealtimeTableConfig(CreatePerfTableRequest createPerfTableRequest) {
    return validateTableConfig(createPerfTableRequest, createPerfTableRequest.getRealtimeTableConfig());
  }

  private TableConfig validateTableConfig(CreatePerfTableRequest createPerfTableRequest, String tableConfigStr) {
    if (tableConfigStr == null) {
      return null;
    }
    TableConfig tableConfig;
    try {
      tableConfig = TableConfig.fromJsonString(tableConfigStr);
    } catch (IOException e) {
      throw new PinotBenchException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST);
    }

    SegmentsValidationAndRetentionConfig segmentConfig = tableConfig.getValidationConfig();
    if (segmentConfig.getReplicationNumber() != 1) {
      throw new PinotBenchException(LOGGER,
          "Replication should be 1. Current replication: " + segmentConfig.getReplicationNumber(),
          Response.Status.BAD_REQUEST);
    }

    TableCustomConfig customConfig = tableConfig.getCustomConfig();
    Map<String, String> customConfigMap = customConfig.getCustomConfigs();
    if (customConfigMap == null) {
      customConfigMap = new HashMap<>();
      customConfig.setCustomConfigs(customConfigMap);
    }

    String ldap = createPerfTableRequest.getLdap();
    String versionId = createPerfTableRequest.getVersionId();
    int tableRetention = createPerfTableRequest.getTableRetention();
    CommonConstants.Helix.TableType tableType = tableConfig.getTableType();

    long creationTimeMs = System.currentTimeMillis();
    long expirationTimeMs = creationTimeMs + TimeUnit.DAYS.toMillis(tableRetention);
    customConfigMap.put(PerfTableCreationParameters.CREATION_TIME_MS_KEY, Long.toString(creationTimeMs));
    customConfigMap.put(PerfTableCreationParameters.EXPIRATION_TIME_MS_KEY, Long.toString(expirationTimeMs));
    customConfigMap.put(PerfTableCreationParameters.TABLE_RETENTION_KEY, Integer.toString(tableRetention));
    customConfigMap.put(PerfTableCreationParameters.LDAP_KEY, ldap);
    customConfigMap.put(PerfTableCreationParameters.VERSION_ID_KEY, versionId);
    customConfigMap
        .put(PerfTableCreationParameters.NUM_BROKERS_KEY, Integer.toString(createPerfTableRequest.getNumBrokers()));

    if (tableType == CommonConstants.Helix.TableType.OFFLINE) {
      int numOfflineServers = createPerfTableRequest.getNumOfflineServers();

      // Set numInstancesPerPartition to the number of hosts specified in the params for offline table
      String segmentAssignmentStrategy = segmentConfig.getSegmentAssignmentStrategy();
      if (CommonConstants.Helix.DataSource.SegmentAssignmentStrategyType.valueOf(segmentAssignmentStrategy)
          == CommonConstants.Helix.DataSource.SegmentAssignmentStrategyType.ReplicaGroupSegmentAssignmentStrategy) {
        segmentConfig.setReplicasPerPartition(Integer.toString(numOfflineServers));
      }

      // Add number of hosts to custom config.
      customConfigMap.put(PerfTableCreationParameters.NUM_OFFLINE_SERVERS_KEY,
          Integer.toString(createPerfTableRequest.getNumOfflineServers()));
    } else {
      // Set replicasPerPartition to 1
      segmentConfig.setReplicasPerPartition("1");

      IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
      Map<String, String> streamConfigsMap = indexingConfig.getStreamConfigs();

      // Set kafka auto offset to “largest”
      streamConfigsMap.put("stream.kafka.consumer.prop.auto.offset.reset", "largest");

      // Add number of hosts to custom config.
      customConfigMap.put(PerfTableCreationParameters.NUM_REALTIME_SERVERS_KEY,
          Integer.toString(createPerfTableRequest.getNumRealtimeServers()));
    }

    String rawTableName = createPerfTableRequest.getRawTableName();
    tableConfig.setTableName(TableNameBuilder.forType(tableType).tableNameWithType(rawTableName));

    TenantConfig tenantConfig = tableConfig.getTenantConfig();
    String tenantTag = createPerfTableRequest.getTenantTag();
    tenantConfig.setBroker(tenantTag);
    tenantConfig.setServer(tenantTag);
    tableConfig.setTenantConfig(tenantConfig);
    return tableConfig;
  }

  private void prepareInstances(CreatePerfTableRequest createPerfTableRequest) {
    String tenantTag = createPerfTableRequest.getTenantTag();
    int numBrokers = createPerfTableRequest.getNumBrokers();
    int numOfflineServers = createPerfTableRequest.getNumOfflineServers();
    int numRealtimeServers = createPerfTableRequest.getNumRealtimeServers();

    List<String> spareBrokerInstances = _helixAdmin
        .getInstancesInClusterWithTag(_helixZkManager.getClusterName(), TenantTags.DEFAULT_UNTAGGED_BROKER_TAG);
    List<String> spareServerInstances = _helixAdmin
        .getInstancesInClusterWithTag(_helixZkManager.getClusterName(), TenantTags.DEFAULT_UNTAGGED_SERVER_TAG);

    if (spareBrokerInstances.size() < numBrokers) {
      throw new PinotBenchException(LOGGER,
          "Not enough spare brokers. Current number of spare brokers: " + spareBrokerInstances.size()
              + ", requested number of brokers: " + numBrokers, Response.Status.BAD_REQUEST);
    }

    if (spareServerInstances.size() < (numOfflineServers + numRealtimeServers)) {
      throw new PinotBenchException(LOGGER,
          "Not enough spare servers. Current number of spare servers: " + spareServerInstances.size()
              + ", requested number of offline servers: " + numOfflineServers
              + ", requested number of realtime servers: " + numRealtimeServers, Response.Status.BAD_REQUEST);
    }

    // TODO: selects instances randomly
    List<String> brokersInUse = new ArrayList<>();
    for (int i = 0; i < numBrokers; i++) {
      String instanceName = spareBrokerInstances.get(i);
      _helixAdmin
          .removeInstanceTag(_helixZkManager.getClusterName(), instanceName, TenantTags.DEFAULT_UNTAGGED_BROKER_TAG);
      _helixAdmin.addInstanceTag(_helixZkManager.getClusterName(), instanceName, tenantTag + TenantTags.BROKER_SUFFIX);
      brokersInUse.add(instanceName);
    }

    List<String> offlineServersInUse = new ArrayList<>();
    List<String> realtimeServersInUse = new ArrayList<>();
    int numServers = numOfflineServers + numRealtimeServers;
    // TODO: selects instances randomly
    for (int i = 0, j = 0; i < numServers; i++, j++) {
      String instanceName = spareServerInstances.get(i);
      String tagSuffix = (j < numOfflineServers) ? TenantTags.OFFLINE_SERVER_SUFFIX : TenantTags.REALTIME_SERVER_SUFFIX;
      _helixAdmin
          .removeInstanceTag(_helixZkManager.getClusterName(), instanceName, TenantTags.DEFAULT_UNTAGGED_SERVER_TAG);
      _helixAdmin.addInstanceTag(_helixZkManager.getClusterName(), instanceName, tenantTag + tagSuffix);

      if (j < numOfflineServers) {
        offlineServersInUse.add(instanceName);
      } else {
        realtimeServersInUse.add(instanceName);
      }
    }
    LOGGER.info("Finish preparing brokers and servers. Brokers: {}, offline servers: {}, realtime servers: {}",
        brokersInUse, offlineServersInUse, realtimeServersInUse);
  }

  /**
   * Similar to Precondition.checkState, but it throws PinotBenchException.
   */
  private void throwExceptionIfViolated(boolean expression, String errorMessage) {
    if (!expression) {
      throw new PinotBenchException(LOGGER, errorMessage, Response.Status.BAD_REQUEST);
    }
  }
}
