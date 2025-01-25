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
package org.apache.pinot.controller.helix;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.controller.api.resources.PauseStatusDetails;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.tenant.Tenant;
import org.apache.pinot.spi.config.tenant.TenantRole;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.ControllerRequestURLBuilder;


/**
 * The {@code ControllerRequestClient} provides handy utilities to make request to controller.
 *
 * <p>It should be provided with a specified {@link ControllerRequestURLBuilder} for constructing the URL requests
 * as well as a reusable {@link HttpClient} during construction.
 */
public class ControllerRequestClient {
  private final HttpClient _httpClient;
  private final ControllerRequestURLBuilder _controllerRequestURLBuilder;
  private final Map<String, String> _headers;

  public ControllerRequestClient(ControllerRequestURLBuilder controllerRequestUrlBuilder, HttpClient httpClient) {
    this(controllerRequestUrlBuilder, httpClient, Collections.emptyMap());
  }

  public ControllerRequestClient(ControllerRequestURLBuilder controllerRequestUrlBuilder, HttpClient httpClient,
      Map<String, String> headers) {
    _controllerRequestURLBuilder = controllerRequestUrlBuilder;
    _httpClient = httpClient;
    _headers = headers;
  }

  public ControllerRequestURLBuilder getControllerRequestURLBuilder() {
    return _controllerRequestURLBuilder;
  }
  /**
   * Add a schema to the controller.
   */
  public void addSchema(Schema schema)
      throws IOException {
    String url = _controllerRequestURLBuilder.forSchemaCreate();
    try {
      HttpClient.wrapAndThrowHttpException(
          _httpClient.sendMultipartPostRequest(url, schema.toSingleLineJsonString(), _headers));
    } catch (HttpErrorStatusException e) {
      throw new IOException(e);
    }
  }

  public Schema getSchema(String schemaName)
      throws IOException {
    String url = _controllerRequestURLBuilder.forSchemaGet(schemaName);
    try {
      SimpleHttpResponse resp =
          HttpClient.wrapAndThrowHttpException(_httpClient.sendGetRequest(new URI(url), _headers));
      return Schema.fromString(resp.getResponse());
    } catch (HttpErrorStatusException | URISyntaxException e) {
      throw new IOException(e);
    }
  }

  public void updateSchema(Schema schema)
      throws IOException {
    String url = _controllerRequestURLBuilder.forSchemaUpdate(schema.getSchemaName());
    try {
      HttpClient.wrapAndThrowHttpException(
          _httpClient.sendMultipartPutRequest(url, schema.toSingleLineJsonString(), _headers));
    } catch (HttpErrorStatusException e) {
      throw new IOException(e);
    }
  }

  public void deleteSchema(String schemaName)
      throws IOException {
    String url = _controllerRequestURLBuilder.forSchemaDelete(schemaName);
    try {
      HttpClient.wrapAndThrowHttpException(_httpClient.sendDeleteRequest(new URI(url), _headers));
    } catch (HttpErrorStatusException | URISyntaxException e) {
      throw new IOException(e);
    }
  }

  public void addTableConfig(TableConfig tableConfig)
      throws IOException {
    try {
      HttpClient.wrapAndThrowHttpException(
          _httpClient.sendJsonPostRequest(new URI(_controllerRequestURLBuilder.forTableCreate()),
              tableConfig.toJsonString(), _headers));
    } catch (HttpErrorStatusException | URISyntaxException e) {
      throw new IOException(e);
    }
  }

  public void updateTableConfig(TableConfig tableConfig)
      throws IOException {
    try {
      HttpClient.wrapAndThrowHttpException(_httpClient.sendJsonPutRequest(
          new URI(_controllerRequestURLBuilder.forUpdateTableConfig(tableConfig.getTableName())),
          tableConfig.toJsonString(), _headers));
    } catch (HttpErrorStatusException | URISyntaxException e) {
      throw new IOException(e);
    }
  }

  public void deleteTable(String tableNameWithType)
      throws IOException {
    try {
      HttpClient.wrapAndThrowHttpException(
          _httpClient.sendDeleteRequest(new URI(_controllerRequestURLBuilder.forTableDelete(tableNameWithType)),
              _headers));
    } catch (HttpErrorStatusException | URISyntaxException e) {
      throw new IOException(e);
    }
  }

  public TableConfig getTableConfig(String tableName, TableType tableType)
      throws IOException {
    try {
      SimpleHttpResponse response = HttpClient.wrapAndThrowHttpException(
          _httpClient.sendGetRequest(new URI(_controllerRequestURLBuilder.forTableGet(tableName)), _headers));
      return JsonUtils.jsonNodeToObject(JsonUtils.stringToJsonNode(response.getResponse()).get(tableType.toString()),
          TableConfig.class);
    } catch (HttpErrorStatusException | URISyntaxException e) {
      throw new IOException(e);
    }
  }

  public long getTableSize(String tableName)
      throws IOException {
    try {
      SimpleHttpResponse response = HttpClient.wrapAndThrowHttpException(
          _httpClient.sendGetRequest(new URI(_controllerRequestURLBuilder.forTableSize(tableName)), _headers));
      return Long.parseLong(JsonUtils.stringToJsonNode(response.getResponse()).get("reportedSizeInBytes").asText());
    } catch (HttpErrorStatusException | URISyntaxException e) {
      throw new IOException(e);
    }
  }

  public void resetTable(String tableNameWithType, String targetInstance)
      throws IOException {
    try {
      HttpClient.wrapAndThrowHttpException(_httpClient.sendJsonPostRequest(
          new URI(_controllerRequestURLBuilder.forTableReset(tableNameWithType, targetInstance)), null, _headers));
    } catch (HttpErrorStatusException | URISyntaxException e) {
      throw new IOException(e);
    }
  }

  public void resetSegment(String tableNameWithType, String segmentName, String targetInstance)
      throws IOException {
    try {
      HttpClient.wrapAndThrowHttpException(_httpClient.sendJsonPostRequest(
          new URI(_controllerRequestURLBuilder.forSegmentReset(tableNameWithType, segmentName, targetInstance)), null,
          _headers));
    } catch (HttpErrorStatusException | URISyntaxException e) {
      throw new IOException(e);
    }
  }

  public String reloadTable(String tableName, TableType tableType, boolean forceDownload)
      throws IOException {
    try {
      SimpleHttpResponse simpleHttpResponse = HttpClient.wrapAndThrowHttpException(_httpClient.sendJsonPostRequest(
          new URI(_controllerRequestURLBuilder.forTableReload(tableName, tableType, forceDownload)), null, _headers));
      return simpleHttpResponse.getResponse();
    } catch (HttpErrorStatusException | URISyntaxException e) {
      throw new IOException(e);
    }
  }

  public String checkIfReloadIsNeeded(String tableNameWithType, Boolean verbose)
      throws IOException {
    try {
      SimpleHttpResponse simpleHttpResponse = HttpClient.wrapAndThrowHttpException(_httpClient.sendGetRequest(
          new URI(_controllerRequestURLBuilder.forTableNeedReload(tableNameWithType, verbose)), _headers, null));
      return simpleHttpResponse.getResponse();
    } catch (HttpErrorStatusException | URISyntaxException e) {
      throw new IOException(e);
    }
  }

  public void reloadSegment(String tableName, String segmentName, boolean forceReload)
      throws IOException {
    try {
      HttpClient.wrapAndThrowHttpException(_httpClient.sendJsonPostRequest(
          new URI(_controllerRequestURLBuilder.forSegmentReload(tableName, segmentName, forceReload)), null, _headers));
    } catch (HttpErrorStatusException | URISyntaxException e) {
      throw new IOException(e);
    }
  }

  public List<String> listSegments(String tableName, @Nullable String tableType, boolean excludeReplacedSegments)
      throws IOException {
    String url = _controllerRequestURLBuilder.forSegmentListAPI(tableName, tableType, excludeReplacedSegments);
    try {
      SimpleHttpResponse resp =
          HttpClient.wrapAndThrowHttpException(_httpClient.sendGetRequest(new URI(url), _headers));
      // Example response: (list of map from table type to segments)
      // [{"REALTIME":["mytable__0__0__20221012T1952Z","mytable__1__0__20221012T1952Z"]}]
      JsonNode jsonNode = JsonUtils.stringToJsonNode(resp.getResponse());
      List<String> segments = new ArrayList<>();
      for (JsonNode tableNode : jsonNode) {
        ArrayNode segmentsNode = (ArrayNode) tableNode.elements().next();
        for (JsonNode segmentNode : segmentsNode) {
          segments.add(segmentNode.asText());
        }
      }
      return segments;
    } catch (HttpErrorStatusException | URISyntaxException e) {
      throw new IOException(e);
    }
  }

  public Map<String, List<String>> getServersToSegmentsMap(String tableName, TableType tableType)
      throws IOException {
    String url = _controllerRequestURLBuilder.forServersToSegmentsMap(tableName, tableType.toString());
    try {
      SimpleHttpResponse resp =
          HttpClient.wrapAndThrowHttpException(_httpClient.sendGetRequest(new URI(url), _headers));
      JsonNode jsonNode = JsonUtils.stringToJsonNode(resp.getResponse());
      if (jsonNode == null || jsonNode.get(0) == null) {
        return Collections.emptyMap();
      }

      JsonNode serversMap = jsonNode.get(0).get("serverToSegmentsMap");
      if (serversMap == null) {
        return Collections.emptyMap();
      }

      HashMap<String, List<String>> result = new HashMap<>();
      Iterator<Map.Entry<String, JsonNode>> fields = serversMap.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> field = fields.next();
        List<String> segments = new ArrayList<>();

        ArrayNode value = (ArrayNode) field.getValue();
        for (int i = 0, len = value.size(); i < len; i++) {
          segments.add(value.get(i).toString());
        }

        result.put(field.getKey(), segments);
      }

      return result;
    } catch (HttpErrorStatusException | URISyntaxException e) {
      throw new IOException(e);
    }
  }

  public void deleteSegment(String tableName, String segmentName)
      throws IOException {
    try {
      HttpClient.wrapAndThrowHttpException(_httpClient.sendDeleteRequest(
          new URI(_controllerRequestURLBuilder.forSegmentDelete(tableName, segmentName)), _headers));
    } catch (HttpErrorStatusException | URISyntaxException e) {
      throw new IOException(e);
    }
  }

  public void deleteSegments(String tableName, TableType tableType)
      throws IOException {
    try {
      HttpClient.wrapAndThrowHttpException(_httpClient.sendDeleteRequest(
          new URI(_controllerRequestURLBuilder.forSegmentDeleteAll(tableName, tableType.toString())), _headers));
    } catch (HttpErrorStatusException | URISyntaxException e) {
      throw new IOException(e);
    }
  }

  public PauseStatusDetails pauseConsumption(String tableName)
      throws IOException {
    try {
      SimpleHttpResponse response = HttpClient.wrapAndThrowHttpException(
          _httpClient.sendJsonPostRequest(new URI(_controllerRequestURLBuilder.forPauseConsumption(tableName)), null,
              _headers));
      return JsonUtils.stringToObject(response.getResponse(), PauseStatusDetails.class);
    } catch (HttpErrorStatusException | URISyntaxException e) {
      throw new IOException(e);
    }
  }

  public PauseStatusDetails resumeConsumption(String tableName)
      throws IOException {
    try {
      SimpleHttpResponse response = HttpClient.wrapAndThrowHttpException(
          _httpClient.sendJsonPostRequest(new URI(_controllerRequestURLBuilder.forResumeConsumption(tableName)), null,
              _headers));
      return JsonUtils.stringToObject(response.getResponse(), PauseStatusDetails.class);
    } catch (HttpErrorStatusException | URISyntaxException e) {
      throw new IOException(e);
    }
  }

  public PauseStatusDetails getPauseStatusDetails(String tableName)
      throws IOException {
    try {
      SimpleHttpResponse response = HttpClient.wrapAndThrowHttpException(
          _httpClient.sendGetRequest(new URI(_controllerRequestURLBuilder.forPauseStatus(tableName)), _headers));
      return JsonUtils.stringToObject(response.getResponse(), PauseStatusDetails.class);
    } catch (HttpErrorStatusException | URISyntaxException e) {
      throw new IOException(e);
    }
  }

  public void createBrokerTenant(String tenantName, int numBrokers)
      throws IOException {
    try {
      HttpClient.wrapAndThrowHttpException(
          _httpClient.sendJsonPostRequest(new URI(_controllerRequestURLBuilder.forTenantCreate()),
              getBrokerTenantRequestPayload(tenantName, numBrokers), _headers));
    } catch (HttpErrorStatusException | URISyntaxException e) {
      throw new IOException(e);
    }
  }

  public void updateBrokerTenant(String tenantName, int numBrokers)
      throws IOException {
    try {
      HttpClient.wrapAndThrowHttpException(
          _httpClient.sendJsonPutRequest(new URI(_controllerRequestURLBuilder.forTenantCreate()),
              getBrokerTenantRequestPayload(tenantName, numBrokers), _headers));
    } catch (HttpErrorStatusException | URISyntaxException e) {
      throw new IOException(e);
    }
  }

  public void deleteBrokerTenant(String tenantName)
      throws IOException {
    try {
      HttpClient.wrapAndThrowHttpException(
          _httpClient.sendDeleteRequest(new URI(_controllerRequestURLBuilder.forBrokerTenantDelete(tenantName)),
              _headers));
    } catch (HttpErrorStatusException | URISyntaxException e) {
      throw new IOException(e);
    }
  }

  public void createServerTenant(String tenantName, int numOfflineServers, int numRealtimeServers)
      throws IOException {
    try {
      HttpClient.wrapAndThrowHttpException(
          _httpClient.sendJsonPostRequest(new URI(_controllerRequestURLBuilder.forTenantCreate()),
              getServerTenantRequestPayload(tenantName, numOfflineServers, numRealtimeServers), _headers));
    } catch (HttpErrorStatusException | URISyntaxException e) {
      throw new IOException(e);
    }
  }

  public void updateServerTenant(String tenantName, int numOfflineServers, int numRealtimeServers)
      throws IOException {
    try {
      HttpClient.wrapAndThrowHttpException(
          _httpClient.sendJsonPutRequest(new URI(_controllerRequestURLBuilder.forTenantCreate()),
              getServerTenantRequestPayload(tenantName, numOfflineServers, numRealtimeServers), _headers));
    } catch (HttpErrorStatusException | URISyntaxException e) {
      throw new IOException(e);
    }
  }

  public void runPeriodicTask(String taskName)
      throws IOException {
    try {
      HttpClient.wrapAndThrowHttpException(
          _httpClient.sendGetRequest(new URI(_controllerRequestURLBuilder.forPeriodTaskRun(taskName)), _headers));
    } catch (HttpErrorStatusException | URISyntaxException e) {
      throw new IOException(e);
    }
  }

  protected String getBrokerTenantRequestPayload(String tenantName, int numBrokers) {
    return new Tenant(TenantRole.BROKER, tenantName, numBrokers, 0, 0).toJsonString();
  }

  protected static String getServerTenantRequestPayload(String tenantName, int numOfflineServers,
      int numRealtimeServers) {
    return new Tenant(TenantRole.SERVER, tenantName, numOfflineServers + numRealtimeServers, numOfflineServers,
        numRealtimeServers).toJsonString();
  }
}
