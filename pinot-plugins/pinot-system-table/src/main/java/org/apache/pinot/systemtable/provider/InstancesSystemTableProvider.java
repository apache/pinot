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
package org.apache.pinot.systemtable.provider;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.helix.HelixAdmin;
import org.apache.pinot.client.admin.PinotAdminClient;
import org.apache.pinot.client.admin.PinotAdminTransport;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.systemtable.SystemTable;
import org.apache.pinot.common.systemtable.SystemTableProvider;
import org.apache.pinot.common.systemtable.datasource.InMemorySystemTableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.InstanceTypeUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Basic system table exposing Pinot instance metadata.
 */
@SystemTable
public final class InstancesSystemTableProvider implements SystemTableProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(InstancesSystemTableProvider.class);
  public static final String TABLE_NAME = "system.instances";

  private static final String CONTROLLER_TIMEOUT_MS_PROPERTY = "pinot.systemtable.instances.controllerTimeoutMs";
  private static final long DEFAULT_CONTROLLER_TIMEOUT_MS = Duration.ofSeconds(5).toMillis();
  private static final long CONTROLLER_TIMEOUT_MS = getPositiveLongProperty(CONTROLLER_TIMEOUT_MS_PROPERTY,
      DEFAULT_CONTROLLER_TIMEOUT_MS);

  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
      .addSingleValueDimension("instanceId", FieldSpec.DataType.STRING)
      .addSingleValueDimension("type", FieldSpec.DataType.STRING)
      .addSingleValueDimension("host", FieldSpec.DataType.STRING)
      .addSingleValueDimension("port", FieldSpec.DataType.INT)
      .addSingleValueDimension("state", FieldSpec.DataType.STRING)
      .addSingleValueDimension("tags", FieldSpec.DataType.STRING)
      .build();

  private final @Nullable HelixAdmin _helixAdmin;
  private final @Nullable String _clusterName;
  private final List<String> _configuredControllerUrls;
  private final Map<String, PinotAdminClient> _adminClientCache = new ConcurrentHashMap<>();

  public InstancesSystemTableProvider() {
    this(null, null, null, null);
  }

  public InstancesSystemTableProvider(TableCache tableCache) {
    this(tableCache, null, null, null);
  }

  public InstancesSystemTableProvider(TableCache tableCache, @Nullable HelixAdmin helixAdmin) {
    this(tableCache, helixAdmin, null, null);
  }

  public InstancesSystemTableProvider(TableCache tableCache, @Nullable HelixAdmin helixAdmin,
      @Nullable String clusterName) {
    this(tableCache, helixAdmin, clusterName, null);
  }

  InstancesSystemTableProvider(TableCache tableCache, @Nullable HelixAdmin helixAdmin, @Nullable String clusterName,
      @Nullable List<String> controllerUrls) {
    _helixAdmin = helixAdmin;
    _clusterName = clusterName;
    _configuredControllerUrls = controllerUrls != null ? new ArrayList<>(controllerUrls) : List.of();
  }

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  @Override
  public Schema getSchema() {
    return SCHEMA;
  }

  @Override
  public TableConfig getTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
  }

  @Override
  public void close()
      throws Exception {
    for (Map.Entry<String, PinotAdminClient> entry : _adminClientCache.entrySet()) {
      try {
        entry.getValue().close();
      } catch (Exception e) {
        LOGGER.debug("Failed to close admin client for {}: {}", entry.getKey(), e.toString());
      }
    }
    _adminClientCache.clear();
  }

  @Override
  public IndexSegment getDataSource() {
    List<String> controllerBaseUrls = getControllerBaseUrls();
    if (controllerBaseUrls.isEmpty()) {
      return new InMemorySystemTableSegment(TABLE_NAME, SCHEMA, 0, Collections.emptyMap());
    }

    List<String> instances = fetchInstances(controllerBaseUrls);
    if (instances.isEmpty()) {
      return new InMemorySystemTableSegment(TABLE_NAME, SCHEMA, 0, Collections.emptyMap());
    }

    Set<String> liveInstances = fetchLiveInstances(controllerBaseUrls);

    List<InstanceRow> rows = new ArrayList<>(instances.size());
    for (String instanceId : instances) {
      rows.add(fetchInstanceRow(instanceId, liveInstances, controllerBaseUrls));
    }

    Map<String, java.util.function.IntFunction<Object>> valueProviders = new java.util.HashMap<>();
    valueProviders.put("instanceId", docId -> rows.get(docId)._instanceId);
    valueProviders.put("type", docId -> rows.get(docId)._type);
    valueProviders.put("host", docId -> rows.get(docId)._host);
    valueProviders.put("port", docId -> rows.get(docId)._port);
    valueProviders.put("state", docId -> rows.get(docId)._state);
    valueProviders.put("tags", docId -> rows.get(docId)._tags);

    return new InMemorySystemTableSegment(TABLE_NAME, SCHEMA, rows.size(), valueProviders);
  }

  private List<String> fetchInstances(List<String> controllerBaseUrls) {
    for (String baseUrl : controllerBaseUrls) {
      try {
        PinotAdminClient adminClient = getOrCreateAdminClient(baseUrl);
        if (adminClient == null) {
          continue;
        }
        return adminClient.getInstanceClient().listInstances();
      } catch (Exception e) {
        LOGGER.debug("{}: error fetching instances via {}", TABLE_NAME, baseUrl, e);
      }
    }
    return List.of();
  }

  private Set<String> fetchLiveInstances(List<String> controllerBaseUrls) {
    for (String baseUrl : controllerBaseUrls) {
      try {
        PinotAdminClient adminClient = getOrCreateAdminClient(baseUrl);
        if (adminClient == null) {
          continue;
        }
        return new java.util.HashSet<>(adminClient.getInstanceClient().listLiveInstances());
      } catch (Exception e) {
        LOGGER.debug("{}: error fetching live instances via {}", TABLE_NAME, baseUrl, e);
      }
    }
    return Set.of();
  }

  private InstanceRow fetchInstanceRow(String instanceId, Set<String> liveInstances, List<String> controllerBaseUrls) {
    JsonNode instanceInfo = fetchInstanceInfo(instanceId, controllerBaseUrls);
    boolean enabled = instanceInfo != null && instanceInfo.has("enabled") ? instanceInfo.path("enabled").asBoolean()
        : true;

    String host = instanceInfo != null ? instanceInfo.path("hostName").asText("") : "";
    int port = instanceInfo != null && instanceInfo.has("port") ? instanceInfo.path("port").asInt(-1) : -1;
    String tags = instanceInfo != null ? joinTags(instanceInfo.path("tags")) : "";

    if ((host.isEmpty() || port < 0)) {
      HostPort parsed = parseHostPort(instanceId);
      if (parsed != null) {
        if (host.isEmpty()) {
          host = parsed._host;
        }
        if (port < 0) {
          port = parsed._port;
        }
      }
    }

    String state;
    if (!enabled) {
      state = "DISABLED";
    } else if (liveInstances.contains(instanceId)) {
      state = "ONLINE";
    } else {
      state = "OFFLINE";
    }

    return new InstanceRow(instanceId, InstanceTypeUtils.getInstanceType(instanceId).name(), host, port, state, tags);
  }

  private @Nullable JsonNode fetchInstanceInfo(String instanceId, List<String> controllerBaseUrls) {
    for (String baseUrl : controllerBaseUrls) {
      try {
        PinotAdminClient adminClient = getOrCreateAdminClient(baseUrl);
        if (adminClient == null) {
          continue;
        }
        String response = adminClient.getInstanceClient().getInstance(instanceId);
        return JsonUtils.stringToJsonNode(response);
      } catch (Exception e) {
        LOGGER.debug("{}: error fetching instance info for {} via {}", TABLE_NAME, instanceId, baseUrl, e);
      }
    }
    return null;
  }

  private static String joinTags(JsonNode tagsNode) {
    if (tagsNode == null || tagsNode.isMissingNode() || tagsNode.isNull()) {
      return "";
    }
    if (!tagsNode.isArray()) {
      return tagsNode.asText(tagsNode.toString());
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < tagsNode.size(); i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append(tagsNode.get(i).asText());
    }
    return sb.toString();
  }

  private List<String> getControllerBaseUrls() {
    Set<String> urls = new LinkedHashSet<>();
    if (_helixAdmin != null) {
      for (String controller : discoverControllersFromHelix()) {
        String normalized = normalizeControllerUrl(controller);
        if (normalized != null) {
          urls.add(normalized);
        }
      }
    }
    for (String url : _configuredControllerUrls) {
      String normalized = normalizeControllerUrl(url);
      if (normalized != null) {
        urls.add(normalized);
      }
    }
    return new ArrayList<>(urls);
  }

  private List<String> discoverControllersFromHelix() {
    if (_helixAdmin == null) {
      return List.of();
    }
    if (_clusterName == null) {
      LOGGER.warn("Cannot discover controllers without cluster name");
      return List.of();
    }
    return HelixControllerUtils.discoverControllerBaseUrls(_helixAdmin, _clusterName, LOGGER);
  }

  private @Nullable PinotAdminClient getOrCreateAdminClient(String controllerBaseUrl) {
    String normalized = normalizeControllerUrl(controllerBaseUrl);
    if (normalized == null) {
      return null;
    }
    return _adminClientCache.computeIfAbsent(normalized, controller -> {
      try {
        String controllerAddress = stripScheme(controller);
        Properties properties = new Properties();
        properties.setProperty(PinotAdminTransport.PINOT_ADMIN_REQUEST_TIMEOUT_MS_PROPERTY_KEY,
            String.valueOf(CONTROLLER_TIMEOUT_MS));
        properties.setProperty(PinotAdminTransport.PINOT_ADMIN_SCHEME_PROPERTY_KEY,
            controller.startsWith("https://") ? "https" : "http");
        return new PinotAdminClient(controllerAddress, properties);
      } catch (Exception e) {
        LOGGER.warn("Failed to create admin client for {}: {}", controller, e.toString());
        return null;
      }
    });
  }

  private static @Nullable String normalizeControllerUrl(@Nullable String controllerUrl) {
    if (controllerUrl == null || controllerUrl.isEmpty()) {
      return null;
    }
    String normalized = controllerUrl;
    if (!normalized.startsWith("http://") && !normalized.startsWith("https://")) {
      normalized = "http://" + normalized;
    }
    if (normalized.endsWith("/")) {
      normalized = normalized.substring(0, normalized.length() - 1);
    }
    return normalized;
  }

  private static String stripScheme(String controllerUrl) {
    if (controllerUrl.startsWith("http://")) {
      return controllerUrl.substring("http://".length());
    }
    if (controllerUrl.startsWith("https://")) {
      return controllerUrl.substring("https://".length());
    }
    return controllerUrl;
  }

  private static long getPositiveLongProperty(String key, long defaultValue) {
    long value = Long.getLong(key, defaultValue);
    return value > 0 ? value : defaultValue;
  }

  private static final class InstanceRow {
    private final String _instanceId;
    private final String _type;
    private final String _host;
    private final int _port;
    private final String _state;
    private final String _tags;

    private InstanceRow(String instanceId, String type, String host, int port, String state, String tags) {
      _instanceId = instanceId;
      _type = type;
      _host = host;
      _port = port;
      _state = state;
      _tags = tags;
    }
  }

  private static final class HostPort {
    private final String _host;
    private final int _port;

    private HostPort(String host, int port) {
      _host = host;
      _port = port;
    }
  }

  private static @Nullable HostPort parseHostPort(String instanceId) {
    int lastUnderscore = instanceId.lastIndexOf('_');
    if (lastUnderscore <= 0 || lastUnderscore >= instanceId.length() - 1) {
      return null;
    }
    String portString = instanceId.substring(lastUnderscore + 1);
    int port;
    try {
      port = Integer.parseInt(portString);
    } catch (Exception e) {
      return null;
    }
    String host;
    int firstUnderscore = instanceId.indexOf('_');
    if (InstanceTypeUtils.isController(instanceId) || InstanceTypeUtils.isBroker(instanceId) || InstanceTypeUtils
        .isMinion(instanceId) || instanceId.startsWith("Server_")) {
      if (firstUnderscore <= 0 || firstUnderscore >= lastUnderscore) {
        return null;
      }
      host = instanceId.substring(firstUnderscore + 1, lastUnderscore);
    } else {
      host = instanceId.substring(0, lastUnderscore);
    }
    return host.isEmpty() ? null : new HostPort(host, port);
  }
}
