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
package org.apache.pinot.common.systemtable.provider;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.helix.HelixAdmin;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.systemtable.SystemTableDataProvider;
import org.apache.pinot.common.systemtable.SystemTableResponseUtils;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.systemtable.SystemTable;
import org.apache.pinot.spi.utils.InstanceTypeUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.FilterKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Basic system table exposing table-level metadata populated from the broker {@link TableCache}.
 */
@SystemTable
public final class TablesSystemTableProvider implements SystemTableDataProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(TablesSystemTableProvider.class);
  public static final String TABLE_NAME = "system.tables";
  private static final long SIZE_CACHE_TTL_MS = Duration.ofMinutes(1).toMillis();

  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
      .addSingleValueDimension("tableName", FieldSpec.DataType.STRING)
      .addSingleValueDimension("type", FieldSpec.DataType.STRING)
      .addSingleValueDimension("status", FieldSpec.DataType.STRING)
      .addSingleValueDimension("segments", FieldSpec.DataType.INT)
      .addSingleValueDimension("totalDocs", FieldSpec.DataType.LONG)
      .addMetric("reportedSize", FieldSpec.DataType.LONG)
      .addMetric("estimatedSize", FieldSpec.DataType.LONG)
      .addSingleValueDimension("storageTier", FieldSpec.DataType.STRING)
      .addSingleValueDimension("brokerTenant", FieldSpec.DataType.STRING)
      .addSingleValueDimension("serverTenant", FieldSpec.DataType.STRING)
      .addSingleValueDimension("replicas", FieldSpec.DataType.INT)
      .addSingleValueDimension("tableConfig", FieldSpec.DataType.STRING)
      .build();

  private static final boolean IS_ADMIN_CLIENT_AVAILABLE;
  static {
    boolean available;
    try {
      Class.forName("org.apache.pinot.client.admin.PinotAdminClient");
      available = true;
    } catch (ClassNotFoundException e) {
      available = false;
    }
    IS_ADMIN_CLIENT_AVAILABLE = available;
  }

  private final TableCache _tableCache;
  private final @Nullable HelixAdmin _helixAdmin;
  private final @Nullable String _clusterName;
  private final HttpClient _httpClient;
  private final @Nullable Function<String, TableSize> _tableSizeFetcherOverride;
  private final List<String> _staticControllerUrls;
  private final Map<String, CachedSize> _sizeCache = new ConcurrentHashMap<>();

  public TablesSystemTableProvider() {
    this(null, null, null, null, null);
  }

  public TablesSystemTableProvider(TableCache tableCache) {
    this(tableCache, null, null, null, null);
  }

  public TablesSystemTableProvider(TableCache tableCache, @Nullable HelixAdmin helixAdmin) {
    this(tableCache, helixAdmin, null, null, null);
  }

  public TablesSystemTableProvider(TableCache tableCache, @Nullable HelixAdmin helixAdmin,
      @Nullable String clusterName) {
    this(tableCache, helixAdmin, clusterName, null, null);
  }

  TablesSystemTableProvider(TableCache tableCache, @Nullable HelixAdmin helixAdmin, @Nullable String clusterName,
      @Nullable Function<String, TableSize> tableSizeFetcherOverride, @Nullable List<String> controllerUrls) {
    _tableCache = tableCache;
    _helixAdmin = helixAdmin;
    _clusterName = clusterName;
    _httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    _tableSizeFetcherOverride = tableSizeFetcherOverride;
    _staticControllerUrls = controllerUrls != null ? new ArrayList<>(controllerUrls) : List.of();
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
  public BrokerResponseNative getBrokerResponse(PinotQuery pinotQuery) {
    List<String> projectionColumns = pinotQuery.getSelectList() != null
        ? pinotQuery.getSelectList().stream().map(expr -> expr.getIdentifier().getName()).collect(Collectors.toList())
        : List.of();
    if (_tableCache == null) {
      return SystemTableResponseUtils.buildBrokerResponse(TABLE_NAME, SCHEMA, projectionColumns, List.of(), 0);
    }
    Set<String> tableNamesWithType = new LinkedHashSet<>();
    for (String tableName : _tableCache.getTableNameMap().values()) {
      if (TableNameBuilder.getTableTypeFromTableName(tableName) != null) {
        tableNamesWithType.add(tableName);
      }
    }
    List<String> sortedTableNames = new ArrayList<>(tableNamesWithType);
    sortedTableNames.sort(Comparator.naturalOrder());

    int offset = Math.max(0, pinotQuery.getOffset());
    int limit = pinotQuery.getLimit();
    boolean hasLimit = limit > 0;
    int totalRows = 0;
    int initialCapacity = hasLimit ? Math.min(sortedTableNames.size(), limit) : 0;
    List<GenericRow> rows = new ArrayList<>(initialCapacity);
    for (String tableNameWithType : sortedTableNames) {
      TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
      if (tableType == null) {
        continue;
      }
      String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
      TableStats stats = buildStats(tableNameWithType, tableType);
      if (!matchesFilter(pinotQuery.getFilterExpression(), stats, rawTableName)) {
        continue;
      }
      if (offset > 0) {
        offset--;
        totalRows++;
        continue;
      }
      totalRows++;
      if (limit == 0) {
        continue;
      }
      if (hasLimit && rows.size() >= limit) {
        continue;
      }
      GenericRow row = new GenericRow();
      row.putValue("tableName", rawTableName);
      row.putValue("type", stats._type);
      row.putValue("status", stats._status);
      row.putValue("segments", stats._segments);
      row.putValue("totalDocs", stats._totalDocs);
      row.putValue("reportedSize", stats._reportedSizeInBytes);
      row.putValue("estimatedSize", stats._estimatedSizeInBytes);
      row.putValue("storageTier", stats._storageTier);
      row.putValue("brokerTenant", stats._brokerTenant);
      row.putValue("serverTenant", stats._serverTenant);
      row.putValue("replicas", stats._replicas);
      row.putValue("tableConfig", stats._tableConfig);
      rows.add(row);
    }
    return SystemTableResponseUtils.buildBrokerResponse(TABLE_NAME, SCHEMA, projectionColumns, rows, totalRows);
  }

  private TableStats buildStats(String tableNameWithType, TableType tableType) {
    TableConfig tableConfig = _tableCache.getTableConfig(tableNameWithType);
    int segments = 0;
    long totalDocs = 0;
    long reportedSize = 0;
    long estimatedSize = 0;
    String tierValue = "";
    String brokerTenant = "";
    String serverTenant = "";
    int replicas = 0;
    if (tableConfig != null && tableConfig.getTenantConfig() != null) {
      brokerTenant = tableConfig.getTenantConfig().getBroker();
      serverTenant = tableConfig.getTenantConfig().getServer();
    }
    if (tableConfig != null && tableConfig.getValidationConfig() != null) {
      Integer repl = tableConfig.getValidationConfig().getReplicationNumber();
      replicas = repl != null ? repl : replicas;
    }
    // Use controller API only
    TableSize sizeFromController = fetchTableSize(tableNameWithType);
    if (sizeFromController != null) {
      if (sizeFromController._reportedSizeInBytes >= 0) {
        reportedSize = sizeFromController._reportedSizeInBytes;
      }
      if (sizeFromController._estimatedSizeInBytes >= 0) {
        estimatedSize = sizeFromController._estimatedSizeInBytes;
      }
      segments = getSegmentCount(sizeFromController, tableType);
      totalDocs = getTotalDocs(sizeFromController, tableType);
    }
    String status = tableConfig != null ? "ONLINE" : (segments > 0 ? "ONLINE" : "UNKNOWN");
    String tableConfigJson = "";
    if (tableConfig != null) {
      try {
        tableConfigJson = JsonUtils.objectToString(tableConfig);
      } catch (Exception e) {
        LOGGER.warn("Failed to serialize table config for {}: {}", tableNameWithType, e.toString());
        tableConfigJson = tableConfig.toString();
      }
    }
    return new TableStats(tableType.name(), status, segments, totalDocs, reportedSize, estimatedSize, tierValue,
        tableConfigJson, brokerTenant, serverTenant, replicas);
  }

  private boolean matchesFilter(@Nullable org.apache.pinot.common.request.Expression filterExpression, TableStats stats,
      String rawTableName) {
    if (filterExpression == null) {
      return true;
    }
    org.apache.pinot.common.request.Function function = filterExpression.getFunctionCall();
    if (function == null) {
      return true;
    }
    FilterKind filterKind = toFilterKind(function.getOperator());
    if (filterKind == null) {
      return true;
    }
    switch (filterKind) {
      case AND:
        for (org.apache.pinot.common.request.Expression child : function.getOperands()) {
          if (!matchesFilter(child, stats, rawTableName)) {
            return false;
          }
        }
        return true;
      case OR:
        for (org.apache.pinot.common.request.Expression child : function.getOperands()) {
          if (matchesFilter(child, stats, rawTableName)) {
            return true;
          }
        }
        return false;
      case NOT:
        if (function.getOperandsSize() == 0) {
          return true;
        }
        return !matchesFilter(function.getOperands().get(0), stats, rawTableName);
      default:
        return matchesLeafFilter(filterKind, function.getOperands(), stats, rawTableName);
    }
  }

  private boolean matchesLeafFilter(FilterKind filterKind,
      List<org.apache.pinot.common.request.Expression> operands, TableStats stats, String rawTableName) {
    String column = extractIdentifier(operands);
    if (column == null) {
      return true;
    }
    List<String> values = extractLiteralValues(operands);
    if (values.isEmpty()) {
      return true;
    }
    switch (column.toLowerCase(Locale.ROOT)) {
      case "tablename":
        return matchesString(values, rawTableName, filterKind);
      case "type":
        return matchesString(values, stats._type, filterKind);
      case "status":
        return matchesString(values, stats._status, filterKind);
      case "segments":
        return matchesNumber(values, stats._segments, filterKind);
      case "reportedsize":
        return matchesNumber(values, stats._reportedSizeInBytes, filterKind);
      case "estimatedsize":
        return matchesNumber(values, stats._estimatedSizeInBytes, filterKind);
      default:
        return true;
    }
  }

  private boolean matchesString(List<String> candidates, String actual, FilterKind filterKind) {
    switch (filterKind) {
      case EQUALS:
      case IN:
        return candidates.stream().anyMatch(v -> v.equalsIgnoreCase(actual));
      case NOT_EQUALS:
        return candidates.stream().noneMatch(v -> v.equalsIgnoreCase(actual));
      default:
        return true;
    }
  }

  private boolean matchesNumber(List<String> candidates, long actual, FilterKind filterKind) {
    try {
      switch (filterKind) {
        case EQUALS:
          return candidates.stream().anyMatch(v -> Long.parseLong(v) == actual);
        case NOT_EQUALS:
          return candidates.stream().noneMatch(v -> Long.parseLong(v) == actual);
        case GREATER_THAN:
          return candidates.stream().anyMatch(v -> actual > Long.parseLong(v));
        case GREATER_THAN_OR_EQUAL:
          return candidates.stream().anyMatch(v -> actual >= Long.parseLong(v));
        case LESS_THAN:
          return candidates.stream().anyMatch(v -> actual < Long.parseLong(v));
        case LESS_THAN_OR_EQUAL:
          return candidates.stream().anyMatch(v -> actual <= Long.parseLong(v));
        case IN:
          for (String candidate : candidates) {
            if (actual == Long.parseLong(candidate)) {
              return true;
            }
          }
          return false;
        case RANGE:
        case BETWEEN:
          if (candidates.size() >= 2) {
            long lower = Long.parseLong(candidates.get(0));
            long upper = Long.parseLong(candidates.get(1));
            return actual >= lower && actual <= upper;
          }
          return true;
        default:
          return true;
      }
    } catch (NumberFormatException e) {
      LOGGER.debug("Failed to parse numeric filter value {}: {}", candidates, e.toString());
      return true;
    }
  }

  private @Nullable String extractIdentifier(List<org.apache.pinot.common.request.Expression> operands) {
    for (org.apache.pinot.common.request.Expression operand : operands) {
      org.apache.pinot.common.request.Identifier identifier = operand.getIdentifier();
      if (identifier != null) {
        return identifier.getName();
      }
    }
    return null;
  }

  private List<String> extractLiteralValues(List<org.apache.pinot.common.request.Expression> operands) {
    List<String> values = new ArrayList<>();
    for (org.apache.pinot.common.request.Expression operand : operands) {
      org.apache.pinot.common.request.Literal literal = operand.getLiteral();
      if (literal != null) {
        if (literal.getSetField() == org.apache.pinot.common.request.Literal._Fields.NULL_VALUE) {
          values.add("null");
          continue;
        }
        try {
          values.add(RequestUtils.getLiteralString(literal));
        } catch (Exception e) {
          values.add(String.valueOf(RequestUtils.getLiteralValue(literal)));
        }
      }
    }
    return values;
  }

  private @Nullable FilterKind toFilterKind(String operator) {
    String normalized = operator.toUpperCase(Locale.ROOT);
    switch (normalized) {
      case "EQ":
        return FilterKind.EQUALS;
      case "NEQ":
        return FilterKind.NOT_EQUALS;
      case "GT":
        return FilterKind.GREATER_THAN;
      case "GTE":
        return FilterKind.GREATER_THAN_OR_EQUAL;
      case "LT":
        return FilterKind.LESS_THAN;
      case "LTE":
        return FilterKind.LESS_THAN_OR_EQUAL;
      default:
        try {
          return FilterKind.valueOf(normalized);
        } catch (Exception e) {
          return null;
        }
    }
  }

  private @Nullable TableSize fetchTableSize(String tableNameWithType) {
    TableSize cached = getCachedSize(tableNameWithType);
    if (cached != null) {
      return cached;
    }
    Function<String, TableSize> fetcher = getSizeFetcher();
    if (fetcher != null) {
      try {
        TableSize fetched = fetcher.apply(tableNameWithType);
        if (fetched != null) {
          cacheSize(tableNameWithType, fetched);
          return fetched;
        }
      } catch (Exception e) {
        LOGGER.warn("Table size fetcher failed for {}: {}", tableNameWithType, e.toString());
      }
    }
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    TableSize size = fetchTableSizeForName(rawTableName);
    if (size == null) {
      size = fetchTableSizeForName(tableNameWithType);
      if (size == null) {
        LOGGER.warn("{}: failed to fetch size for {} (raw: {}), returning null", TABLE_NAME, tableNameWithType,
            rawTableName);
      }
    }
    if (size != null) {
      cacheSize(tableNameWithType, size);
    }
    return size;
  }

  private @Nullable TableSize fetchTableSizeForName(String tableName) {
    for (String baseUrl : getControllerBaseUrls()) {
      try {
        String url = baseUrl + "/tables/" + tableName + "/size?verbose=true&includeReplacedSegments=false";
        HttpRequest request = HttpRequest.newBuilder(URI.create(url))
            .timeout(Duration.ofSeconds(5))
            .GET()
            .header("Accept", "application/json")
            .build();
        HttpResponse<String> response = _httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 200 && response.statusCode() < 300) {
          TableSize parsed = JsonUtils.stringToObject(response.body(), TableSize.class);
          LOGGER.debug("{}: controller size response for {} via {} -> segments offline={}, realtime={}, "
                  + "reportedSize={}, estimatedSize={}", TABLE_NAME, tableName, baseUrl,
              parsed._offlineSegments != null && parsed._offlineSegments._segments != null
                  ? parsed._offlineSegments._segments.size() : 0,
              parsed._realtimeSegments != null && parsed._realtimeSegments._segments != null
                  ? parsed._realtimeSegments._segments.size() : 0,
              parsed._reportedSizeInBytes, parsed._estimatedSizeInBytes);
          return parsed;
        } else {
          LOGGER.warn("{}: failed to fetch table size for {} via {}: status {}, body={}", TABLE_NAME, tableName,
              baseUrl, response.statusCode(), response.body());
        }
      } catch (Exception e) {
        LOGGER.warn("{}: error fetching table size for {} via {}", TABLE_NAME, tableName, baseUrl, e);
      }
    }
    return null;
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
    for (String url : _staticControllerUrls) {
      String normalized = normalizeControllerUrl(url);
      if (normalized != null) {
        urls.add(normalized);
      }
    }
    return new ArrayList<>(urls);
  }

  private int getSegmentCount(TableSize sizeFromController, TableType tableType) {
    if (tableType == TableType.OFFLINE && sizeFromController._offlineSegments != null
        && sizeFromController._offlineSegments._segments != null) {
      return sizeFromController._offlineSegments._segments.size();
    }
    if (tableType == TableType.REALTIME && sizeFromController._realtimeSegments != null
        && sizeFromController._realtimeSegments._segments != null) {
      return sizeFromController._realtimeSegments._segments.size();
    }
    return 0;
  }

  private long getTotalDocs(TableSize sizeFromController, TableType tableType) {
    if (tableType == TableType.OFFLINE && sizeFromController._offlineSegments != null
        && sizeFromController._offlineSegments._segments != null) {
      return sizeFromController._offlineSegments._segments.values().stream()
          .mapToLong(segmentSize -> segmentSize._totalDocs).sum();
    }
    if (tableType == TableType.REALTIME && sizeFromController._realtimeSegments != null
        && sizeFromController._realtimeSegments._segments != null) {
      return sizeFromController._realtimeSegments._segments.values().stream()
          .mapToLong(segmentSize -> segmentSize._totalDocs).sum();
    }
    return 0;
  }

  private @Nullable Function<String, TableSize> getSizeFetcher() {
    if (_tableSizeFetcherOverride != null) {
      return _tableSizeFetcherOverride;
    }
    List<String> controllers = getControllerBaseUrls();
    if (controllers.isEmpty() || !isAdminClientAvailable()) {
      return null;
    }
    return tableNameWithType -> fetchWithAdminClient(controllers, tableNameWithType);
  }

  private List<String> discoverControllersFromHelix() {
    List<String> urls = new ArrayList<>();
    try {
      if (_clusterName == null) {
        LOGGER.warn("Cannot discover controllers without cluster name");
        return List.of();
      }
      for (String controllerId : _helixAdmin.getInstancesInCluster(_clusterName)) {
        if (!InstanceTypeUtils.isController(controllerId)) {
          continue;
        }
        int firstUnderscore = controllerId.indexOf('_');
        int lastUnderscore = controllerId.lastIndexOf('_');
        if (firstUnderscore > 0 && lastUnderscore > firstUnderscore && lastUnderscore < controllerId.length() - 1) {
          String host = controllerId.substring(firstUnderscore + 1, lastUnderscore);
          String port = controllerId.substring(lastUnderscore + 1);
          if (!host.isEmpty() && !port.isEmpty()) {
            urls.add(host + ":" + port);
          } else {
            LOGGER.warn("Unable to parse controller address from instance id (empty host/port): {}", controllerId);
          }
        } else {
          LOGGER.warn("Unable to parse controller address from instance id: {}", controllerId);
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to discover controllers from Helix", e);
    }
    return urls;
  }

  private boolean isAdminClientAvailable() {
    if (!IS_ADMIN_CLIENT_AVAILABLE) {
      LOGGER.debug("PinotAdminClient not on classpath; falling back to HTTP for table size fetch");
    }
    return IS_ADMIN_CLIENT_AVAILABLE;
  }

  private @Nullable TableSize fetchWithAdminClient(List<String> controllers, String tableNameWithType) {
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    for (String controller : controllers) {
      AutoCloseable adminClient = null;
      try {
        Class<?> clientClass = Class.forName("org.apache.pinot.client.admin.PinotAdminClient");
        String controllerAddress = stripScheme(controller);
        adminClient = (AutoCloseable) clientClass.getConstructor(String.class).newInstance(controllerAddress);
        Object tableClient = clientClass.getMethod("getTableClient").invoke(adminClient);
        Object sizeNode =
            tableClient.getClass().getMethod("getTableSize", String.class, boolean.class, boolean.class)
                .invoke(tableClient, rawTableName, true, false);
        if (sizeNode != null) {
          return JsonUtils.stringToObject(sizeNode.toString(), TableSize.class);
        }
      } catch (ClassNotFoundException e) {
        return null;
      } catch (Exception e) {
        LOGGER.warn("Failed to fetch table size for {} from controller {} via admin client: {}", rawTableName,
            controller, e.toString());
      } finally {
        if (adminClient != null) {
          try {
            adminClient.close();
          } catch (Exception e) {
            LOGGER.debug("Failed to close admin client for {}: {}", controller, e.toString());
          }
        }
      }
    }
    return null;
  }

  private static String normalizeControllerUrl(@Nullable String controllerUrl) {
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
    if (controllerUrl == null) {
      return null;
    }
    if (controllerUrl.startsWith("http://")) {
      return controllerUrl.substring("http://".length());
    }
    if (controllerUrl.startsWith("https://")) {
      return controllerUrl.substring("https://".length());
    }
    return controllerUrl;
  }

  private static final class TableStats {
    private final String _type;
    private final String _status;
    private final int _segments;
    private final long _totalDocs;
    private final long _reportedSizeInBytes;
    private final long _estimatedSizeInBytes;
    private final String _storageTier;
    private final String _tableConfig;
    private final String _brokerTenant;
    private final String _serverTenant;
    private final int _replicas;

    TableStats(String type, String status, int segments, long totalDocs, long reportedSizeInBytes,
        long estimatedSizeInBytes, String storageTier, String tableConfigJson, String brokerTenant,
        String serverTenant, int replicas) {
      _type = type;
      _status = status;
      _segments = segments;
      _totalDocs = totalDocs;
      _reportedSizeInBytes = reportedSizeInBytes;
      _estimatedSizeInBytes = estimatedSizeInBytes;
      _storageTier = storageTier;
      _tableConfig = tableConfigJson;
      _brokerTenant = brokerTenant;
      _serverTenant = serverTenant;
      _replicas = replicas;
    }
  }

  /**
   * Minimal shape of controller table size response.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static final class TableSize {
    @JsonProperty("reportedSizeInBytes")
    public long _reportedSizeInBytes = -1;

    @JsonProperty("estimatedSizeInBytes")
    public long _estimatedSizeInBytes = -1;

    @JsonProperty("offlineSegments")
    public TableSubType _offlineSegments;

    @JsonProperty("realtimeSegments")
    public TableSubType _realtimeSegments;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static final class TableSubType {
    @JsonProperty("segments")
    public Map<String, SegmentSize> _segments;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static final class SegmentSize {
    @JsonProperty("totalDocs")
    public long _totalDocs = 0;
  }

  private @Nullable TableSize getCachedSize(String tableNameWithType) {
    CachedSize cached = _sizeCache.get(tableNameWithType);
    if (cached == null) {
      return null;
    }
    if (System.currentTimeMillis() - cached._timestampMs > SIZE_CACHE_TTL_MS) {
      _sizeCache.remove(tableNameWithType, cached);
      return null;
    }
    return cached._tableSize;
  }

  private void cacheSize(String tableNameWithType, TableSize size) {
    _sizeCache.put(tableNameWithType, new CachedSize(size, System.currentTimeMillis()));
  }

  private static final class CachedSize {
    private final TableSize _tableSize;
    private final long _timestampMs;

    CachedSize(TableSize tableSize, long timestampMs) {
      _tableSize = tableSize;
      _timestampMs = timestampMs;
    }
  }
}
