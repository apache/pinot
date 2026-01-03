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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.IntFunction;
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
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Basic system table exposing table-level metadata populated from the broker {@link TableCache}.
 */
@SystemTable
public final class TablesSystemTableProvider implements SystemTableProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(TablesSystemTableProvider.class);
  public static final String TABLE_NAME = "system.tables";
  private static final String SIZE_CACHE_TTL_MS_PROPERTY = "pinot.systemtable.tables.sizeCacheTtlMs";
  private static final long DEFAULT_SIZE_CACHE_TTL_MS = Duration.ofMinutes(1).toMillis();
  private static final long SIZE_CACHE_TTL_MS = getNonNegativeLongProperty(SIZE_CACHE_TTL_MS_PROPERTY,
      DEFAULT_SIZE_CACHE_TTL_MS);

  private static final String CONTROLLER_TIMEOUT_MS_PROPERTY = "pinot.systemtable.tables.controllerTimeoutMs";
  private static final long DEFAULT_CONTROLLER_TIMEOUT_MS = Duration.ofSeconds(5).toMillis();
  private static final long CONTROLLER_TIMEOUT_MS = getPositiveLongProperty(CONTROLLER_TIMEOUT_MS_PROPERTY,
      DEFAULT_CONTROLLER_TIMEOUT_MS);

  private static final long SIZE_FETCH_FAILURE_WARN_INTERVAL_MS = Duration.ofHours(1).toMillis();

  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
      .addSingleValueDimension("tableName", FieldSpec.DataType.STRING)
      .addSingleValueDimension("type", FieldSpec.DataType.STRING)
      .addSingleValueDimension("status", FieldSpec.DataType.STRING)
      .addSingleValueDimension("segments", FieldSpec.DataType.INT)
      .addSingleValueDimension("totalDocs", FieldSpec.DataType.LONG)
      .addMetric("reportedSize", FieldSpec.DataType.LONG)
      .addMetric("estimatedSize", FieldSpec.DataType.LONG)
      .addSingleValueDimension("brokerTenant", FieldSpec.DataType.STRING)
      .addSingleValueDimension("serverTenant", FieldSpec.DataType.STRING)
      .addSingleValueDimension("replicas", FieldSpec.DataType.INT)
      .addSingleValueDimension("tableConfig", FieldSpec.DataType.STRING)
      .build();

  private final TableCache _tableCache;
  private final @Nullable HelixAdmin _helixAdmin;
  private final @Nullable String _clusterName;
  private final @Nullable Function<String, TableSize> _tableSizeFetcherOverride;
  private final List<String> _configuredControllerUrls;
  private final Map<String, CachedSize> _sizeCache = new ConcurrentHashMap<>();
  private final Map<String, PinotAdminClient> _adminClientCache = new ConcurrentHashMap<>();
  private final AtomicLong _lastSizeFetchFailureWarnLogMs = new AtomicLong();

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
    _tableSizeFetcherOverride = tableSizeFetcherOverride;
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
    if (_tableCache == null) {
      return new InMemorySystemTableSegment(TABLE_NAME, SCHEMA, 0, Collections.emptyMap());
    }

    Set<String> tableNamesWithType = new LinkedHashSet<>();
    for (String tableName : _tableCache.getTableNameMap().values()) {
      if (TableNameBuilder.getTableTypeFromTableName(tableName) != null) {
        tableNamesWithType.add(tableName);
      }
    }
    List<String> sortedTableNames = new ArrayList<>(tableNamesWithType);
    sortedTableNames.sort(Comparator.naturalOrder());

    List<String> controllerBaseUrls = getControllerBaseUrls();
    Function<String, TableSize> sizeFetcher = getSizeFetcher();
    class TableRow {
      final String _tableNameWithType;
      final TableType _tableType;
      final String _rawTableName;
      final @Nullable TableConfig _tableConfig;
      private volatile @Nullable String _tableConfigJson;
      private volatile @Nullable TableSize _tableSize;
      private volatile boolean _tableSizeFetched;

      private TableRow(String tableNameWithType, TableType tableType, String rawTableName,
          @Nullable TableConfig tableConfig) {
        _tableNameWithType = tableNameWithType;
        _tableType = tableType;
        _rawTableName = rawTableName;
        _tableConfig = tableConfig;
      }

      @Nullable
      private TableSize getTableSize() {
        if (_tableSizeFetched) {
          return _tableSize;
        }
        synchronized (this) {
          if (_tableSizeFetched) {
            return _tableSize;
          }
          _tableSize = fetchTableSize(_tableNameWithType, sizeFetcher, controllerBaseUrls);
          _tableSizeFetched = true;
          return _tableSize;
        }
      }

      private String getStatus() {
        if (_tableConfig != null) {
          return "ONLINE";
        }
        TableSize sizeFromController = getTableSize();
        int segments = sizeFromController != null ? getSegmentCount(sizeFromController, _tableType) : 0;
        return segments > 0 ? "ONLINE" : "UNKNOWN";
      }

      private int getSegments() {
        TableSize sizeFromController = getTableSize();
        return sizeFromController != null ? getSegmentCount(sizeFromController, _tableType) : 0;
      }

      private long getTotalDocs() {
        TableSize sizeFromController = getTableSize();
        return sizeFromController != null ? TablesSystemTableProvider.this.getTotalDocs(sizeFromController, _tableType,
            _tableNameWithType, controllerBaseUrls) : 0L;
      }

      private long getReportedSize() {
        TableSize sizeFromController = getTableSize();
        if (sizeFromController == null || sizeFromController._reportedSizeInBytes < 0) {
          return 0L;
        }
        return sizeFromController._reportedSizeInBytes;
      }

      private long getEstimatedSize() {
        TableSize sizeFromController = getTableSize();
        if (sizeFromController == null || sizeFromController._estimatedSizeInBytes < 0) {
          return 0L;
        }
        return sizeFromController._estimatedSizeInBytes;
      }

      private String getBrokerTenant() {
        if (_tableConfig != null && _tableConfig.getTenantConfig() != null) {
          String tenant = _tableConfig.getTenantConfig().getBroker();
          return tenant != null ? tenant : "";
        }
        return "";
      }

      private String getServerTenant() {
        if (_tableConfig != null && _tableConfig.getTenantConfig() != null) {
          String tenant = _tableConfig.getTenantConfig().getServer();
          return tenant != null ? tenant : "";
        }
        return "";
      }

      private int getReplicas() {
        if (_tableConfig != null && _tableConfig.getValidationConfig() != null) {
          Integer replicationNumber = _tableConfig.getValidationConfig().getReplicationNumber();
          if (replicationNumber != null) {
            return replicationNumber;
          }
        }
        return 0;
      }

      private String getTableConfigJson() {
        String cached = _tableConfigJson;
        if (cached != null) {
          return cached;
        }
        synchronized (this) {
          cached = _tableConfigJson;
          if (cached != null) {
            return cached;
          }
          cached = "";
          if (_tableConfig != null) {
            try {
              cached = JsonUtils.objectToString(_tableConfig);
            } catch (Exception e) {
              LOGGER.warn("Failed to serialize table config for {}: {}", _tableNameWithType, e.toString());
              cached = _tableConfig.toString();
            }
          }
          _tableConfigJson = cached;
          return cached;
        }
      }
    }

    List<TableRow> tableRows = new ArrayList<>(sortedTableNames.size());
    for (String tableNameWithType : sortedTableNames) {
      TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
      if (tableType == null) {
        continue;
      }
      String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
      TableConfig tableConfig = _tableCache.getTableConfig(tableNameWithType);
      tableRows.add(new TableRow(tableNameWithType, tableType, rawTableName, tableConfig));
    }

    Map<String, IntFunction<Object>> valueProviders = new java.util.HashMap<>();
    valueProviders.put("tableName", docId -> tableRows.get(docId)._rawTableName);
    valueProviders.put("type", docId -> tableRows.get(docId)._tableType.name());
    valueProviders.put("status", docId -> tableRows.get(docId).getStatus());
    valueProviders.put("segments", docId -> tableRows.get(docId).getSegments());
    valueProviders.put("totalDocs", docId -> tableRows.get(docId).getTotalDocs());
    valueProviders.put("reportedSize", docId -> tableRows.get(docId).getReportedSize());
    valueProviders.put("estimatedSize", docId -> tableRows.get(docId).getEstimatedSize());
    valueProviders.put("brokerTenant", docId -> tableRows.get(docId).getBrokerTenant());
    valueProviders.put("serverTenant", docId -> tableRows.get(docId).getServerTenant());
    valueProviders.put("replicas", docId -> tableRows.get(docId).getReplicas());
    valueProviders.put("tableConfig", docId -> tableRows.get(docId).getTableConfigJson());

    return new InMemorySystemTableSegment(TABLE_NAME, SCHEMA, tableRows.size(), valueProviders);
  }

  @Nullable
  private TableSize fetchTableSize(String tableNameWithType,
      @Nullable Function<String, TableSize> fetcher, List<String> controllerBaseUrls) {
    boolean cacheEnabled = SIZE_CACHE_TTL_MS > 0;
    TableSize cached = cacheEnabled ? getCachedSize(tableNameWithType) : null;
    if (cached != null) {
      return cached;
    }
    if (fetcher != null) {
      try {
        TableSize fetched = fetcher.apply(tableNameWithType);
        if (fetched != null) {
          if (cacheEnabled) {
            cacheSize(tableNameWithType, fetched);
          }
          return fetched;
        }
      } catch (Exception e) {
        LOGGER.warn("Table size fetcher failed for {}: {}", tableNameWithType, e.toString());
      }
    }
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    TableSize size = fetchTableSizeForName(controllerBaseUrls, rawTableName);
    if (size == null) {
      size = fetchTableSizeForName(controllerBaseUrls, tableNameWithType);
      if (size == null) {
        logSizeFetchFailure("{}: failed to fetch size for {} from controllers {} "
                + "(tried raw table name '{}' and table name with type '{}')",
            TABLE_NAME, tableNameWithType, controllerBaseUrls, rawTableName, tableNameWithType);
      }
    }
    if (size != null && cacheEnabled) {
      cacheSize(tableNameWithType, size);
    }
    return size;
  }

  @Nullable
  private TableSize fetchTableSizeForName(List<String> controllerBaseUrls, String tableName) {
    for (String baseUrl : controllerBaseUrls) {
      try {
        PinotAdminClient adminClient = getOrCreateAdminClient(baseUrl);
        if (adminClient == null) {
          continue;
        }

        JsonNode sizeNode = adminClient.getTableSize(tableName, true, false);

        if (sizeNode == null) {
          continue;
        }

        TableSize parsed = JsonUtils.stringToObject(sizeNode.toString(), TableSize.class);
        LOGGER.debug("{}: controller size response for {} via {} -> segments offline={}, realtime={}, "
                + "reportedSize={}, estimatedSize={}", TABLE_NAME, tableName, baseUrl,
            parsed._offlineSegments != null && parsed._offlineSegments._segments != null
                ? parsed._offlineSegments._segments.size() : 0,
            parsed._realtimeSegments != null && parsed._realtimeSegments._segments != null
                ? parsed._realtimeSegments._segments.size() : 0,
            parsed._reportedSizeInBytes, parsed._estimatedSizeInBytes);
        return parsed;
      } catch (Exception e) {
        logSizeFetchFailure("{}: error fetching table size for {} via {} using admin client", TABLE_NAME, tableName,
            baseUrl, e);
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
    for (String url : _configuredControllerUrls) {
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

  private long getTotalDocsFromSize(TableSize sizeFromController, TableType tableType) {
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

  private long getTotalDocs(TableSize sizeFromController, TableType tableType, String tableNameWithType,
      List<String> controllerBaseUrls) {
    if (tableType == TableType.OFFLINE && sizeFromController._offlineSegments != null
        && sizeFromController._offlineSegments._segments != null) {
      long cached = sizeFromController._offlineTotalDocs;
      if (cached >= 0) {
        return cached;
      }
      long totalDocsFromSize = getTotalDocsFromSize(sizeFromController, tableType);
      if (totalDocsFromSize > 0) {
        synchronized (sizeFromController) {
          if (sizeFromController._offlineTotalDocs < 0) {
            sizeFromController._offlineTotalDocs = totalDocsFromSize;
          }
          return sizeFromController._offlineTotalDocs;
        }
      }
      long fetched = fetchTotalDocsFromSegmentMetadata(tableNameWithType, sizeFromController._offlineSegments._segments,
          controllerBaseUrls);
      synchronized (sizeFromController) {
        if (sizeFromController._offlineTotalDocs < 0) {
          sizeFromController._offlineTotalDocs = fetched;
        }
        return sizeFromController._offlineTotalDocs;
      }
    }
    if (tableType == TableType.REALTIME && sizeFromController._realtimeSegments != null
        && sizeFromController._realtimeSegments._segments != null) {
      long cached = sizeFromController._realtimeTotalDocs;
      if (cached >= 0) {
        return cached;
      }
      long totalDocsFromSize = getTotalDocsFromSize(sizeFromController, tableType);
      if (totalDocsFromSize > 0) {
        synchronized (sizeFromController) {
          if (sizeFromController._realtimeTotalDocs < 0) {
            sizeFromController._realtimeTotalDocs = totalDocsFromSize;
          }
          return sizeFromController._realtimeTotalDocs;
        }
      }
      long fetched = fetchTotalDocsFromSegmentMetadata(tableNameWithType,
          sizeFromController._realtimeSegments._segments, controllerBaseUrls);
      synchronized (sizeFromController) {
        if (sizeFromController._realtimeTotalDocs < 0) {
          sizeFromController._realtimeTotalDocs = fetched;
        }
        return sizeFromController._realtimeTotalDocs;
      }
    }
    return 0;
  }

  private long fetchTotalDocsFromSegmentMetadata(String tableNameWithType, Map<String, SegmentSize> segments,
      List<String> controllerBaseUrls) {
    if (segments.isEmpty()) {
      return 0;
    }
    for (String baseUrl : controllerBaseUrls) {
      try {
        PinotAdminClient adminClient = getOrCreateAdminClient(baseUrl);
        if (adminClient == null) {
          continue;
        }

        long totalDocs = 0;
        for (String segmentName : segments.keySet()) {
          JsonNode segmentMetadata = adminClient.getSegmentApiClient().getSegmentMetadata(tableNameWithType,
              segmentName);
          totalDocs += segmentMetadata.path(CommonConstants.Segment.TOTAL_DOCS).asLong(0);
        }
        return totalDocs;
      } catch (Exception e) {
        logSizeFetchFailure("{}: error fetching segment metadata for {} via {}", TABLE_NAME, tableNameWithType, baseUrl,
            e);
      }
    }
    return 0;
  }

  @Nullable
  private Function<String, TableSize> getSizeFetcher() {
    if (_tableSizeFetcherOverride != null) {
      return _tableSizeFetcherOverride;
    }
    return null;
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

  @Nullable
  private PinotAdminClient getOrCreateAdminClient(String controllerBaseUrl) {
    String normalized = normalizeControllerUrl(controllerBaseUrl);
    if (normalized == null) {
      return null;
    }
    return _adminClientCache.computeIfAbsent(normalized, controller -> {
      try {
        String controllerAddress = stripScheme(controller);
        Properties properties = new Properties();
        properties.setProperty(PinotAdminTransport.ADMIN_TRANSPORT_REQUEST_TIMEOUT_MS,
            String.valueOf(CONTROLLER_TIMEOUT_MS));
        properties.setProperty(PinotAdminTransport.ADMIN_TRANSPORT_SCHEME,
            controller.startsWith("https://") ? "https" : "http");
        return new PinotAdminClient(controllerAddress, properties);
      } catch (Exception e) {
        LOGGER.warn("Failed to create admin client for {}: {}", controller, e.toString());
        return null;
      }
    });
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
    if (controllerUrl.startsWith("http://")) {
      return controllerUrl.substring("http://".length());
    }
    if (controllerUrl.startsWith("https://")) {
      return controllerUrl.substring("https://".length());
    }
    return controllerUrl;
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

    public volatile long _offlineTotalDocs = -1;
    public volatile long _realtimeTotalDocs = -1;
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

  private static long getPositiveLongProperty(String key, long defaultValue) {
    long value = Long.getLong(key, defaultValue);
    return value > 0 ? value : defaultValue;
  }

  private static long getNonNegativeLongProperty(String key, long defaultValue) {
    long value = Long.getLong(key, defaultValue);
    return value >= 0 ? value : defaultValue;
  }

  private void logSizeFetchFailure(String message, Object... args) {
    long nowMs = System.currentTimeMillis();
    long lastWarnLogMs = _lastSizeFetchFailureWarnLogMs.get();
    if (nowMs - lastWarnLogMs >= SIZE_FETCH_FAILURE_WARN_INTERVAL_MS
        && _lastSizeFetchFailureWarnLogMs.compareAndSet(lastWarnLogMs, nowMs)) {
      LOGGER.warn(message, args);
    } else {
      LOGGER.debug(message, args);
    }
  }

  @Nullable
  private TableSize getCachedSize(String tableNameWithType) {
    if (SIZE_CACHE_TTL_MS <= 0) {
      return null;
    }
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
    if (SIZE_CACHE_TTL_MS <= 0) {
      return;
    }
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
