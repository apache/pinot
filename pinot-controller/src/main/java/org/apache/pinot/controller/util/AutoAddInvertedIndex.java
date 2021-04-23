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
package org.apache.pinot.controller.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.apache.helix.PropertyPathConfig;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.controller.helix.ControllerRequestURLBuilder;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>AutoAddInvertedIndex</code> class can be used to automatically add inverted index to tables based on the
 * strategy and mode specified.
 * <p>Currently support:
 * <ul>
 *   <li>
 *     Strategy:
 *     <ul>
 *       <li>
 *         QUERY: add inverted index based on the query result
 *         <ul>
 *           <li>1. Get the table size, only add inverted index to large tables</li>
 *           <li>2. Get the latest timestamp, only get dimension DISTINCTCOUNT values for one timestamp</li>
 *           <li>3. Sort the DISTINCTCOUNT value for all dimensions</li>
 *           <li>4. Add inverted index to dimensions with large DISTINCTCOUNT value</li>
 *         </ul>
 *       </li>
 *     </ul>
 *   </li>
 *   <li>
 *     Mode:
 *     <ul>
 *       <li>NEW: apply only to tables without inverted index</li>
 *       <li>REMOVE: remove all auto-generated inverted index</li>
 *       <li>REFRESH: refresh the auto-generated inverted index</li>
 *       <li>APPEND: append to the auto-generated inverted index</li>
 *     </ul>
 *   </li>
 * </ul>
 */
public class AutoAddInvertedIndex {
  public enum Strategy {
    QUERY     // Add inverted index based on the query result
  }

  public enum Mode {
    NEW,      // Apply only to tables without inverted index
    REMOVE,   // Remove all auto-generated inverted index
    REFRESH,  // Refresh the auto-generated inverted index
    APPEND    // Append to the auto-generated inverted index
  }

  public static final long DEFAULT_TABLE_SIZE_THRESHOLD = 10_000_000;
  public static final long DEFAULT_CARDINALITY_THRESHOLD = 100;
  public static final int DEFAULT_MAX_NUM_INVERTED_INDEX_ADDED = 2;

  private static final Logger LOGGER = LoggerFactory.getLogger(AutoAddInvertedIndex.class);

  private final String _clusterName;
  private final String _controllerAddress;
  private final String _brokerAddress;
  private final ZKHelixAdmin _helixAdmin;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final Strategy _strategy;
  private final Mode _mode;

  private String _tableNamePattern = null;
  private long _tableSizeThreshold = DEFAULT_TABLE_SIZE_THRESHOLD;
  private long _cardinalityThreshold = DEFAULT_CARDINALITY_THRESHOLD;
  private int _maxNumInvertedIndexAdded = DEFAULT_MAX_NUM_INVERTED_INDEX_ADDED;

  public AutoAddInvertedIndex(@Nonnull String zkAddress, @Nonnull String clusterName, @Nonnull String controllerAddress,
      @Nonnull String brokerAddress, @Nonnull Strategy strategy, @Nonnull Mode mode) {
    _clusterName = clusterName;
    _controllerAddress = controllerAddress;
    _brokerAddress = brokerAddress;
    _helixAdmin = new ZKHelixAdmin(zkAddress);
    _propertyStore = new ZkHelixPropertyStore<>(zkAddress, new ZNRecordSerializer(),
        PropertyPathConfig.getPath(PropertyType.PROPERTYSTORE, clusterName));
    _strategy = strategy;
    _mode = mode;
  }

  public void overrideDefaultSettings(@Nonnull String tableNamePattern, long tableSizeThreshold,
      long cardinalityThreshold, int maxNumInvertedIndex) {
    _tableNamePattern = tableNamePattern;
    _tableSizeThreshold = tableSizeThreshold;
    _cardinalityThreshold = cardinalityThreshold;
    _maxNumInvertedIndexAdded = maxNumInvertedIndex;
  }

  public void run()
      throws Exception {
    if (_strategy == Strategy.QUERY) {
      runQueryStrategy();
    } else {
      throw new IllegalStateException("Invalid Strategy: " + _strategy);
    }
  }

  private void runQueryStrategy()
      throws Exception {
    // Get all resources in cluster
    List<String> resourcesInCluster = _helixAdmin.getResourcesInCluster(_clusterName);

    for (String tableNameWithType : resourcesInCluster) {
      // Skip non-table resources
      if (!TableNameBuilder.isTableResource(tableNameWithType)) {
        continue;
      }

      // Skip tables that do not match the defined name pattern
      if (_tableNamePattern != null && !tableNameWithType.matches(_tableNamePattern)) {
        continue;
      }
      LOGGER.info("Table: {} matches the table name pattern: {}", tableNameWithType, _tableNamePattern);

      // Get the inverted index config
      TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
      Preconditions.checkNotNull(tableConfig);
      IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
      List<String> invertedIndexColumns = indexingConfig.getInvertedIndexColumns();
      boolean autoGeneratedInvertedIndex = indexingConfig.isAutoGeneratedInvertedIndex();

      // Handle auto-generated inverted index
      if (autoGeneratedInvertedIndex) {
        Preconditions.checkState(!invertedIndexColumns.isEmpty(), "Auto-generated inverted index list is empty");

        // NEW mode, skip
        if (_mode == Mode.NEW) {
          LOGGER.info(
              "Table: {}, skip adding inverted index because it has auto-generated inverted index and under NEW mode",
              tableNameWithType);
          continue;
        }

        // REMOVE mode, remove the inverted index and update
        if (_mode == Mode.REMOVE) {
          invertedIndexColumns.clear();
          indexingConfig.setAutoGeneratedInvertedIndex(false);
          if (updateIndexConfig(tableNameWithType, tableConfig)) {
            LOGGER.info("Table: {}, removed auto-generated inverted index", tableNameWithType);
          } else {
            LOGGER.error("Table: {}, failed to remove auto-generated inverted index", tableNameWithType);
          }
          continue;
        }

        // REFRESH mode, remove auto-generated inverted index
        if (_mode == Mode.REFRESH) {
          invertedIndexColumns.clear();
        }
      } else {
        // Handle null inverted index columns
        if (invertedIndexColumns == null) {
          invertedIndexColumns = new ArrayList<>();
          indexingConfig.setInvertedIndexColumns(invertedIndexColumns);
        }

        // Remove empty strings
        int emptyStringIndex;
        while ((emptyStringIndex = invertedIndexColumns.indexOf("")) != -1) {
          invertedIndexColumns.remove(emptyStringIndex);
        }

        // Skip non-empty non-auto-generated inverted index
        if (!invertedIndexColumns.isEmpty()) {
          LOGGER.info("Table: {}, skip adding inverted index because it has non-auto-generated inverted index",
              tableNameWithType);
          continue;
        }
      }

      // Skip tables without a schema
      Schema tableSchema = ZKMetadataProvider.getTableSchema(_propertyStore, tableNameWithType);
      if (tableSchema == null) {
        LOGGER.info("Table: {}, skip adding inverted index because it does not have a schema", tableNameWithType);
        continue;
      }

      // Skip tables without dimensions
      List<String> dimensionNames = tableSchema.getDimensionNames();
      if (dimensionNames.size() == 0) {
        LOGGER.info("Table: {}, skip adding inverted index because it does not have any dimension column",
            tableNameWithType);
        continue;
      }

      // Skip tables without a proper time column
      String timeColumnName = tableConfig.getValidationConfig().getTimeColumnName();
      if (timeColumnName == null) {
        LOGGER.info(
            "Table: {}, skip adding inverted index because it does not have a time column specified in the table config",
            tableNameWithType);
        continue;
      }
      DateTimeFieldSpec dateTimeSpec = tableSchema.getSpecForTimeColumn(timeColumnName);
      if (dateTimeSpec == null || dateTimeSpec.getDataType() == FieldSpec.DataType.STRING) {
        LOGGER.info("Table: {}, skip adding inverted index because it does not have a numeric time column",
            tableNameWithType);
        continue;
      }
      TimeUnit timeUnit = new DateTimeFormatSpec(dateTimeSpec.getFormat()).getColumnUnit();
      if (timeUnit != TimeUnit.DAYS) {
        LOGGER.warn("Table: {}, time column {] has non-DAYS time unit: {}", timeColumnName, timeUnit);
      }

      // Only add inverted index to table larger than a threshold
      JsonNode queryResponse = sendQuery("SELECT COUNT(*) FROM " + tableNameWithType);
      long numTotalDocs = queryResponse.get("totalDocs").asLong();
      LOGGER.info("Table: {}, number of total documents: {}", tableNameWithType, numTotalDocs);
      if (numTotalDocs <= _tableSizeThreshold) {
        LOGGER.info("Table: {}, skip adding inverted index because the table is too small", tableNameWithType);
        continue;
      }

      // Get each dimension's cardinality on one timestamp's data
      queryResponse = sendQuery("SELECT Max(" + timeColumnName + ") FROM " + tableNameWithType);
      long maxTimeStamp = queryResponse.get("aggregationResults").get(0).get("value").asLong();

      LOGGER.info("Table: {}, max time column {}: {}", tableNameWithType, timeColumnName, maxTimeStamp);

      // Query DISTINCTCOUNT on all dimensions in one query might cause timeout, so query them separately
      List<ResultPair> resultPairs = new ArrayList<>();
      for (String dimensionName : dimensionNames) {
        String query =
            "SELECT DISTINCTCOUNT(" + dimensionName + ") FROM " + tableNameWithType + " WHERE " + timeColumnName + " = "
                + maxTimeStamp;
        queryResponse = sendQuery(query);
        JsonNode result = queryResponse.get("aggregationResults").get(0);
        resultPairs.add(new ResultPair(result.get("function").asText().substring("distinctCount_".length()),
            result.get("value").asLong()));
      }

      // Sort the dimensions based on their cardinalities
      Collections.sort(resultPairs);

      // Add the top dimensions into inverted index columns
      int numInvertedIndex = Math.min(_maxNumInvertedIndexAdded, resultPairs.size());
      for (int i = 0; i < numInvertedIndex; i++) {
        ResultPair resultPair = resultPairs.get(i);
        String columnName = resultPair._key;
        long cardinality = resultPair._value;
        if (cardinality > _cardinalityThreshold) {
          // Do not append inverted index if already exists
          if (!invertedIndexColumns.contains(columnName)) {
            invertedIndexColumns.add(columnName);
          }
          LOGGER.info("Table: {}, add inverted index to column {} with cardinality: {}", tableNameWithType, columnName,
              cardinality);
        } else {
          LOGGER.info("Table: {}, skip adding inverted index to column {} with cardinality: {}", tableNameWithType,
              columnName, cardinality);
          break;
        }
      }

      // Update indexing config
      if (!invertedIndexColumns.isEmpty()) {
        indexingConfig.setAutoGeneratedInvertedIndex(true);
        if (updateIndexConfig(tableNameWithType, tableConfig)) {
          LOGGER.info("Table: {}, added inverted index to columns: {}", tableNameWithType, invertedIndexColumns);
        } else {
          LOGGER
              .error("Table: {}, failed to add inverted index to columns: {}", tableNameWithType, invertedIndexColumns);
        }
      } else {
        if (autoGeneratedInvertedIndex) {
          Preconditions.checkState(_mode == Mode.REFRESH);

          // Remove existing auto-generated inverted index because no column matches all the conditions
          indexingConfig.setAutoGeneratedInvertedIndex(false);
          if (updateIndexConfig(tableNameWithType, tableConfig)) {
            LOGGER.info("Table: {}, removed auto-generated inverted index", tableNameWithType);
          } else {
            LOGGER.error("Table: {}, failed to remove auto-generated inverted index", tableNameWithType);
          }
        }
      }
    }
  }

  private JsonNode sendQuery(String query)
      throws Exception {
    URLConnection urlConnection = new URL("http://" + _brokerAddress + "/query").openConnection();
    urlConnection.setDoOutput(true);

    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(urlConnection.getOutputStream(), "UTF-8"));
    writer.write(JsonUtils.newObjectNode().put("pql", query).toString());
    writer.flush();

    BufferedReader reader = new BufferedReader(new InputStreamReader(urlConnection.getInputStream(), "UTF-8"));
    return JsonUtils.stringToJsonNode(reader.readLine());
  }

  private boolean updateIndexConfig(String tableName, TableConfig tableConfig)
      throws Exception {
    String request =
        ControllerRequestURLBuilder.baseUrl("http://" + _controllerAddress).forTableUpdateIndexingConfigs(tableName);
    HttpURLConnection httpURLConnection = (HttpURLConnection) new URL(request).openConnection();
    httpURLConnection.setDoOutput(true);
    httpURLConnection.setRequestMethod("PUT");

    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(httpURLConnection.getOutputStream(), "UTF-8"));
    writer.write(tableConfig.toJsonString());
    writer.flush();

    BufferedReader reader = new BufferedReader(new InputStreamReader(httpURLConnection.getInputStream(), "UTF-8"));
    return reader.readLine().equals("done");
  }

  private static class ResultPair implements Comparable<ResultPair> {
    private final String _key;
    private final long _value;

    public ResultPair(String key, long value) {
      _key = key;
      _value = value;
    }

    @Override
    public int compareTo(@Nonnull ResultPair o) {
      return Long.compare(o._value, _value);
    }

    @Override
    public String toString() {
      return _key + ": " + _value;
    }
  }
}
