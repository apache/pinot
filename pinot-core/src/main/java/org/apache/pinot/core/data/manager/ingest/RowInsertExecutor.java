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
package org.apache.pinot.core.data.manager.ingest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.ingest.InsertExecutor;
import org.apache.pinot.spi.ingest.InsertRequest;
import org.apache.pinot.spi.ingest.InsertResult;
import org.apache.pinot.spi.ingest.InsertStatementState;
import org.apache.pinot.spi.ingest.PreparedStore;
import org.apache.pinot.spi.ingest.ShardLog;
import org.apache.pinot.spi.ingest.ShardLogProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Server-side INSERT INTO executor using a local durable log and prepared store.
 *
 * <p><strong>Note (v1 limitation):</strong> This executor requires table configs to be registered
 * via {@link RowInsertExecutorFactory#registerTableConfig}. In v1, the primary INSERT path uses
 * {@code ControllerRowInsertExecutor} which resolves table configs from ZooKeeper. This server-side
 * executor is intended for a future direct-server-ingestion path where the server's
 * {@code InstanceDataManager} will register table configs as tables are loaded.
 *
 * <p>The execution flow for each request:
 * <ol>
 *   <li>Validate the request (table exists, rows present, etc.)</li>
 *   <li>Route rows to target partition(s) based on partition function</li>
 *   <li>Write rows to the {@link ShardLog} for each partition</li>
 *   <li>Write prepared batch to the {@link PreparedStore}</li>
 *   <li>Return {@link InsertResult} with state={@link InsertStatementState#PREPARED}</li>
 * </ol>
 *
 * <p>This class is thread-safe; a single instance may be invoked concurrently from multiple
 * request threads.
 */
public class RowInsertExecutor implements InsertExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(RowInsertExecutor.class);

  private final ShardLogProvider _shardLogProvider;
  private final PreparedStore _preparedStore;
  private final ConcurrentHashMap<String, InsertStatementState> _statementStates = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, StatementExecutionContext> _statementContexts =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, TableConfig> _tableConfigs;

  /**
   * Creates a new row insert executor.
   *
   * @param shardLogProvider the provider for shard logs
   * @param preparedStore the prepared store for staging data
   * @param tableConfigs a map of table name to table config for validation and partition routing
   */
  public RowInsertExecutor(ShardLogProvider shardLogProvider, PreparedStore preparedStore,
      ConcurrentHashMap<String, TableConfig> tableConfigs) {
    _shardLogProvider = shardLogProvider;
    _preparedStore = preparedStore;
    _tableConfigs = tableConfigs;
  }

  @Override
  public InsertResult execute(InsertRequest request) {
    String statementId = request.getStatementId();
    String tableName = request.getTableName();
    List<GenericRow> rows = request.getRows();

    LOGGER.info("Executing INSERT for statement {} on table {} with {} rows", statementId, tableName, rows.size());

    // Validate the request
    if (tableName == null || tableName.isEmpty()) {
      return buildErrorResult(statementId, "Table name is required", "INVALID_TABLE");
    }
    if (rows == null || rows.isEmpty()) {
      return buildErrorResult(statementId, "At least one row is required for ROW insert", "EMPTY_ROWS");
    }

    TableConfig tableConfig = _tableConfigs.get(tableName);
    if (tableConfig == null) {
      return buildErrorResult(statementId, "Table not found: " + tableName, "TABLE_NOT_FOUND");
    }

    // Create partition router
    InsertPartitionRouter router;
    try {
      router = new InsertPartitionRouter(tableConfig);
    } catch (IllegalArgumentException e) {
      return buildErrorResult(statementId, e.getMessage(), "PARTITION_CONFIG_ERROR");
    }

    // Route rows to partitions
    Map<Integer, List<GenericRow>> partitionedRows = router.routeRows(rows);
    List<Integer> preparedPartitions = new ArrayList<>(partitionedRows.size());

    _statementStates.put(statementId, InsertStatementState.ACCEPTED);

    // Write rows to shard log and prepared store for each partition
    try {
      for (Map.Entry<Integer, List<GenericRow>> entry : partitionedRows.entrySet()) {
        int partitionId = entry.getKey();
        List<GenericRow> partitionRows = entry.getValue();

        ShardLog shardLog = _shardLogProvider.getShardLog(tableName, partitionId);

        // Serialize and append rows to the shard log
        byte[] serializedRows = GenericRowSerializer.serializeRows(partitionRows);
        long startOffset = shardLog.append(serializedRows);
        long endOffset = startOffset;  // Single append, so start == end

        // Also write to prepared store for durability
        _preparedStore.store(statementId, partitionId, 0, serializedRows);

        // Mark the offset range as prepared in the shard log
        shardLog.prepare(statementId, startOffset, endOffset);
        preparedPartitions.add(partitionId);
      }

      _statementStates.put(statementId, InsertStatementState.PREPARED);
      _statementContexts.put(statementId, new StatementExecutionContext(tableName, preparedPartitions));

      LOGGER.info("INSERT statement {} prepared with {} partition(s)", statementId, partitionedRows.size());

      return new InsertResult.Builder()
          .setStatementId(statementId)
          .setState(InsertStatementState.PREPARED)
          .setMessage("Insert prepared with " + rows.size() + " rows across "
              + partitionedRows.size() + " partition(s)")
          .build();
    } catch (Exception e) {
      LOGGER.error("Failed to execute INSERT for statement {}", statementId, e);
      _statementStates.put(statementId, InsertStatementState.ABORTED);
      rollbackStatement(tableName, statementId, preparedPartitions);
      return buildErrorResult(statementId, "Failed to write rows: " + e.getMessage(), "WRITE_ERROR");
    }
  }

  @Override
  public InsertResult getStatus(String statementId) {
    InsertStatementState state = _statementStates.get(statementId);
    if (state == null) {
      return new InsertResult.Builder()
          .setStatementId(statementId)
          .setState(InsertStatementState.ABORTED)
          .setMessage("Unknown statement: " + statementId)
          .build();
    }
    return new InsertResult.Builder()
        .setStatementId(statementId)
        .setState(state)
        .build();
  }

  @Override
  public InsertResult abort(String statementId) {
    InsertStatementState currentState = _statementStates.get(statementId);
    if (currentState == null) {
      return new InsertResult.Builder()
          .setStatementId(statementId)
          .setState(InsertStatementState.ABORTED)
          .setMessage("Unknown statement: " + statementId)
          .build();
    }

    _statementStates.put(statementId, InsertStatementState.ABORTED);
    StatementExecutionContext context = _statementContexts.remove(statementId);
    if (context != null) {
      rollbackStatement(context._tableNameWithType, statementId, context._preparedPartitions);
    } else {
      try {
        _preparedStore.cleanup(statementId);
      } catch (Exception e) {
        LOGGER.warn("Failed to clean prepared data for aborted statement {}", statementId, e);
      }
    }

    LOGGER.info("Aborted INSERT statement {}", statementId);

    return new InsertResult.Builder()
        .setStatementId(statementId)
        .setState(InsertStatementState.ABORTED)
        .setMessage("Statement aborted")
        .build();
  }

  /**
   * Returns the current state of a statement from the in-memory map.
   */
  InsertStatementState getStatementState(String statementId) {
    return _statementStates.get(statementId);
  }

  private static InsertResult buildErrorResult(String statementId, String message, String errorCode) {
    return new InsertResult.Builder()
        .setStatementId(statementId)
        .setState(InsertStatementState.ABORTED)
        .setMessage(message)
        .setErrorCode(errorCode)
        .build();
  }

  private void rollbackStatement(String tableNameWithType, String statementId, List<Integer> preparedPartitions) {
    for (int partitionId : preparedPartitions) {
      try {
        _shardLogProvider.getShardLog(tableNameWithType, partitionId).abort(statementId);
      } catch (Exception e) {
        LOGGER.warn("Failed to abort shard log state for statement {} partition {}", statementId, partitionId, e);
      }
    }
    try {
      _preparedStore.cleanup(statementId);
    } catch (Exception e) {
      LOGGER.warn("Failed to clean prepared data for statement {}", statementId, e);
    }
  }

  private static final class StatementExecutionContext {
    private final String _tableNameWithType;
    private final List<Integer> _preparedPartitions;

    private StatementExecutionContext(String tableNameWithType, List<Integer> preparedPartitions) {
      _tableNameWithType = tableNameWithType;
      _preparedPartitions = List.copyOf(preparedPartitions);
    }
  }
}
