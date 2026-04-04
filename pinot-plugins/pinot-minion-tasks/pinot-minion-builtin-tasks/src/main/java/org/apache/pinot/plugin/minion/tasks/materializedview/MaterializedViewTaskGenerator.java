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
package org.apache.pinot.plugin.minion.tasks.materializedview;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.task.TaskState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.minion.MaterializedViewMetadata;
import org.apache.pinot.common.minion.MaterializedViewMetadataUtils;
import org.apache.pinot.common.minion.MaterializedViewTaskMetadata;
import org.apache.pinot.controller.helix.core.minion.generator.BaseTaskGenerator;
import org.apache.pinot.controller.helix.core.minion.generator.TaskGeneratorUtils;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.common.MinionConstants.MaterializedViewTask;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.annotations.minion.TaskGenerator;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Task generator for {@link MaterializedViewTask}.
 *
 * <p>Unlike segment-conversion tasks, this generator does not scan source segments. It only
 * computes a time window and appends it to the user-defined SQL, producing a
 * {@link PinotTaskConfig} for the executor.
 *
 * <p>Steps:
 * <ol>
 *   <li>Parse the source table name from the SQL {@code FROM} clause.</li>
 *   <li>Read the watermark from {@link MaterializedViewTaskMetadata} ZNode.
 *       On cold-start, find the minimum segment start time from the source table and
 *       align it to the bucket boundary.</li>
 *   <li>Compute the execution window: {@code [watermarkMs, watermarkMs + bucketMs)}.</li>
 *   <li>Skip if the window end is in the future.</li>
 *   <li>Look up the source table's time column and append a {@code WHERE} time-range
 *       filter to the SQL.</li>
 *   <li>Emit a single {@link PinotTaskConfig} per table.</li>
 * </ol>
 */
@TaskGenerator
public class MaterializedViewTaskGenerator extends BaseTaskGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MaterializedViewTaskGenerator.class);

  private static final String DEFAULT_BUCKET_PERIOD = "1d";

  @Override
  public String getTaskType() {
    return MaterializedViewTask.TASK_TYPE;
  }

  @Override
  public List<PinotTaskConfig> generateTasks(List<TableConfig> tableConfigs) {
    String taskType = MaterializedViewTask.TASK_TYPE;
    List<PinotTaskConfig> pinotTaskConfigs = new ArrayList<>();

    for (TableConfig tableConfig : tableConfigs) {
      String offlineTableName = tableConfig.getTableName();

      if (tableConfig.getTableType() != TableType.OFFLINE) {
        LOGGER.warn("Skip generating task: {} for non-OFFLINE table: {}", taskType, offlineTableName);
        continue;
      }
      LOGGER.info("Start generating task configs for table: {} for task: {}", offlineTableName, taskType);

      // Only schedule 1 task of this type per table
      Map<String, TaskState> incompleteTasks =
          TaskGeneratorUtils.getIncompleteTasks(taskType, offlineTableName, _clusterInfoAccessor);
      if (!incompleteTasks.isEmpty()) {
        LOGGER.warn("Found incomplete tasks: {} for table: {} and task type: {}. Skipping.",
            incompleteTasks.keySet(), offlineTableName, taskType);
        continue;
      }

      TableTaskConfig tableTaskConfig = tableConfig.getTaskConfig();
      Preconditions.checkState(tableTaskConfig != null);
      Map<String, String> taskConfigs = tableTaskConfig.getConfigsForTaskType(taskType);
      Preconditions.checkState(taskConfigs != null, "Task config shouldn't be null for table: %s", offlineTableName);

      String definedSQL = taskConfigs.get(MaterializedViewTask.DEFINED_SQL_KEY);
      Preconditions.checkState(definedSQL != null && !definedSQL.isEmpty(),
          "definedSQL must be specified for table: %s", offlineTableName);

      // Parse source table via Calcite AST -- needed for cold-start watermark computation
      String sourceTableName = MaterializedViewAnalyzer.extractSourceTableName(definedSQL);

      // Bucket and buffer
      String bucketTimePeriod =
          taskConfigs.getOrDefault(MaterializedViewTask.BUCKET_TIME_PERIOD_KEY, DEFAULT_BUCKET_PERIOD);
      long bucketMs = TimeUtils.convertPeriodToMillis(bucketTimePeriod);
      String bufferTimePeriod =
          taskConfigs.getOrDefault(MaterializedViewTask.BUFFER_TIME_PERIOD_KEY, "0d");
      long bufferMs = TimeUtils.convertPeriodToMillis(bufferTimePeriod);

      // Watermark (cold-start uses source table's earliest segment time)
      long windowStartMs = getWatermarkMs(offlineTableName, sourceTableName, bucketMs, definedSQL);
      long windowEndMs = windowStartMs + bucketMs;

      // Skip if window end is too recent (within buffer period from now)
      if (windowEndMs > System.currentTimeMillis() - bufferMs) {
        LOGGER.info("Window [{}, {}) is within buffer period ({}) from now. Skipping task: {}",
            windowStartMs, windowEndMs, bufferTimePeriod, taskType);
        continue;
      }

      // Resolve source time column, add it to SELECT/GROUP BY, and append time-range filter
      String sourceTimeColumn = resolveSourceTimeColumn(sourceTableName);
      String sqlWithTimeColumn = appendTimeColumnToSelect(definedSQL, sourceTimeColumn);
      DateTimeFormatSpec timeFormatSpec = resolveSourceTimeFormatSpec(sourceTableName, sourceTimeColumn);
      String windowStart = timeFormatSpec.fromMillisToFormat(windowStartMs);
      String windowEnd = timeFormatSpec.fromMillisToFormat(windowEndMs);
      String sqlWithTimeRange = appendTimeRange(sqlWithTimeColumn, sourceTimeColumn, windowStart, windowEnd);

      // Build task config
      Map<String, String> configs = new HashMap<>();
      configs.put(MinionConstants.TABLE_NAME_KEY, offlineTableName);
      configs.put(MaterializedViewTask.DEFINED_SQL_KEY, sqlWithTimeRange);
      configs.put(MaterializedViewTask.ORIGINAL_DEFINED_SQL_KEY, definedSQL);
      configs.put(MaterializedViewTask.WINDOW_START_MS_KEY, String.valueOf(windowStartMs));
      configs.put(MaterializedViewTask.WINDOW_END_MS_KEY, String.valueOf(windowEndMs));
      configs.put(MaterializedViewTask.SOURCE_TABLE_NAME_KEY, sourceTableName);
      configs.put(MinionConstants.UPLOAD_URL_KEY,
          _clusterInfoAccessor.getVipUrl() + "/segments");

      String maxNumRecords = taskConfigs.get(MaterializedViewTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY);
      if (maxNumRecords != null) {
        configs.put(MaterializedViewTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY, maxNumRecords);
      }

      pinotTaskConfigs.add(new PinotTaskConfig(taskType, configs));
      LOGGER.info("Finished generating task configs for table: {} for task: {}", offlineTableName, taskType);
    }
    return pinotTaskConfigs;
  }

  @Override
  public void validateTaskConfigs(TableConfig tableConfig, Schema schema, Map<String, String> taskConfigs) {
    MaterializedViewAnalyzer.analyze(
        taskConfigs.get(MaterializedViewTask.DEFINED_SQL_KEY),
        tableConfig, schema, taskConfigs, _clusterInfoAccessor);
  }

  /**
   * Resolves the time column for the source table by looking up its TableConfig.
   */
  private String resolveSourceTimeColumn(String rawSourceTableName) {
    // Try OFFLINE first, then REALTIME
    String sourceTableWithType = TableNameBuilder.OFFLINE.tableNameWithType(rawSourceTableName);
    TableConfig sourceTableConfig = _clusterInfoAccessor.getTableConfig(sourceTableWithType);
    if (sourceTableConfig == null) {
      sourceTableWithType = TableNameBuilder.REALTIME.tableNameWithType(rawSourceTableName);
      sourceTableConfig = _clusterInfoAccessor.getTableConfig(sourceTableWithType);
    }
    Preconditions.checkState(sourceTableConfig != null,
        "Source table config not found for: %s", rawSourceTableName);

    String timeColumn = sourceTableConfig.getValidationConfig().getTimeColumnName();
    Preconditions.checkState(timeColumn != null && !timeColumn.isEmpty(),
        "Time column not configured for source table: %s", rawSourceTableName);
    return timeColumn;
  }

  /**
   * Resolves the {@link DateTimeFormatSpec} for the source table's time column by looking up
   * the table schema. This spec is used to convert millisecond-based watermarks to the
   * time column's native format (e.g. days since epoch for {@code 1:DAYS:EPOCH}).
   */
  private DateTimeFormatSpec resolveSourceTimeFormatSpec(String rawSourceTableName, String timeColumn) {
    String sourceTableWithType = resolveSourceTableNameWithType(rawSourceTableName);
    Schema sourceSchema = _clusterInfoAccessor.getTableSchema(sourceTableWithType);
    Preconditions.checkState(sourceSchema != null,
        "Schema not found for source table: %s", rawSourceTableName);

    DateTimeFieldSpec fieldSpec = sourceSchema.getSpecForTimeColumn(timeColumn);
    Preconditions.checkState(fieldSpec != null,
        "No DateTimeFieldSpec found for time column '%s' in source table: %s", timeColumn, rawSourceTableName);
    return fieldSpec.getFormatSpec();
  }

  /**
   * Injects the time column into the SQL SELECT list and, if a GROUP BY clause is present,
   * appends it there too. If the time column already appears in the SELECT list (case-insensitive),
   * the SQL is returned unchanged.
   *
   * <p>Example: given {@code SELECT city, count(*) as cnt FROM orders GROUP BY city} and
   * time column {@code DaysSinceEpoch}, produces
   * {@code SELECT DaysSinceEpoch, city, count(*) as cnt FROM orders GROUP BY city, DaysSinceEpoch}.
   */
  static String appendTimeColumnToSelect(String sql, String timeColumn) {
    String trimmed = sql.trim();
    if (trimmed.endsWith(";")) {
      trimmed = trimmed.substring(0, trimmed.length() - 1).trim();
    }

    String upperSql = trimmed.toUpperCase();
    String upperTimeCol = timeColumn.toUpperCase();

    // Check if the time column already exists in the SELECT list
    int selectIdx = upperSql.indexOf("SELECT ");
    Preconditions.checkState(selectIdx >= 0, "No SELECT keyword in SQL: %s", sql);
    int fromIdx = upperSql.indexOf(" FROM ");
    Preconditions.checkState(fromIdx > selectIdx, "No FROM keyword in SQL: %s", sql);

    String selectPart = upperSql.substring(selectIdx + 7, fromIdx);
    String[] selectItems = selectPart.split(",");
    for (String item : selectItems) {
      if (item.trim().equals(upperTimeCol)) {
        return trimmed;
      }
    }

    // Insert time column right after "SELECT "
    int insertAfterSelect = selectIdx + 7;
    String result = trimmed.substring(0, insertAfterSelect) + timeColumn + ", "
        + trimmed.substring(insertAfterSelect);

    // If GROUP BY exists, append time column to it
    String upperResult = result.toUpperCase();
    int groupByIdx = upperResult.indexOf(" GROUP BY ");
    if (groupByIdx >= 0) {
      int groupByContentStart = groupByIdx + 10;
      int groupByEnd = findClauseEndAfterGroupBy(upperResult, groupByContentStart);
      result = result.substring(0, groupByEnd) + ", " + timeColumn + result.substring(groupByEnd);
    }

    return result;
  }

  /**
   * Finds the end of the GROUP BY column list, i.e. the position of the next major clause
   * keyword (ORDER, HAVING, LIMIT) or the end of the string.
   */
  private static int findClauseEndAfterGroupBy(String upperSql, int fromIdx) {
    String[] keywords = {" ORDER ", " HAVING ", " LIMIT "};
    int minIdx = upperSql.length();
    for (String keyword : keywords) {
      int idx = upperSql.indexOf(keyword, fromIdx);
      if (idx >= 0 && idx < minIdx) {
        minIdx = idx;
      }
    }
    return minIdx;
  }

  /**
   * Appends a time-range WHERE clause to the SQL. The window values must already be in the
   * time column's native format (e.g. days since epoch, not milliseconds). If a WHERE clause
   * already exists, appends with AND; otherwise inserts before GROUP BY / ORDER BY / the
   * trailing semicolon.
   */
  static String appendTimeRange(String sql, String timeColumn, String windowStart, String windowEnd) {
    String timeFilter = timeColumn + " >= " + windowStart + " AND " + timeColumn + " < " + windowEnd;

    // Remove trailing semicolon for easier manipulation
    String trimmed = sql.trim();
    if (trimmed.endsWith(";")) {
      trimmed = trimmed.substring(0, trimmed.length() - 1).trim();
    }

    String upperSql = trimmed.toUpperCase();
    int whereIdx = upperSql.indexOf(" WHERE ");
    if (whereIdx >= 0) {
      // Find the end of the existing WHERE conditions (before GROUP BY, ORDER BY, LIMIT, or end)
      int insertPos = findClauseEnd(upperSql, whereIdx + 7);
      return trimmed.substring(0, insertPos) + " AND " + timeFilter + trimmed.substring(insertPos);
    }

    // No WHERE — insert before GROUP BY / ORDER BY / LIMIT / HAVING / end
    int insertPos = findClauseEnd(upperSql, upperSql.indexOf(" FROM ") + 6);
    // Move past the table name to find where to insert
    insertPos = findClauseEnd(upperSql, insertPos);
    return trimmed.substring(0, insertPos) + " WHERE " + timeFilter + trimmed.substring(insertPos);
  }

  /**
   * Finds the position of the next major SQL clause keyword (GROUP, ORDER, HAVING, LIMIT)
   * starting from {@code fromIdx}, or the end of the string if none found.
   */
  private static int findClauseEnd(String upperSql, int fromIdx) {
    String[] keywords = {" GROUP ", " ORDER ", " HAVING ", " LIMIT "};
    int minIdx = upperSql.length();
    for (String keyword : keywords) {
      int idx = upperSql.indexOf(keyword, fromIdx);
      if (idx >= 0 && idx < minIdx) {
        minIdx = idx;
      }
    }
    return minIdx;
  }

  /**
   * Reads the watermark from ZK or initialises it on cold-start by finding the minimum
   * segment start time from the source table and aligning it to the bucket boundary.
   */
  private long getWatermarkMs(String mvTableName, String sourceTableName, long bucketMs, String definedSQL) {
    ZNRecord znRecord = _clusterInfoAccessor.getMinionTaskMetadataZNRecord(
        MaterializedViewTask.TASK_TYPE, mvTableName);
    MaterializedViewTaskMetadata metadata =
        znRecord != null ? MaterializedViewTaskMetadata.fromZNRecord(znRecord) : null;

    if (metadata == null) {
      // Cold-start: find the earliest segment start time from the source table
      String sourceTableWithType = resolveSourceTableNameWithType(sourceTableName);
      List<SegmentZKMetadata> segmentsZKMetadata = getSegmentsZKMetadataForTable(sourceTableWithType);

      long minStartTimeMs = Long.MAX_VALUE;
      for (SegmentZKMetadata segmentZKMetadata : segmentsZKMetadata) {
        long startTimeMs = segmentZKMetadata.getStartTimeMs();
        if (startTimeMs > 0) {
          minStartTimeMs = Math.min(minStartTimeMs, startTimeMs);
        }
      }
      Preconditions.checkState(minStartTimeMs != Long.MAX_VALUE,
          "No valid segments found in source table: %s for cold-start watermark", sourceTableName);

      long watermarkMs = (minStartTimeMs / bucketMs) * bucketMs;
      metadata = new MaterializedViewTaskMetadata(mvTableName, watermarkMs);
      _clusterInfoAccessor.setMinionTaskMetadata(metadata, MaterializedViewTask.TASK_TYPE, -1);
      LOGGER.info("Cold-start: initialized watermark to {} for MV table: {} from source table: {}",
          watermarkMs, mvTableName, sourceTableName);

      // Initialize MaterializedViewMetadata with base table info and empty partition maps
      MaterializedViewMetadata mvMetadata = new MaterializedViewMetadata(
          mvTableName,
          Collections.singletonList(sourceTableName),
          sourceTableName,
          definedSQL,
          new HashMap<>(), new HashMap<>());
      MaterializedViewMetadataUtils.persistMaterializedViewMetadata(
          _clusterInfoAccessor.getPinotHelixResourceManager().getPropertyStore(), mvMetadata, -1);
      LOGGER.info("Cold-start: initialized MaterializedViewMetadata for MV table: {} with source table: {}",
          mvTableName, sourceTableName);
    }
    return metadata.getWatermarkMs();
  }

  /**
   * Resolves the source table name with type suffix. Tries OFFLINE first, then REALTIME.
   */
  private String resolveSourceTableNameWithType(String rawSourceTableName) {
    String sourceTableWithType = TableNameBuilder.OFFLINE.tableNameWithType(rawSourceTableName);
    TableConfig sourceTableConfig = _clusterInfoAccessor.getTableConfig(sourceTableWithType);
    if (sourceTableConfig != null) {
      return sourceTableWithType;
    }
    sourceTableWithType = TableNameBuilder.REALTIME.tableNameWithType(rawSourceTableName);
    sourceTableConfig = _clusterInfoAccessor.getTableConfig(sourceTableWithType);
    Preconditions.checkState(sourceTableConfig != null,
        "Source table config not found for: %s", rawSourceTableName);
    return sourceTableWithType;
  }
}
