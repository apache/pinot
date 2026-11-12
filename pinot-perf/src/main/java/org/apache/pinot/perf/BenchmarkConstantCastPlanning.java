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
package org.apache.pinot.perf;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.config.provider.StaticTableCache;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;


/// Measures SQL parsing, validation and logical optimization of timestamp range filters, with and without a window
/// selecting the latest version of each event. CAST, EPOCH_STRING_CAST and TIMESTAMP_LITERAL use equivalent constants;
/// EPOCH_MILLIS uses a LONG time column as a control without timestamp casts. No cluster or query execution is needed.
/// Each benchmark thread owns its environment and cycles through prebuilt queries with different account IDs, keeping
/// query-string construction outside the measured operation.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(2)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Thread)
public class BenchmarkConstantCastPlanning {
  private static final String TABLE_NAME = "events";
  private static final int NUM_QUERIES = 1024;

  @Param({"FILTER", "WINDOW"})
  private String _queryShape;

  @Param({"CAST", "EPOCH_STRING_CAST", "TIMESTAMP_LITERAL", "EPOCH_MILLIS"})
  private String _literalForm;

  private QueryEnvironment _queryEnvironment;
  private String[] _queries;
  private int _nextQuery;

  @Setup
  public void setUp() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension("account_id", DataType.LONG)
        .addSingleValueDimension("version", DataType.LONG)
        .addSingleValueDimension("sequence_id", DataType.LONG)
        .addMetric("reading", DataType.DOUBLE)
        .addDateTime("event_time", DataType.TIMESTAMP, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .addDateTime("event_time_ms", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
    schema.setEnableColumnBasedNullHandling(true);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    StaticTableCache tableCache = new StaticTableCache(List.of(tableConfig), List.of(schema), List.of(), false);
    _queryEnvironment = new QueryEnvironment(QueryEnvironment.configBuilder()
        .requestId(0L)
        .database(CommonConstants.DEFAULT_DATABASE)
        .tableCache(tableCache)
        .isNullHandlingEnabled(true)
        .defaultUsePhysicalOptimizer(false)
        .build());

    String timeColumn = "event_time";
    String lowerBound;
    String upperBound;
    switch (_literalForm) {
      case "CAST":
        lowerBound = "CAST('2024-01-01 00:00:00' AS TIMESTAMP)";
        upperBound = "CAST('2024-02-01 00:00:00' AS TIMESTAMP)";
        break;
      case "EPOCH_STRING_CAST":
        lowerBound = "CAST('1704067200000' AS TIMESTAMP)";
        upperBound = "CAST('1706745600000' AS TIMESTAMP)";
        break;
      case "TIMESTAMP_LITERAL":
        lowerBound = "TIMESTAMP '2024-01-01 00:00:00'";
        upperBound = "TIMESTAMP '2024-02-01 00:00:00'";
        break;
      case "EPOCH_MILLIS":
        timeColumn = "event_time_ms";
        lowerBound = "1704067200000";
        upperBound = "1706745600000";
        break;
      default:
        throw new IllegalArgumentException("Unknown literal form: " + _literalForm);
    }

    _queries = new String[NUM_QUERIES];
    for (int i = 0; i < NUM_QUERIES; i++) {
      String filter = " FROM " + TABLE_NAME + " WHERE account_id = " + (1000000L + i)
          + " AND " + timeColumn + " >= " + lowerBound + " AND " + timeColumn + " < " + upperBound;
      switch (_queryShape) {
        case "FILTER":
          _queries[i] = "SELECT " + timeColumn + ", reading" + filter + " ORDER BY " + timeColumn + " LIMIT 1000";
          break;
        case "WINDOW":
          _queries[i] = "SELECT " + timeColumn + ", reading FROM (SELECT " + timeColumn
              + ", reading, ROW_NUMBER() OVER (PARTITION BY account_id, " + timeColumn
              + " ORDER BY version DESC, sequence_id DESC) AS row_num" + filter
              + ") WHERE row_num = 1 ORDER BY " + timeColumn + " LIMIT 1000";
          break;
        default:
          throw new IllegalArgumentException("Unknown query shape: " + _queryShape);
      }
    }
    _nextQuery = 0;
  }

  @Benchmark
  public Set<String> compile() {
    String query = _queries[_nextQuery];
    _nextQuery = (_nextQuery + 1) & (NUM_QUERIES - 1);
    try (QueryEnvironment.CompiledQuery compiledQuery = _queryEnvironment.compile(query)) {
      return compiledQuery.getTableNames();
    }
  }
}
