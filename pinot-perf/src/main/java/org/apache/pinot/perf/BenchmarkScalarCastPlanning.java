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


/// Measures SQL parsing, validation and logical optimization of scalar filters using constant string casts or
/// equivalent numeric/boolean literals. Predicates compare each constant with a matching column type. DECIMAL uses
/// the schema's default precision and scale. No cluster or query execution is needed.
/// Each thread owns its environment and cycles through 1,024 prebuilt queries. Numeric cast values vary with each
/// query; boolean values alternate, with a varying account filter keeping the queries distinct. Query construction
/// stays outside the measured operation, and warmup populates reusable conversion templates.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(2)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Thread)
public class BenchmarkScalarCastPlanning {
  private static final String TABLE_NAME = "scalar_events";
  private static final int NUM_QUERIES = 1024;

  @Param({"INTEGER", "BIGINT", "DOUBLE", "DECIMAL", "BOOLEAN"})
  private String _dataType;

  @Param({"CAST", "LITERAL"})
  private String _literalForm;

  private QueryEnvironment _queryEnvironment;
  private String[] _queries;
  private int _nextQuery;

  @Setup
  public void setUp() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension("account_id", DataType.LONG)
        .addSingleValueDimension("int_value", DataType.INT)
        .addSingleValueDimension("long_value", DataType.LONG)
        .addSingleValueDimension("double_value", DataType.DOUBLE)
        .addSingleValueDimension("decimal_value", DataType.BIG_DECIMAL)
        .addSingleValueDimension("boolean_value", DataType.BOOLEAN)
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

    _queries = new String[NUM_QUERIES];
    for (int i = 0; i < NUM_QUERIES; i++) {
      String column;
      String value;
      String literal;
      switch (_dataType) {
        case "INTEGER":
          column = "int_value";
          value = Integer.toString(1000000 + i);
          literal = value;
          break;
        case "BIGINT":
          column = "long_value";
          value = Long.toString(5000000000L + i);
          literal = value;
          break;
        case "DOUBLE":
          column = "double_value";
          value = (1000000 + i) + ".25";
          literal = value + "E0";
          break;
        case "DECIMAL":
          column = "decimal_value";
          value = (1000000 + i) + ".25";
          literal = value;
          break;
        case "BOOLEAN":
          column = "boolean_value";
          value = (i & 1) == 0 ? "true" : "false";
          literal = value;
          break;
        default:
          throw new IllegalArgumentException("Unknown data type: " + _dataType);
      }
      String constant;
      switch (_literalForm) {
        case "CAST":
          constant = "CAST('" + value + "' AS " + _dataType + ")";
          break;
        case "LITERAL":
          constant = literal;
          break;
        default:
          throw new IllegalArgumentException("Unknown literal form: " + _literalForm);
      }
      _queries[i] = "SELECT account_id, " + column + " FROM " + TABLE_NAME + " WHERE " + column + " = " + constant
          + " AND account_id >= " + (1000000L + i) + " ORDER BY account_id LIMIT 1000";
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
