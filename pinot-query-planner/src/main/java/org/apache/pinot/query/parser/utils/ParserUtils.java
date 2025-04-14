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
package org.apache.pinot.query.parser.utils;

import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.planner.logical.RelToPlanNodeConverter;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ParserUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(ParserUtils.class);

  private ParserUtils() {
  }

  /**
   * Returns whether the query can be parsed and compiled using the multi-stage query engine.
   */
  public static boolean canCompileWithMultiStageEngine(String query, String database, TableCache tableCache) {
    // try to parse and compile the query with the Calcite planner used by the multi-stage query engine
    long compileStartTime = System.currentTimeMillis();
    LOGGER.debug("Trying to compile query `{}` using the multi-stage query engine", query);
    QueryEnvironment queryEnvironment = new QueryEnvironment(database, tableCache, null);
    boolean canCompile = queryEnvironment.canCompileQuery(query);
    LOGGER.debug("Multi-stage query compilation time = {}ms", System.currentTimeMillis() - compileStartTime);
    return canCompile;
  }

  /**
   * Tries to fill an empty or not properly filled {@link DataSchema} when no row has been returned.
   *
   * Response data schema can be inaccurate or incomplete in several forms:
   * 1. No result table at all (when all segments have been pruned on broker).
   * 2. Data schema has all columns set to default type (STRING) (when all segments pruned on server).
   *
   * Priority is:
   * - Types from multi-stage engine validation for the given query (if allowed).
   * - Types from schema for the given table (only applicable to selection fields).
   * - Types from single-stage engine response (no action).
   *
   * Multi-stage engine schema will be available only if query compiles.
   */
  public static void fillEmptyResponseSchema(boolean useMSE, BrokerResponse response, TableCache tableCache,
      Schema schema, String database, String query) {
    Preconditions.checkState(response.getNumRowsResultSet() == 0, "Cannot fill schema for non-empty response");

    DataSchema dataSchema = response.getResultTable() != null ? response.getResultTable().getDataSchema() : null;

    List<RelDataTypeField> dataTypeFields = null;
    // Turn on (with pinot.broker.use.mse.to.fill.empty.response.schema=true or query option
    // useMSEToFillEmptyResponseSchema=true) only for clusters where no queries with huge IN clauses are expected
    // (see https://github.com/apache/pinot/issues/15064)
    if (useMSE) {
      QueryEnvironment queryEnvironment = new QueryEnvironment(database, tableCache, null);
      RelRoot root;
      try (QueryEnvironment.OptimizedQuery optimizedQuery = queryEnvironment.optimize(query)) {
        root = optimizedQuery.getRelRoot();
      } catch (Exception ignored) {
        root = null;
      }
      if (root != null && root.validatedRowType != null) {
        dataTypeFields = root.validatedRowType.getFieldList();
      }
    }

    if (dataSchema == null && dataTypeFields == null) {
      // No schema available, nothing we can do
      return;
    }

    if (dataSchema == null || (dataTypeFields != null && dataSchema.size() != dataTypeFields.size())) {
      // If data schema is not available or has different number of columns than the validated row type, we use the
      // validated row type to populate the schema.
      int numColumns = dataTypeFields.size();
      String[] columnNames = new String[numColumns];
      ColumnDataType[] columnDataTypes = new ColumnDataType[numColumns];
      for (int i = 0; i < numColumns; i++) {
        RelDataTypeField dataTypeField = dataTypeFields.get(i);
        columnNames[i] = dataTypeField.getName();
        ColumnDataType columnDataType;
        try {
          columnDataType = RelToPlanNodeConverter.convertToColumnDataType(dataTypeField.getType());
        } catch (Exception ignored) {
          columnDataType = ColumnDataType.UNKNOWN;
        }
        columnDataTypes[i] = columnDataType;
      }
      response.setResultTable(new ResultTable(new DataSchema(columnNames, columnDataTypes), List.of()));
      return;
    }

    // When data schema is available, try to fix the data types within it.
    ColumnDataType[] columnDataTypes = dataSchema.getColumnDataTypes();
    int numColumns = columnDataTypes.length;
    if (dataTypeFields != null) {
      // Fill data type with the validated row type when it is available.
      for (int i = 0; i < numColumns; i++) {
        try {
          columnDataTypes[i] = RelToPlanNodeConverter.convertToColumnDataType(dataTypeFields.get(i).getType());
        } catch (Exception ignored) {
          // Ignore exception and keep the type from response
        }
      }
    } else {
      // Fill data type with the schema when validated row type is not available.
      String[] columnNames = dataSchema.getColumnNames();
      for (int i = 0; i < numColumns; i++) {
        FieldSpec fieldSpec = schema.getFieldSpecFor(columnNames[i]);
        if (fieldSpec != null) {
          try {
            columnDataTypes[i] = ColumnDataType.fromDataType(fieldSpec.getDataType(), fieldSpec.isSingleValueField());
          } catch (Exception ignored) {
            // Ignore exception and keep the type from response
          }
        }
      }
    }
  }
}
