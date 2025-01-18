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

import java.util.List;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
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
   * Tries to fill an empty or not properly filled schema when no rows have been returned.
   *
   * Priority is:
   * - Types in schema provided by V2 validation for the given query.
   * - Types in schema provided by V1 for the given table (only appliable to selection fields).
   * - Types in response provided by V1 server (no action).
   */
  public static void fillEmptyResponseSchema(
      BrokerResponse response, TableCache tableCache, Schema schema, String database, String query
  ) {
    if (response == null || response.getNumRowsResultSet() > 0) {
      return;
    }

    QueryEnvironment queryEnvironment = new QueryEnvironment(database, tableCache, null);
    RelRoot node = queryEnvironment.getRelRootIfCanCompile(query);
    DataSchema.ColumnDataType resolved;

    // V1 schema info for the response can be inaccurate or incomplete in several forms:
    // 1) No schema provided at all (when no segments have been even pruned).
    // 2) Schema provided but all columns set to default type (STRING) (when no segments have been matched).
    // V2 schema will be available only if query compiles.

    boolean hasV1Schema = response.getResultTable() != null;
    boolean hasV2Schema = node != null && node.validatedRowType != null;

    if (hasV1Schema && hasV2Schema) {
      // match v1 column types with v2 column types using column names
      // if no match, rely on v1 schema based on column name
      // if no match either, just leave it as it is
      DataSchema responseSchema = response.getResultTable().getDataSchema();
      List<RelDataTypeField> fields = node.validatedRowType.getFieldList();
      for (int i = 0; i < responseSchema.size(); i++) {
        resolved = RelToPlanNodeConverter.convertToColumnDataType(fields.get(i).getType());
        if (resolved == null || resolved.isUnknown()) {
          FieldSpec spec = schema.getFieldSpecFor(responseSchema.getColumnName(i));
          try {
            resolved = DataSchema.ColumnDataType.fromDataType(spec.getDataType(), false);
          } catch (Exception e) {
            try {
              resolved = DataSchema.ColumnDataType.fromDataType(spec.getDataType(), true);
            } catch (Exception e2) {
              resolved = DataSchema.ColumnDataType.UNKNOWN;
            }
          }
        }
        if (resolved == null || resolved.isUnknown()) {
          resolved = responseSchema.getColumnDataType(i);
        }
        responseSchema.getColumnDataTypes()[i] = resolved;
      }
    } else if (hasV1Schema) {
      // match v1 column types with v1 schema columns using column names
      // if no match, just leave it as it is
      DataSchema responseSchema = response.getResultTable().getDataSchema();
      for (int i = 0; i < responseSchema.size(); i++) {
        FieldSpec spec = schema.getFieldSpecFor(responseSchema.getColumnName(i));
        try {
          resolved = DataSchema.ColumnDataType.fromDataType(spec.getDataType(), false);
        } catch (Exception e) {
          try {
            resolved = DataSchema.ColumnDataType.fromDataType(spec.getDataType(), true);
          } catch (Exception e2) {
            resolved = DataSchema.ColumnDataType.UNKNOWN;
          }
        }
        if (resolved == null || resolved.isUnknown()) {
          resolved = responseSchema.getColumnDataType(i);
        }
        responseSchema.getColumnDataTypes()[i] = resolved;
      }
    } else if (hasV2Schema) {
      // trust v2 column types blindly
      // if a type cannot be resolved, leave it as UNKNOWN
      List<RelDataTypeField> fields = node.validatedRowType.getFieldList();
      String[] columnNames = new String[fields.size()];
      DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[fields.size()];
      for (int i = 0; i < fields.size(); i++) {
        columnNames[i] = fields.get(i).getName();
        resolved = RelToPlanNodeConverter.convertToColumnDataType(fields.get(i).getType());
        if (resolved == null) {
          resolved = DataSchema.ColumnDataType.UNKNOWN;
        }
        columnDataTypes[i] = resolved;
      }
      response.setResultTable(new ResultTable(new DataSchema(columnNames, columnDataTypes), List.of()));
    }
    // else { /* nothing else we can do */ }
  }
}
