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

  public static void fillEmptyResponseTypes(
      BrokerResponse response, TableCache tableCache, Schema schema, String database, String query) {
    // 1) try to compile it with V2 engine and, in that case, take advantage of its schema validation
    // 2) just set the correct data type for each simple column projection
    // 3) fallback to the STRING type already returned by the server
    if (response == null || response.getResultTable() == null || response.getResultTable().getDataSchema() == null) {
      return;
    }
    QueryEnvironment queryEnvironment = new QueryEnvironment(database, tableCache, null);
    RelRoot node = queryEnvironment.getRelRootIfCanCompile(query);
    List<RelDataTypeField> nodeFields =
        node != null && node.validatedRowType != null
            ? node.validatedRowType.getFieldList()
            : null;
    String[] responseNames = response.getResultTable().getDataSchema().getColumnNames();
    DataSchema.ColumnDataType[] responseTypes = response.getResultTable().getDataSchema().getColumnDataTypes();
    for (int i = 0; i < responseTypes.length; i++) {
      DataSchema.ColumnDataType resolved = null;
      try {
        if (nodeFields != null) {
          resolved = RelToPlanNodeConverter.convertToColumnDataType(nodeFields.get(i).getType());
        }
        if (resolved == null || DataSchema.ColumnDataType.UNKNOWN.equals(resolved)) {
          FieldSpec spec = schema.getFieldSpecFor(responseNames[i]);
          if (spec != null) {
            try {
              resolved = DataSchema.ColumnDataType.fromDataType(spec.getDataType(), false);
            } catch (Exception e) {
              resolved = DataSchema.ColumnDataType.fromDataType(spec.getDataType(), true);
            }
          }
        }
        if (resolved == null || DataSchema.ColumnDataType.UNKNOWN.equals(resolved)) {
          resolved = DataSchema.ColumnDataType.STRING;
        }
      } catch (Exception e) {
        resolved = DataSchema.ColumnDataType.STRING;
      }
      responseTypes[i] = resolved;
    }
  }
}
