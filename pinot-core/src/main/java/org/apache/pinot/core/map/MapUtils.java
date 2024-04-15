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

package org.apache.pinot.core.map;

import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.segment.local.segment.index.map.MapDataSource;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;


public class MapUtils {
  private MapUtils() {
  }

  /**
   * In the current model of integration between Map columns and the Pinot query engine, when an Item operation is
   * applied to a map column (e.g., `myMap['foo']`) we create a new DataSource that treats that expression as if it
   * were a column.  In other words, the Query Engine treats a Key within a Map column just as it would a user
   * defined Column. In order for this to work, we must map Item operations to unique column  names and then map
   * those unique column names to a Data Source. This function handles traversing a query expression, finding any
   * Map Item operations, constructing the unique internal column and mapping it to the appropriate Key Data Source.
   *
   * @param indexSegment
   * @param dataSourceMap - the Caller's mapping from column names to Data Source for that column. This function will
   *                      add Key's to this mapping.
   * @param expression - The expression to analyze for Map Item operations.
   */
  public static void addMapItemOperationsToDataSourceMap(IndexSegment indexSegment,
      Map<String, DataSource> dataSourceMap, ExpressionContext expression) {
    if (expression.getType() == ExpressionContext.Type.FUNCTION) {
      if (expression.getFunction().getFunctionName().equals("item")) {
        String columnOp = expression.getFunction().getArguments().get(0).toString();
        String key = expression.getFunction().getArguments().get(1).getLiteral().getStringValue();

        dataSourceMap.put(constructKeyDataSourceIdentifier(columnOp, key),
            ((MapDataSource) indexSegment.getDataSource(columnOp)).getKeyDataSource(key));
      } else {
        // Iterate over the operands and check if any of them are Map Item operations
        expression.getFunction().getArguments().forEach(
            arg -> addMapItemOperationsToDataSourceMap(indexSegment, dataSourceMap, arg));
      }
    }
  }

  /**
   * Constructs the internal identifier for DataSources that represent the values of a specific key within a Map
   * column.
   *
   * @param column
   * @param key
   * @return
   */
  public static String constructKeyDataSourceIdentifier(String column, String key) {
    return String.format("map_col__%s.%s", column, key);
  }
}
