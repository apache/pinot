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
package org.apache.pinot.core.query.aggregation.utils;

import java.io.Serializable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.utils.rewriter.ParentAggregationResultRewriter;


/**
 * Interface for the result of a parent aggregation function, as can be used to populate the results of corresponding
 * of child aggregation functions. Each child aggregation function will have a corresponding column in the result
 * schema, please see {@link ParentAggregationResultRewriter} for more details.
 */
public interface ParentAggregationFunctionResultObject
    extends Comparable<ParentAggregationFunctionResultObject>, Serializable {

  /// Get the field from a projection column in the internal form (e.g. `ByteArray` for BYTES, `ByteArray[]` for
  /// BYTES_ARRAY). Callers that surface the value as a query result must convert to the external form first via
  /// [DataSchema.ColumnDataType#convert]
  Object getField(int rowId, int colId);

  // get total number of rows
  int getNumberOfRows();

  // get the nested schema of the result
  DataSchema getSchema();
}
