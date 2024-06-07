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
package org.apache.pinot.query.runtime.operator.window.value;

import java.util.Arrays;
import java.util.List;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;


public class LeadValueWindowFunction extends ValueWindowFunction {

  public LeadValueWindowFunction(RexExpression.FunctionCall aggCall, DataSchema inputSchema,
      List<RelFieldCollation> collations, boolean partitionByOnly) {
    super(aggCall, inputSchema, collations, partitionByOnly);
  }

  @Override
  public List<Object> processRows(List<Object[]> rows) {
    int numRows = rows.size();
    Object[] result = new Object[numRows];
    Object[] nextRow = null;
    for (int i = numRows - 1; i >= 0; i--) {
      if (nextRow == null) {
        result[i] = null;
      } else {
        result[i] = extractValueFromRow(nextRow);
      }
      nextRow = rows.get(i);
    }
    return Arrays.asList(result);
  }
}
