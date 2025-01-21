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
package org.apache.pinot.query.runtime.operator.window.range;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.operator.window.WindowFrame;
import org.apache.pinot.query.runtime.operator.window.WindowFunction;


/**
 * Rank based window functions don't support custom window frames (ROWS / RANGE) and are computed over the
 * entire partition. Calcite enforces that a custom window frame cannot be specified for these functions.
 */
public abstract class RankBasedWindowFunction extends WindowFunction {
  //@formatter:off
  public static final Map<String, Class<? extends WindowFunction>> WINDOW_FUNCTION_MAP =
      ImmutableMap.<String, Class<? extends WindowFunction>>builder()
          // Rank based window functions
          .put("ROW_NUMBER", RowNumberWindowFunction.class)
          .put("RANK", RankWindowFunction.class)
          .put("DENSE_RANK", DenseRankWindowFunction.class)
          .put("NTILE", NtileWindowFunction.class)
          .build();
  //@formatter:on

  public RankBasedWindowFunction(RexExpression.FunctionCall aggCall, DataSchema inputSchema,
      List<RelFieldCollation> collations, WindowFrame windowFrame) {
    super(aggCall, inputSchema, collations, windowFrame);
  }

  protected int compareRows(Object[] leftRow, Object[] rightRow) {
    for (int inputRef : _inputRefs) {
      if (inputRef < 0) {
        continue;
      }
      Object leftValue = leftRow[inputRef];
      Object rightValue = rightRow[inputRef];
      if (leftValue == null) {
        if (rightValue != null) {
          return -1;
        }
      } else {
        if (rightValue == null) {
          return 1;
        } else {
          //noinspection rawtypes,unchecked
          int result = ((Comparable) leftValue).compareTo(rightValue);
          if (result != 0) {
            return result;
          }
        }
      }
    }
    return 0;
  }
}
