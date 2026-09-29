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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.operator.window.WindowFrame;


public class NtileWindowFunction extends RankBasedWindowFunction {

  private final int _numBuckets;

  public NtileWindowFunction(RexExpression.FunctionCall aggCall, DataSchema inputSchema,
      List<RelFieldCollation> collations, WindowFrame windowFrame) {
    super(aggCall, inputSchema, collations, windowFrame);

    List<RexExpression> operands = aggCall.getFunctionOperands();
    Preconditions.checkArgument(operands.size() == 1, "NTILE function must have exactly 1 operand");
    RexExpression firstOperand = operands.get(0);
    Preconditions.checkArgument(firstOperand instanceof RexExpression.Literal,
        "The operand for the NTILE function must be a literal");
    Object operandValue = ((RexExpression.Literal) firstOperand).getValue();
    Preconditions.checkArgument(operandValue instanceof Number, "The operand for the NTILE function must be a number");
    _numBuckets = ((Number) operandValue).intValue();
  }

  @Override
  public List<Object> processRows(List<Object[]> rows) {
    int numRows = rows.size();
    List<Object> result = new ArrayList<>(numRows);
    int bucketSize = numRows / _numBuckets;

    if (numRows % _numBuckets == 0) {
      for (int i = 0; i < numRows; i++) {
        result.add(i / bucketSize + 1);
      }
    } else {
      int numLargeBuckets = numRows % _numBuckets;
      int largeBucketSize = bucketSize + 1;
      for (int i = 0; i < numRows; i++) {
        if (i < numLargeBuckets * largeBucketSize) {
          result.add(i / largeBucketSize + 1);
        } else {
          result.add(numLargeBuckets + (i - numLargeBuckets * largeBucketSize) / bucketSize + 1);
        }
      }
    }

    return result;
  }
}
