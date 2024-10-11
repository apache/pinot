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
package org.apache.pinot.calcite.sql2rel;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.fun.SqlBetweenOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.util.Litmus;


/**
 * PinotConvertletTable is a wrapper of {@link StandardConvertletTable} with the customizations of not converting
 * certain SqlCalls, e.g. TIMESTAMPADD, TIMESTAMPDIFF.
 */
public class PinotConvertletTable implements SqlRexConvertletTable {

  public static final PinotConvertletTable INSTANCE = new PinotConvertletTable();
  private static final SqlBetweenOperator PINOT_BETWEEN =
      new SqlBetweenOperator(SqlBetweenOperator.Flag.ASYMMETRIC, false) {
        @Override
        public boolean validRexOperands(int count, Litmus litmus) {
          return litmus.succeed();
        }
      };

  private PinotConvertletTable() {
  }

  @Nullable
  @Override
  public SqlRexConvertlet get(SqlCall call) {
    switch (call.getKind()) {
      case TIMESTAMP_ADD:
        return TimestampAddConvertlet.INSTANCE;
      case TIMESTAMP_DIFF:
        return TimestampDiffConvertlet.INSTANCE;
      case BETWEEN:
        return BetweenConvertlet.INSTANCE;
      default:
        return StandardConvertletTable.INSTANCE.get(call);
    }
  }

  /**
   * Override {@link org.apache.calcite.sql2rel.StandardConvertletTable.TimestampAddConvertlet} to not convert the
   * SqlCall to arithmetic time expression.
   */
  private static class TimestampAddConvertlet implements SqlRexConvertlet {
    private static final TimestampAddConvertlet INSTANCE = new TimestampAddConvertlet();

    @Override
    public RexNode convertCall(SqlRexContext cx, SqlCall call) {
      RexBuilder rexBuilder = cx.getRexBuilder();
      return rexBuilder.makeCall(cx.getValidator().getValidatedNodeType(call), SqlStdOperatorTable.TIMESTAMP_ADD,
          List.of(cx.convertExpression(call.operand(0)), cx.convertExpression(call.operand(1)),
              cx.convertExpression(call.operand(2))));
    }
  }

  /**
   * Override {@link org.apache.calcite.sql2rel.StandardConvertletTable.TimestampDiffConvertlet} to not convert the
   * SqlCall to arithmetic time expression.
   */
  private static class TimestampDiffConvertlet implements SqlRexConvertlet {
    private static final TimestampDiffConvertlet INSTANCE = new TimestampDiffConvertlet();

    @Override
    public RexNode convertCall(SqlRexContext cx, SqlCall call) {
      RexBuilder rexBuilder = cx.getRexBuilder();
      return rexBuilder.makeCall(cx.getValidator().getValidatedNodeType(call), SqlStdOperatorTable.TIMESTAMP_DIFF,
          List.of(cx.convertExpression(call.operand(0)), cx.convertExpression(call.operand(1)),
              cx.convertExpression(call.operand(2))));
    }
  }

  /**
   * Override the standard convertlet for BETWEEN to avoid the rewrite to >= AND <= for MV columns since that breaks
   * the filter predicate's semantics.
   */
  private static class BetweenConvertlet implements SqlRexConvertlet {
    private static final BetweenConvertlet INSTANCE = new BetweenConvertlet();

    @Override
    public RexNode convertCall(SqlRexContext cx, SqlCall call) {
      if (call.operand(0) instanceof SqlCall && ((SqlCall) call.operand(0)).getOperator().getName()
          .equals("ARRAY_TO_MV")) {
        RexBuilder rexBuilder = cx.getRexBuilder();

        SqlBetweenOperator betweenOperator = (SqlBetweenOperator) call.getOperator();

        RexNode rexNode = rexBuilder.makeCall(cx.getValidator().getValidatedNodeType(call), PINOT_BETWEEN,
            List.of(cx.convertExpression(call.operand(0)), cx.convertExpression(call.operand(1)),
                cx.convertExpression(call.operand(2))));

        // Since Pinot only has support for ASYMMETRIC BETWEEN, we need to rewrite SYMMETRIC BETWEEN, ASYMMETRIC NOT
        // BETWEEN, and SYMMETRIC NOT BETWEEN to the equivalent BETWEEN expressions.

        // (val BETWEEN SYMMETRIC x AND y) is equivalent to (val BETWEEN x AND y OR val BETWEEN y AND x)
        if (betweenOperator.flag == SqlBetweenOperator.Flag.SYMMETRIC) {
          RexNode flipped = rexBuilder.makeCall(cx.getValidator().getValidatedNodeType(call), PINOT_BETWEEN,
              List.of(cx.convertExpression(call.operand(0)), cx.convertExpression(call.operand(2)),
                  cx.convertExpression(call.operand(1))));
          rexNode = rexBuilder.makeCall(SqlStdOperatorTable.OR, rexNode, flipped);
        }

        if (betweenOperator.isNegated()) {
          rexNode = rexBuilder.makeCall(SqlStdOperatorTable.NOT, rexNode);
        }

        return rexNode;
      } else {
        return StandardConvertletTable.INSTANCE.convertBetween(cx, (SqlBetweenOperator) call.getOperator(), call);
      }
    }
  }
}
