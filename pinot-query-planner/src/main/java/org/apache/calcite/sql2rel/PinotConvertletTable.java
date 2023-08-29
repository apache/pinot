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
package org.apache.calcite.sql2rel;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.checkerframework.checker.nullness.qual.Nullable;


/**
 * PinotConvertletTable is a wrapper of {@link StandardConvertletTable} with the customizations of not converting
 * certain SqlCalls, e.g. TIMESTAMPADD, TIMESTAMPDIFF.
 */
public class PinotConvertletTable implements SqlRexConvertletTable {

  public static final PinotConvertletTable INSTANCE = new PinotConvertletTable();

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
      final RexBuilder rexBuilder = cx.getRexBuilder();
      return rexBuilder.makeCall(SqlStdOperatorTable.TIMESTAMP_ADD,
          cx.convertExpression(call.operand(0)),
          cx.convertExpression(call.operand(1)),
          cx.convertExpression(call.operand(2)));
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
      final RexBuilder rexBuilder = cx.getRexBuilder();
      return rexBuilder.makeCall(SqlStdOperatorTable.TIMESTAMP_DIFF,
          cx.convertExpression(call.operand(0)),
          cx.convertExpression(call.operand(1)),
          cx.convertExpression(call.operand(2)));
    }
  }
}
