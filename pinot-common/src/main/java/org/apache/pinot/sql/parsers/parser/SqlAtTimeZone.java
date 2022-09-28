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
package org.apache.pinot.sql.parsers.parser;

import java.util.Arrays;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

import static java.util.Objects.requireNonNull;


public class SqlAtTimeZone extends SqlSpecialOperator {

  public static final SqlAtTimeZone INSTANCE = new SqlAtTimeZone();

  private SqlAtTimeZone() {
    super(
        "AT_TIME_ZONE",
        SqlKind.OTHER_FUNCTION,
        32, // put this at the same precedence as LIKE (see SqlLikeOperator)
        false,
        opBinding -> opBinding.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP),
        null,
        OperandTypes.family(SqlTypeFamily.TIME)
    );
  }

  @Override
  public ReduceResult reduceExpr(int ordinal, TokenSequence list) {
    SqlNode left = list.node(ordinal - 1);
    SqlNode right = list.node(ordinal + 1);
    return new ReduceResult(ordinal - 1,
        ordinal + 2,
        createCall(
            SqlParserPos.sum(
                Arrays.asList(requireNonNull(left, "left").getParserPosition(),
                    requireNonNull(right, "right").getParserPosition(),
                    list.pos(ordinal))),
            left,
            right));
  }
}
