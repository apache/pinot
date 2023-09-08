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
package org.apache.calcite.rex;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;


public class PinotRexBuilder extends RexBuilder {
  public PinotRexBuilder(RelDataTypeFactory typeFactory) {
    super(typeFactory);
  }

  /**
   * Ensures expression is interpreted as a specified type. This is an extension of the standard method, this method
   * does not cast expression if its {@link SqlTypeName} is float/real and the desired type is double.
   *
   * This makes queries like `select count(*) from table where aFloatColumn = 0.05` works correctly in multi-stage query
   * engine.
   * Without this change, `select count(*) from table where aFloatColumn = 0.05` will be converted to
   * `select count(*) from table where CAST(aFloatColumn as "DOUBLE") = 0.05`. While casting from float to double does
   * not always produce the same double value as the original float value, this leads to wrong query result.
   *
   * With this change, the behavior in multi-stage query engine will be the same as the query in v1 query engine.
   *
   * @param type             desired type
   * @param node             expression
   * @param matchNullability whether to correct nullability of specified
   *                         type to match the expression; this usually should
   *                         be true, except for explicit casts which can
   *                         override default nullability
   * @return a casted expression or the original expression
   */
  @Override
  public RexNode ensureType(
      RelDataType type,
      RexNode node,
      boolean matchNullability) {
    SqlTypeName typeToCastTo = type.getSqlTypeName();
    SqlTypeName typeToCastFrom = node.getType().getSqlTypeName();
    if (typeToCastTo == SqlTypeName.DOUBLE
        && (typeToCastFrom == SqlTypeName.REAL || typeToCastFrom == SqlTypeName.FLOAT)) {
      return node;
    }
    return super.ensureType(type, node, matchNullability);
  }
}
