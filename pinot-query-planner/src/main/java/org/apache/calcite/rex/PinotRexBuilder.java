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
   * An extension of the standard method that does not cast {@link SqlTypeName} float/real to double.
   *
   * This makes queries like `select count(*) from table where aFloatColumn = 0.05` works correctly in multi-stage query
   * engine.
   * Without this change, `select count(*) from table where aFloatColumn = 0.05` will be converted to
   * `select count(*) from table where CAST(aFloatColumn as "DOUBLE") = 0.05`. While casting from float to double does
   * not always produce the same double value as the original float value, this leads to wrong query result.
   *
   * With this change, the behavior in multi-stage query engine will be the same as the query in v1 query engine.
   *
   * @param type The {@link RelDataType} to cast to
   * @param exp The {@link RexNode} to cast
   * @return A new {@link RexNode} that casts the given expression to the given type
   */
  @Override
  public RexNode makeAbstractCast(RelDataType type, RexNode exp) {
    SqlTypeName typeToCastTo = type.getSqlTypeName();
    SqlTypeName typeToCastFrom = exp.getType().getSqlTypeName();
    if (typeToCastTo == SqlTypeName.DOUBLE
        && (typeToCastFrom == SqlTypeName.REAL || typeToCastFrom == SqlTypeName.FLOAT)) {
      return exp;
    }
    return super.makeAbstractCast(type, exp);
  }
}
