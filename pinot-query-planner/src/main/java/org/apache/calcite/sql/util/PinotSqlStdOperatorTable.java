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
package org.apache.calcite.sql.util;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.PinotSqlCoalesceFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.util.Util;
import org.apache.pinot.common.function.FunctionRegistry;


public class PinotSqlStdOperatorTable extends SqlStdOperatorTable {
  private static PinotSqlStdOperatorTable _instance;

  // supplier instance. this should replace all lazy init static objects in the codebase
  public static synchronized PinotSqlStdOperatorTable instance() {
    if (_instance == null) {
      // Creates and initializes the standard operator table.
      // Uses two-phase construction, because we can't initialize the
      // table until the constructor of the sub-class has completed.
      _instance = new PinotSqlStdOperatorTable();
      _instance.initNoDuplicate();
    }
    return _instance;
  }

  /**
   * Initialize without duplicate, e.g. when 2 duplicate operator is linked with the same op
   * {@link org.apache.calcite.sql.SqlKind} it causes problem.
   *
   * <p>This is a direct copy of the {@link org.apache.calcite.sql.util.ReflectiveSqlOperatorTable} and can be hard to
   * debug, suggest changing to a non-dynamic registration. Dynamic function support should happen via catalog.
   *
   * This also registers aggregation functions defined in {@link org.apache.pinot.segment.spi.AggregationFunctionType}
   * which are multistage enabled.
   */
  public final void initNoDuplicate() {
    // Pinot supports native COALESCE function, thus no need to create CASE WHEN conversion.
    register(new PinotSqlCoalesceFunction());
    // Ensure ArrayValueConstructor is registered before ArrayQueryConstructor
    register(ARRAY_VALUE_CONSTRUCTOR);

    // TODO: reflection based registration is not ideal, we should use a static list of operators and register them
    // Use reflection to register the expressions stored in public fields.
    for (Field field : getClass().getFields()) {
      try {
        if (SqlFunction.class.isAssignableFrom(field.getType())) {
          SqlFunction op = (SqlFunction) field.get(this);
          if (op != null && notRegistered(op)) {
            register(op);
          }
        } else if (SqlOperator.class.isAssignableFrom(field.getType())) {
          SqlOperator op = (SqlOperator) field.get(this);
          if (op != null && notRegistered(op)) {
            register(op);
          }
        }
      } catch (IllegalArgumentException | IllegalAccessException e) {
        throw Util.throwAsRuntime(Util.causeOrSelf(e));
      }
    }
  }

  private boolean notRegistered(SqlFunction op) {
    List<SqlOperator> operatorList = new ArrayList<>();
    lookupOperatorOverloads(op.getNameAsId(), op.getFunctionType(), op.getSyntax(), operatorList,
        SqlNameMatchers.withCaseSensitive(FunctionRegistry.CASE_SENSITIVITY));
    return operatorList.size() == 0;
  }

  private boolean notRegistered(SqlOperator op) {
    List<SqlOperator> operatorList = new ArrayList<>();
    lookupOperatorOverloads(op.getNameAsId(), null, op.getSyntax(), operatorList,
        SqlNameMatchers.withCaseSensitive(FunctionRegistry.CASE_SENSITIVITY));
    return operatorList.size() == 0;
  }
}
