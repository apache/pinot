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
package org.apache.pinot.query.runtime.operator.operands;

import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.CalciteSchemaBuilder;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.pinot.common.function.sql.PinotCalciteCatalogReader;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.catalog.PinotCatalog;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.operator.utils.OperatorUtils;
import org.apache.pinot.query.type.TypeFactory;
import org.apache.pinot.query.type.TypeSystem;


public class TransformOperandFactory {
  private static final RelDataTypeFactory FUNCTION_CATALOGREL_DATA_TYPE_FACTORY = new TypeFactory(new TypeSystem());
  private static final CalciteSchema FUNCTION_CATALOG_ROOT_SCHEMA =
      CalciteSchemaBuilder.asRootSchema(new PinotCatalog(null));
  private static final CalciteConnectionConfig FUNCTION_CATALOG_CONFIG = CalciteConnectionConfig.DEFAULT;
  private static final PinotCalciteCatalogReader FUNCTION_CATALOG_OPERATOR_TABLE =
      new PinotCalciteCatalogReader(FUNCTION_CATALOG_ROOT_SCHEMA, FUNCTION_CATALOG_ROOT_SCHEMA.path(null),
          FUNCTION_CATALOGREL_DATA_TYPE_FACTORY, FUNCTION_CATALOG_CONFIG);

  private TransformOperandFactory() {
  }

  public static TransformOperand getTransformOperand(RexExpression rexExpression, DataSchema dataSchema) {
    if (rexExpression instanceof RexExpression.FunctionCall) {
      return getTransformOperand((RexExpression.FunctionCall) rexExpression, dataSchema);
    } else if (rexExpression instanceof RexExpression.InputRef) {
      return new ReferenceOperand(((RexExpression.InputRef) rexExpression).getIndex(), dataSchema);
    } else if (rexExpression instanceof RexExpression.Literal) {
      return new LiteralOperand((RexExpression.Literal) rexExpression);
    } else {
      throw new UnsupportedOperationException("Unsupported RexExpression: " + rexExpression);
    }
  }

  private static TransformOperand getTransformOperand(RexExpression.FunctionCall functionCall, DataSchema dataSchema) {
    List<RexExpression> operands = functionCall.getFunctionOperands();
    int numOperands = operands.size();
    String canonicalName = OperatorUtils.canonicalizeFunctionName(functionCall.getFunctionName());
    switch (canonicalName) {
      case "AND":
        Preconditions.checkState(numOperands >= 2, "AND takes >=2 arguments, got: %s", numOperands);
        return new FilterOperand.And(operands, dataSchema);
      case "OR":
        Preconditions.checkState(numOperands >= 2, "OR takes >=2 arguments, got: %s", numOperands);
        return new FilterOperand.Or(operands, dataSchema);
      case "NOT":
        Preconditions.checkState(numOperands == 1, "NOT takes one argument, got: %s", numOperands);
        return new FilterOperand.Not(operands.get(0), dataSchema);
      case "ISTRUE":
        Preconditions.checkState(numOperands == 1, "IS_TRUE takes one argument, got: %s", numOperands);
        return new FilterOperand.IsTrue(operands.get(0), dataSchema);
      case "ISNOTTRUE":
        Preconditions.checkState(numOperands == 1, "IS_NOT_TRUE takes one argument, got: %s", numOperands);
        return new FilterOperand.IsNotTrue(operands.get(0), dataSchema);
      case "equals":
        return new FilterOperand.Predicate(operands, dataSchema, v -> v == 0);
      case "notEquals":
        return new FilterOperand.Predicate(operands, dataSchema, v -> v != 0);
      case "greaterThan":
        return new FilterOperand.Predicate(operands, dataSchema, v -> v > 0);
      case "greaterThanOrEqual":
        return new FilterOperand.Predicate(operands, dataSchema, v -> v >= 0);
      case "lessThan":
        return new FilterOperand.Predicate(operands, dataSchema, v -> v < 0);
      case "lessThanOrEqual":
        return new FilterOperand.Predicate(operands, dataSchema, v -> v <= 0);
      default:
        return new FunctionOperand(FUNCTION_CATALOG_OPERATOR_TABLE, FUNCTION_CATALOGREL_DATA_TYPE_FACTORY,
            functionCall, canonicalName, dataSchema);
    }
  }
}
