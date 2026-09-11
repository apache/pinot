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
package org.apache.pinot.core.query.aggregation;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.segment.spi.AggregationFunctionType;


/// Restores bindings through the gapfill query wrappers using the server query's actual result schema. The request
/// must be private to the broker reduction thread while this utility attaches metadata.
public final class GapfillAggregationFunctionBinder {
  private GapfillAggregationFunctionBinder() {
  }

  public static void bind(PinotQuery query, PinotQuery serverQuery, @Nullable DataSchema resultSchema) {
    List<PinotQuery> levels = new ArrayList<>();
    PinotQuery current = query;
    boolean gapfill = false;
    while (current != null) {
      levels.add(current);
      for (Expression expression : current.getSelectList()) {
        Expression selection = stripAlias(expression);
        gapfill |= isGapfill(selection);
      }
      current = current.getDataSource() != null ? current.getDataSource().getSubquery() : null;
    }
    if (!gapfill || levels.stream().flatMap(level -> collectAggregations(level).stream())
        .noneMatch(GapfillAggregationFunctionBinder::requiresBinding)) {
      return;
    }
    PinotQuery inner = levels.get(levels.size() - 1);
    copyServerBindings(inner, serverQuery);
    if (resultSchema == null || levels.size() == 1) {
      return;
    }

    Preconditions.checkArgument(inner.getSelectListSize() == resultSchema.size(),
        "Gapfill input columns do not match the server result schema");
    String[] names = new String[resultSchema.size()];
    for (int i = 0; i < names.length; i++) {
      names[i] = outputName(inner.getSelectList().get(i));
    }
    DataSchema inputSchema = new DataSchema(names, resultSchema.getColumnDataTypes());
    for (int level = levels.size() - 2; level >= 0; level--) {
      PinotQuery outer = levels.get(level);
      AggregationFunctionBinder.bind(outer, inputSchema);
      if (level > 0) {
        // The intervening gapfill SELECT only projects inner columns and its time bucket. Its output aliases,
        // rather than the base table's field names, define the outer aggregate's input types.
        ExpressionTypeResolver resolver = new ExpressionTypeResolver(inputSchema);
        List<Expression> selections = outer.getSelectList();
        names = new String[selections.size()];
        ColumnDataType[] types = new ColumnDataType[selections.size()];
        for (int i = 0; i < selections.size(); i++) {
          names[i] = outputName(selections.get(i));
          types[i] = resolver.resolve(RequestContextUtils.getExpression(stripGapfill(stripAlias(selections.get(i)))));
        }
        inputSchema = new DataSchema(names, types);
      }
    }
  }

  private static void copyServerBindings(PinotQuery inner, PinotQuery serverQuery) {
    List<Function> executed = collectAggregations(serverQuery);
    if (executed.stream().noneMatch(Function::isSetAggregationBinding)) {
      return;
    }
    List<Function> original = collectAggregations(inner);
    if (original.stream().filter(GapfillAggregationFunctionBinder::requiresBinding)
        .allMatch(Function::isSetAggregationBinding)) {
      return;
    }
    Preconditions.checkArgument(original.size() == executed.size(),
        "Gapfill aggregation layout changed during planning");
    for (int i = 0; i < original.size(); i++) {
      Function source = executed.get(i);
      if (source.isSetAggregationBinding()) {
        Function target = original.get(i);
        if (target.isSetAggregationBinding()) {
          continue;
        }
        Preconditions.checkArgument(AggregationFunctionType.getAggregationFunctionType(target.getOperator())
                == AggregationFunctionType.getAggregationFunctionType(source.getOperator()),
            "Gapfill aggregation order changed during planning");
        target.setAggregationBinding(source.getAggregationBinding().deepCopy());
      }
    }
  }

  private static boolean requiresBinding(Function function) {
    return AggregationFunctionType.getAggregationFunctionType(function.getOperator())
        .isTypeBindingRequired(function.getOperandsSize(), index -> {
          Expression operand = function.getOperands().get(index);
          return operand.isSetLiteral() && operand.getLiteral().isSetStringValue();
        });
  }

  private static List<Function> collectAggregations(PinotQuery query) {
    List<Function> functions = new ArrayList<>();
    query.getSelectList().forEach(expression -> collect(expression, functions));
    collect(query.getHavingExpression(), functions);
    if (query.getOrderByList() != null) {
      query.getOrderByList().forEach(expression -> collect(expression, functions));
    }
    return functions;
  }

  private static void collect(@Nullable Expression expression, List<Function> functions) {
    if (expression == null || !expression.isSetFunctionCall()) {
      return;
    }
    Function function = expression.getFunctionCall();
    if (AggregationFunctionType.isAggregationFunction(function.getOperator())) {
      functions.add(function);
    }
    function.getOperands().forEach(operand -> collect(operand, functions));
  }

  private static Expression stripAlias(Expression expression) {
    return expression.isSetFunctionCall() && expression.getFunctionCall().getOperator().equalsIgnoreCase("as")
        ? expression.getFunctionCall().getOperands().get(0)
        : expression;
  }

  private static Expression stripGapfill(Expression expression) {
    return isGapfill(expression) ? expression.getFunctionCall().getOperands().get(0) : expression;
  }

  private static boolean isGapfill(Expression expression) {
    return expression.isSetFunctionCall() && expression.getFunctionCall().getOperator().equalsIgnoreCase("gapfill");
  }

  private static String outputName(Expression expression) {
    Expression selection = stripAlias(expression);
    return selection != expression
        ? expression.getFunctionCall().getOperands().get(1).getIdentifier().getName()
        : RequestContextUtils.getExpression(stripGapfill(selection)).toString();
  }
}
