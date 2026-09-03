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
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.VariantUtils;
import org.apache.pinot.query.planner.logical.RexExpression;


/**
 * Query-local constant operand for parsing a JSON literal into Variant.
 *
 * <p>The literal is parsed exactly once while the expression tree is constructed. The cached internal value is
 * immutable by convention and can therefore be reused for every input row. Instances are thread-safe after
 * construction.
 */
final class LiteralParseJsonOperand implements TransformOperand {
  private static final String PARSE_JSON = "parsejson";
  private static final String PARSE_JSON_TO_VARIANT = "parsejsontovariant";
  private static final String TRY_PARSE_JSON = "tryparsejson";
  private static final String TRY_PARSE_JSON_TO_VARIANT = "tryparsejsontovariant";

  private final ColumnDataType _resultType;
  @Nullable
  private final Object _value;

  LiteralParseJsonOperand(RexExpression.FunctionCall functionCall, String canonicalName) {
    Preconditions.checkArgument(isSupported(canonicalName), "Unsupported JSON-to-Variant function: %s",
        functionCall.getFunctionName());
    List<RexExpression> operands = functionCall.getFunctionOperands();
    Preconditions.checkArgument(operands.size() == 1 && operands.get(0) instanceof RexExpression.Literal,
        "%s expects one literal argument", functionCall.getFunctionName());

    RexExpression.Literal literal = (RexExpression.Literal) operands.get(0);
    Object literalValue = literal.getValue();
    ColumnDataType literalType = literal.getDataType();
    boolean validLiteral = literalValue == null
        || ((literalType == ColumnDataType.STRING || literalType == ColumnDataType.JSON)
        && literalValue instanceof String);
    Preconditions.checkArgument(validLiteral,
        "%s argument must be a STRING or JSON literal", functionCall.getFunctionName());

    _resultType = functionCall.getDataType();
    String json = (String) literalValue;
    byte[] variant = isTolerant(canonicalName)
        ? VariantUtils.tryParseJsonToVariant(json) : VariantUtils.parseJsonToVariant(json);
    _value = variant != null ? _resultType.toInternal(variant) : null;
  }

  static boolean isSupported(String canonicalName) {
    switch (canonicalName) {
      case PARSE_JSON:
      case PARSE_JSON_TO_VARIANT:
      case TRY_PARSE_JSON:
      case TRY_PARSE_JSON_TO_VARIANT:
        return true;
      default:
        return false;
    }
  }

  private static boolean isTolerant(String canonicalName) {
    return canonicalName.equals(TRY_PARSE_JSON) || canonicalName.equals(TRY_PARSE_JSON_TO_VARIANT);
  }

  @Override
  public ColumnDataType getResultType() {
    return _resultType;
  }

  @Nullable
  @Override
  public Object apply(List<Object> row) {
    return _value;
  }
}
