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
package org.apache.pinot.plugin.vector;

import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.pinot.common.filter.FilterPredicatePlugin;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.sql.parsers.SqlCompilationException;


/**
 * Plugin that registers the VECTOR_SIMILARITY filter predicate into Pinot.
 *
 * <p>This plugin handles three layers:
 * <ol>
 *   <li><b>SQL Parsing</b> -- validation of the Thrift expression tree</li>
 *   <li><b>Calcite Registration</b> -- operand type metadata for the SQL operator</li>
 *   <li><b>Predicate Creation</b> -- converting parsed operands into a {@link VectorSimilarityPredicate}</li>
 * </ol>
 */
public class VectorSimilarityPlugin implements FilterPredicatePlugin {

  @Override
  public String name() {
    return "VECTOR_SIMILARITY";
  }

  @Override
  public void validateFilterExpression(List<Expression> operands) {
    Expression vectorIdentifier = operands.get(0);
    if (!vectorIdentifier.isSetIdentifier()) {
      throw new IllegalStateException(
          "The first argument of VECTOR_SIMILARITY must be an identifier of float array");
    }
    Expression vectorLiteral = operands.get(1);
    if ((vectorLiteral.isSetFunctionCall()
        && !vectorLiteral.getFunctionCall().getOperator().equalsIgnoreCase("arrayvalueconstructor"))
        || (vectorLiteral.isSetLiteral() && !vectorLiteral.getLiteral().isSetFloatArrayValue()
            && !vectorLiteral.getLiteral().isSetDoubleArrayValue())) {
      throw new IllegalStateException(
          "The second argument of VECTOR_SIMILARITY must be a float/double array literal");
    }
    if (operands.size() == 3) {
      Expression topK = operands.get(2);
      if (!topK.isSetLiteral()) {
        throw new IllegalStateException(
            "The third argument of VECTOR_SIMILARITY must be an integer literal");
      }
    }
  }

  @Override
  public void rewriteExpression(List<Expression> operands) {
    Preconditions.checkArgument(operands.size() >= 2 && operands.size() <= 3,
        "For VECTOR_SIMILARITY predicate, the number of operands must be 2 or 3, got: %s", operands.size());
    if ((operands.get(1).getFunctionCall() != null
        && !operands.get(1).getFunctionCall().getOperator().equalsIgnoreCase("arrayvalueconstructor"))
        || (operands.get(1).getLiteral() != null && !operands.get(1).getLiteral().isSetFloatArrayValue()
            && !operands.get(1).getLiteral().isSetDoubleArrayValue())) {
      throw new SqlCompilationException(
          "For VECTOR_SIMILARITY predicate, the second operand must be a float/double array literal");
    }
    if (operands.size() == 3 && operands.get(2).getLiteral() == null) {
      throw new SqlCompilationException(
          "For VECTOR_SIMILARITY predicate, the third operand must be a literal");
    }
  }

  @Override
  public List<OperandType> getOperandTypes() {
    return List.of(OperandType.ARRAY, OperandType.ARRAY, OperandType.INTEGER);
  }

  @Override
  public List<Integer> getOptionalOperandIndices() {
    return List.of(2);
  }

  @Override
  public Predicate createPredicate(List<ExpressionContext> operands) {
    int topK = VectorSimilarityPredicate.DEFAULT_TOP_K;
    if (operands.size() == 3) {
      topK = (int) operands.get(2).getLiteral().getLongValue();
    }
    return new VectorSimilarityPredicate(operands.get(0), getVectorValue(operands.get(1)), topK);
  }

  private static float[] getVectorValue(ExpressionContext expressionContext) {
    // Handle literal float/double arrays (e.g., from Thrift deserialization)
    if (expressionContext.getType() == ExpressionContext.Type.LITERAL) {
      Object value = expressionContext.getLiteral().getValue();
      if (value instanceof float[]) {
        return (float[]) value;
      }
      if (value instanceof double[]) {
        double[] doubles = (double[]) value;
        float[] floats = new float[doubles.length];
        for (int i = 0; i < doubles.length; i++) {
          floats[i] = (float) doubles[i];
        }
        return floats;
      }
      throw new IllegalArgumentException(
          "The second argument of VECTOR_SIMILARITY must be a float/double array literal, got: " + value.getClass());
    }
    // Handle ARRAYVALUECONSTRUCTOR function (e.g., from SQL parser)
    if (expressionContext.getType() == ExpressionContext.Type.FUNCTION) {
      List<ExpressionContext> arguments = expressionContext.getFunction().getArguments();
      float[] vector = new float[arguments.size()];
      for (int i = 0; i < arguments.size(); i++) {
        vector[i] = Float.parseFloat(arguments.get(i).getLiteral().getValue().toString());
      }
      return vector;
    }
    throw new IllegalArgumentException(
        "The second argument of VECTOR_SIMILARITY must be a float array literal or ARRAYVALUECONSTRUCTOR");
  }
}
