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
package org.apache.pinot.core.operator.transform.function;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.plan.DocIdSetPlanNode;


/**
 * The <code>CaseTransformFunction</code> class implements the CASE-WHEN-THEN-ELSE transformation.
 *
 * The SQL Syntax is:
 *    CASE
 *        WHEN condition1 THEN result1
 *        WHEN condition2 THEN result2
 *        WHEN conditionN THEN resultN
 *        ELSE result
 *    END;
 *
 * Usage:
 *    case(${WHEN_STATEMENT_1}, ..., ${WHEN_STATEMENT_N},
 *         ${THEN_EXPRESSION_1}, ..., ${THEN_EXPRESSION_N},
 *         ${ELSE_EXPRESSION})
 *
 * There are 2 * N + 1 arguments:
 *    <code>WHEN_STATEMENT_$i</code> is a <code>BinaryOperatorTransformFunction</code> represents <code>condition$i</code>
 *    <code>THEN_EXPRESSION_$i</code> is a <code>TransformFunction</code> represents <code>result$i</code>
 *    <code>ELSE_EXPRESSION</code> is a <code>TransformFunction</code> represents <code>result</code>
 *
 */
public class CaseTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "case";
  private final List<TransformFunction> _whenStatements = new ArrayList<>();
  private final List<TransformFunction> _elseThenStatements = new ArrayList<>();
  private int _numberWhenStatements;
  private TransformResultMetadata _resultMetadata;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    // Check that there are more than 1 arguments
    if (arguments.size() % 2 != 1 || arguments.size() < 3) {
      throw new IllegalArgumentException("At least 3 odd number of arguments are required for CASE-WHEN-ELSE function");
    }
    _numberWhenStatements = arguments.size() / 2;
    for (int i = 0; i < _numberWhenStatements; i++) {
      _whenStatements.add(arguments.get(i));
    }
    // Add ELSE Statement first
    _elseThenStatements.add(arguments.get(_numberWhenStatements * 2));
    for (int i = _numberWhenStatements; i < _numberWhenStatements * 2; i++) {
      _elseThenStatements.add(arguments.get(i));
    }
    _resultMetadata = _elseThenStatements.get(0).getResultMetadata();
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  private int[] getSelectedArray(ProjectionBlock projectionBlock) {
    int[] selected = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    for (int i = 0; i < _numberWhenStatements; i++) {
      TransformFunction transformFunction = _whenStatements.get(i);
      int[] conditions = transformFunction.transformToIntValuesSV(projectionBlock);
      for (int j = 0; j < conditions.length; j++) {
        if (selected[j] == 0 && conditions[j] == 1) {
          selected[j] = i + 1;
        }
      }
    }
    return selected;
  }

  @Override
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {
    int[] selected = getSelectedArray(projectionBlock);
    int[] results = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    for (int i = 0; i < _elseThenStatements.size(); i++) {
      int[] eval = _elseThenStatements.get(i).transformToIntValuesSV(projectionBlock);
      for (int j = 0; j < selected.length; j++) {
        if (selected[j] == i) {
          results[j] = eval[j];
        }
      }
    }
    return results;
  }

  @Override
  public long[] transformToLongValuesSV(ProjectionBlock projectionBlock) {
    int[] selected = getSelectedArray(projectionBlock);
    long[] results = new long[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    for (int i = 0; i < _elseThenStatements.size(); i++) {
      long[] eval = _elseThenStatements.get(i).transformToLongValuesSV(projectionBlock);
      for (int j = 0; j < selected.length; j++) {
        if (selected[j] == i) {
          results[j] = eval[j];
        }
      }
    }
    return results;
  }

  @Override
  public float[] transformToFloatValuesSV(ProjectionBlock projectionBlock) {
    int[] selected = getSelectedArray(projectionBlock);
    float[] results = new float[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    for (int i = 0; i < _elseThenStatements.size(); i++) {
      float[] eval = _elseThenStatements.get(i).transformToFloatValuesSV(projectionBlock);
      for (int j = 0; j < selected.length; j++) {
        if (selected[j] == i) {
          results[j] = eval[j];
        }
      }
    }
    return results;
  }

  @Override
  public double[] transformToDoubleValuesSV(ProjectionBlock projectionBlock) {
    int[] selected = getSelectedArray(projectionBlock);
    double[] results = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    for (int i = 0; i < _elseThenStatements.size(); i++) {
      double[] eval = _elseThenStatements.get(i).transformToDoubleValuesSV(projectionBlock);
      for (int j = 0; j < selected.length; j++) {
        if (selected[j] == i) {
          results[j] = eval[j];
        }
      }
    }
    return results;
  }

  @Override
  public String[] transformToStringValuesSV(ProjectionBlock projectionBlock) {
    int[] selected = getSelectedArray(projectionBlock);
    String[] results = new String[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    for (int i = 0; i < _elseThenStatements.size(); i++) {
      String[] eval = _elseThenStatements.get(i).transformToStringValuesSV(projectionBlock);
      for (int j = 0; j < selected.length; j++) {
        if (selected[j] == i) {
          results[j] = eval[j];
        }
      }
    }
    return results;
  }
}
