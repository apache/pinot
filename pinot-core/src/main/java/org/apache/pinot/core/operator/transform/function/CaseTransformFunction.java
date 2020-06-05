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

import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.spi.data.FieldSpec;


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
    Preconditions.checkState(_elseThenStatements.size() == _numberWhenStatements + 1, "Missing THEN/ELSE clause in CASE statement");
    _resultMetadata = getResultMetadata(_elseThenStatements);
  }

  private TransformResultMetadata getResultMetadata(List<TransformFunction> elseThenStatements) {
    FieldSpec.DataType dataType = elseThenStatements.get(0).getResultMetadata().getDataType();
    for (int i = 1; i < elseThenStatements.size(); i++) {
      TransformResultMetadata resultMetadata = elseThenStatements.get(i).getResultMetadata();
      if (!resultMetadata.isSingleValue()) {
        throw new IllegalStateException(
            String.format("Incompatible expression types in THEN Clause [%s].", resultMetadata));
      }
      switch (dataType) {
        case INT:
          switch (resultMetadata.getDataType()) {
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case STRING:
              dataType = resultMetadata.getDataType();
              break;
            default:
              throw new IllegalStateException(
                  String.format("Incompatible expression types in THEN Clause [%s].", resultMetadata));
          }
          break;
        case LONG:
          switch (resultMetadata.getDataType()) {
            case INT:
            case LONG:
              break;
            case FLOAT:
            case DOUBLE:
              dataType = FieldSpec.DataType.DOUBLE;
              break;
            case STRING:
              dataType = FieldSpec.DataType.STRING;
              break;
            default:
              throw new IllegalStateException(
                  String.format("Incompatible expression types in THEN Clause [%s].", resultMetadata));
          }
          break;
        case FLOAT:
          switch (resultMetadata.getDataType()) {
            case INT:
            case FLOAT:
              break;
            case LONG:
            case DOUBLE:
              dataType = FieldSpec.DataType.DOUBLE;
              break;
            case STRING:
              dataType = resultMetadata.getDataType();
              break;
            default:
              throw new IllegalStateException(
                  String.format("Incompatible expression types in THEN Clause [%s].", resultMetadata));
          }
          break;
        case DOUBLE:
          switch (resultMetadata.getDataType()) {
            case INT:
            case FLOAT:
            case LONG:
            case DOUBLE:
              break;
            case STRING:
              dataType = resultMetadata.getDataType();
              break;
            default:
              throw new IllegalStateException(
                  String.format("Incompatible expression types in THEN Clause [%s].", resultMetadata));
          }
          break;
        case STRING:
          switch (resultMetadata.getDataType()) {
            case INT:
            case FLOAT:
            case LONG:
            case DOUBLE:
            case STRING:
              break;
            default:
              throw new IllegalStateException(
                  String.format("Incompatible expression types in THEN Clause [%s].", resultMetadata));
          }
          break;
        default:
          if (resultMetadata.getDataType() != dataType) {
            throw new IllegalStateException(
                String.format("Incompatible expression types in THEN Clause [%s].", resultMetadata));
          }
      }
    }
    return new TransformResultMetadata(dataType, true, false);
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
      TransformFunction transformFunction = _elseThenStatements.get(i);
      FieldSpec.DataType dataType = transformFunction.getResultMetadata().getDataType();
      switch (dataType) {
        case INT:
          int[] evalInts = transformFunction.transformToIntValuesSV(projectionBlock);
          for (int j = 0; j < selected.length; j++) {
            if (selected[j] == i) {
              results[j] = evalInts[j];
            }
          }
          break;
        case LONG:
          long[] evalLongs = transformFunction.transformToLongValuesSV(projectionBlock);
          for (int j = 0; j < selected.length; j++) {
            if (selected[j] == i) {
              results[j] = (int) evalLongs[j];
            }
          }
          break;
        case FLOAT:
          float[] evalFloats = transformFunction.transformToFloatValuesSV(projectionBlock);
          for (int j = 0; j < selected.length; j++) {
            if (selected[j] == i) {
              results[j] = (int) evalFloats[j];
            }
          }
          break;
        case DOUBLE:
          double[] evalDoubles = transformFunction.transformToDoubleValuesSV(projectionBlock);
          for (int j = 0; j < selected.length; j++) {
            if (selected[j] == i) {
              results[j] = (int) evalDoubles[j];
            }
          }
          break;
        case STRING:
          String[] evalStrings = transformFunction.transformToStringValuesSV(projectionBlock);
          for (int j = 0; j < selected.length; j++) {
            if (selected[j] == i) {
              results[j] = new BigDecimal(evalStrings[i]).intValue();
            }
          }
          break;
        default:
          throw new IllegalStateException(String
              .format("Cannot convert result type [%s] to [INT] for transform function [%s]", dataType,
                  transformFunction));
      }
    }
    return results;
  }

  @Override
  public long[] transformToLongValuesSV(ProjectionBlock projectionBlock) {
    int[] selected = getSelectedArray(projectionBlock);
    long[] results = new long[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    for (int i = 0; i < _elseThenStatements.size(); i++) {
      TransformFunction transformFunction = _elseThenStatements.get(i);
      FieldSpec.DataType dataType = transformFunction.getResultMetadata().getDataType();
      switch (dataType) {
        case INT:
          int[] evalInts = transformFunction.transformToIntValuesSV(projectionBlock);
          for (int j = 0; j < selected.length; j++) {
            if (selected[j] == i) {
              results[j] = evalInts[j];
            }
          }
          break;
        case LONG:
          long[] evalLongs = transformFunction.transformToLongValuesSV(projectionBlock);
          for (int j = 0; j < selected.length; j++) {
            if (selected[j] == i) {
              results[j] = evalLongs[j];
            }
          }
          break;
        case FLOAT:
          float[] evalFloats = transformFunction.transformToFloatValuesSV(projectionBlock);
          for (int j = 0; j < selected.length; j++) {
            if (selected[j] == i) {
              results[j] = (long) evalFloats[j];
            }
          }
          break;
        case DOUBLE:
          double[] evalDoubles = transformFunction.transformToDoubleValuesSV(projectionBlock);
          for (int j = 0; j < selected.length; j++) {
            if (selected[j] == i) {
              results[j] = (long) evalDoubles[j];
            }
          }
          break;
        case STRING:
          String[] evalStrings = transformFunction.transformToStringValuesSV(projectionBlock);
          for (int j = 0; j < selected.length; j++) {
            if (selected[j] == i) {
              results[j] = new BigDecimal(evalStrings[i]).longValue();
            }
          }
          break;
        default:
          throw new IllegalStateException(String
              .format("Cannot convert result type [%s] to [LONG] for transform function [%s]", dataType,
                  transformFunction));
      }
    }
    return results;
  }

  @Override
  public float[] transformToFloatValuesSV(ProjectionBlock projectionBlock) {
    int[] selected = getSelectedArray(projectionBlock);
    float[] results = new float[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    for (int i = 0; i < _elseThenStatements.size(); i++) {
      TransformFunction transformFunction = _elseThenStatements.get(i);
      FieldSpec.DataType dataType = transformFunction.getResultMetadata().getDataType();
      switch (dataType) {
        case INT:
          int[] evalInts = transformFunction.transformToIntValuesSV(projectionBlock);
          for (int j = 0; j < selected.length; j++) {
            if (selected[j] == i) {
              results[j] = evalInts[j];
            }
          }
          break;
        case LONG:
          long[] evalLongs = transformFunction.transformToLongValuesSV(projectionBlock);
          for (int j = 0; j < selected.length; j++) {
            if (selected[j] == i) {
              results[j] = evalLongs[j];
            }
          }
          break;
        case FLOAT:
          float[] evalFloats = transformFunction.transformToFloatValuesSV(projectionBlock);
          for (int j = 0; j < selected.length; j++) {
            if (selected[j] == i) {
              results[j] = evalFloats[j];
            }
          }
          break;
        case DOUBLE:
          double[] evalDoubles = transformFunction.transformToDoubleValuesSV(projectionBlock);
          for (int j = 0; j < selected.length; j++) {
            if (selected[j] == i) {
              results[j] = (float) evalDoubles[j];
            }
          }
          break;
        case STRING:
          String[] evalStrings = transformFunction.transformToStringValuesSV(projectionBlock);
          for (int j = 0; j < selected.length; j++) {
            if (selected[j] == i) {
              results[j] = new BigDecimal(evalStrings[i]).floatValue();
            }
          }
          break;
        default:
          throw new IllegalStateException(String
              .format("Cannot convert result type [%s] to [FLOAT] for transform function [%s]", dataType,
                  transformFunction));
      }
    }
    return results;
  }

  @Override
  public double[] transformToDoubleValuesSV(ProjectionBlock projectionBlock) {
    int[] selected = getSelectedArray(projectionBlock);
    double[] results = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    for (int i = 0; i < _elseThenStatements.size(); i++) {
      TransformFunction transformFunction = _elseThenStatements.get(i);
      FieldSpec.DataType dataType = transformFunction.getResultMetadata().getDataType();
      switch (dataType) {
        case INT:
          int[] evalInts = transformFunction.transformToIntValuesSV(projectionBlock);
          for (int j = 0; j < selected.length; j++) {
            if (selected[j] == i) {
              results[j] = evalInts[j];
            }
          }
          break;
        case LONG:
          long[] evalLongs = transformFunction.transformToLongValuesSV(projectionBlock);
          for (int j = 0; j < selected.length; j++) {
            if (selected[j] == i) {
              results[j] = evalLongs[j];
            }
          }
          break;
        case FLOAT:
          float[] evalFloats = transformFunction.transformToFloatValuesSV(projectionBlock);
          for (int j = 0; j < selected.length; j++) {
            if (selected[j] == i) {
              results[j] = evalFloats[j];
            }
          }
          break;
        case DOUBLE:
          double[] evalDoubles = transformFunction.transformToDoubleValuesSV(projectionBlock);
          for (int j = 0; j < selected.length; j++) {
            if (selected[j] == i) {
              results[j] = evalDoubles[j];
            }
          }
          break;
        case STRING:
          String[] evalStrings = transformFunction.transformToStringValuesSV(projectionBlock);
          for (int j = 0; j < selected.length; j++) {
            if (selected[j] == i) {
              results[j] = new BigDecimal(evalStrings[i]).doubleValue();
            }
          }
          break;
        default:
          throw new IllegalStateException(String
              .format("Cannot convert result type [%s] to [DOUBLE] for transform function [%s]", dataType,
                  transformFunction));
      }
    }
    return results;
  }

  @Override
  public String[] transformToStringValuesSV(ProjectionBlock projectionBlock) {
    int[] selected = getSelectedArray(projectionBlock);
    String[] results = new String[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    for (int i = 0; i < _elseThenStatements.size(); i++) {
      TransformFunction transformFunction = _elseThenStatements.get(i);
      FieldSpec.DataType dataType = transformFunction.getResultMetadata().getDataType();
      switch (dataType) {
        case INT:
          int[] evalInts = transformFunction.transformToIntValuesSV(projectionBlock);
          for (int j = 0; j < selected.length; j++) {
            if (selected[j] == i) {
              results[j] = Integer.toString(evalInts[j]);
            }
          }
          break;
        case LONG:
          long[] evalLongs = transformFunction.transformToLongValuesSV(projectionBlock);
          for (int j = 0; j < selected.length; j++) {
            if (selected[j] == i) {
              results[j] = Long.toString(evalLongs[j]);
            }
          }
          break;
        case FLOAT:
          float[] evalFloats = transformFunction.transformToFloatValuesSV(projectionBlock);
          for (int j = 0; j < selected.length; j++) {
            if (selected[j] == i) {
              results[j] = Float.toString(evalFloats[j]);
            }
          }
          break;
        case DOUBLE:
          double[] evalDoubles = transformFunction.transformToDoubleValuesSV(projectionBlock);
          for (int j = 0; j < selected.length; j++) {
            if (selected[j] == i) {
              results[j] = Double.toString(evalDoubles[j]);
            }
          }
          break;
        case STRING:
          String[] evalStrings = transformFunction.transformToStringValuesSV(projectionBlock);
          for (int j = 0; j < selected.length; j++) {
            if (selected[j] == i) {
              results[j] = evalStrings[i];
            }
          }
          break;
        default:
          throw new IllegalStateException(String
              .format("Cannot convert result type [%s] to [LONG] for transform function [%s]", dataType,
                  transformFunction));
      }
    }
    return results;
  }
}
