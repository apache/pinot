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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.data.FieldSpec.DataType;


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

  private List<TransformFunction> _whenStatements = new ArrayList<>();
  private List<TransformFunction> _elseThenStatements = new ArrayList<>();
  private TransformResultMetadata _resultMetadata;
  private int[] _selectedResults;
  private int[] _intResults;
  private long[] _longResults;
  private float[] _floatResults;
  private double[] _doubleResults;
  private String[] _stringResults;
  private byte[][] _bytesResults;

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
    int numWhenStatements = arguments.size() / 2;
    _whenStatements = new ArrayList<>(numWhenStatements);
    for (int i = 0; i < numWhenStatements; i++) {
      _whenStatements.add(arguments.get(i));
    }
    // Add ELSE Statement first
    _elseThenStatements = new ArrayList<>(numWhenStatements + 1);
    _elseThenStatements.add(arguments.get(numWhenStatements * 2));
    for (int i = numWhenStatements; i < numWhenStatements * 2; i++) {
      _elseThenStatements.add(arguments.get(i));
    }
    _resultMetadata = calculateResultMetadata();
  }

  private TransformResultMetadata calculateResultMetadata() {
    TransformFunction elseStatement = _elseThenStatements.get(0);
    TransformResultMetadata elseStatementResultMetadata = elseStatement.getResultMetadata();
    DataType dataType = elseStatementResultMetadata.getDataType();
    Preconditions.checkState(elseStatementResultMetadata.isSingleValue(),
        "Unsupported multi-value expression in the ELSE clause");
    int numThenStatements = _elseThenStatements.size() - 1;
    for (int i = 0; i < numThenStatements; i++) {
      TransformFunction thenStatement = _elseThenStatements.get(i + 1);
      TransformResultMetadata thenStatementResultMetadata = thenStatement.getResultMetadata();
      Preconditions.checkState(thenStatementResultMetadata.isSingleValue(),
          String.format("Unsupported multi-value expression in the THEN clause of index: %d", i));
      DataType thenStatementDataType = thenStatementResultMetadata.getDataType();
      switch (dataType) {
        case INT:
          if (thenStatement instanceof LiteralTransformFunction) {
            dataType = LiteralTransformFunction.inferLiteralDataType((LiteralTransformFunction) thenStatement);
            break;
          }
          switch (thenStatementDataType) {
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case STRING:
              dataType = thenStatementDataType;
              break;
            default:
              throw new IllegalStateException(String
                  .format("Incompatible expression type: %s in the THEN clause of index: %d, main type: %s",
                      thenStatementDataType, i, dataType));
          }
          break;
        case LONG:
          if (thenStatement instanceof LiteralTransformFunction) {
            DataType literalDataType =
                LiteralTransformFunction.inferLiteralDataType((LiteralTransformFunction) thenStatement);
            switch (literalDataType) {
              case INT:
              case LONG:
                break;
              case FLOAT:
              case DOUBLE:
                dataType = DataType.DOUBLE;
                break;
              default:
                dataType = literalDataType;
            }
            break;
          }
          switch (thenStatementDataType) {
            case INT:
            case LONG:
              break;
            case FLOAT:
            case DOUBLE:
              dataType = DataType.DOUBLE;
              break;
            case STRING:
              dataType = DataType.STRING;
              break;
            default:
              throw new IllegalStateException(String
                  .format("Incompatible expression type: %s in the THEN clause of index: %d, main type: %s",
                      thenStatementDataType, i, dataType));
          }
          break;
        case FLOAT:
          if (thenStatement instanceof LiteralTransformFunction) {
            DataType literalDataType =
                LiteralTransformFunction.inferLiteralDataType((LiteralTransformFunction) thenStatement);
            switch (literalDataType) {
              case INT:
              case FLOAT:
                break;
              case LONG:
              case DOUBLE:
                dataType = DataType.DOUBLE;
                break;
              default:
                dataType = literalDataType;
            }
            break;
          }
          switch (thenStatementDataType) {
            case INT:
            case FLOAT:
              break;
            case LONG:
            case DOUBLE:
              dataType = DataType.DOUBLE;
              break;
            case STRING:
              dataType = DataType.STRING;
              break;
            default:
              throw new IllegalStateException(String
                  .format("Incompatible expression type: %s in the THEN clause of index: %d, main type: %s",
                      thenStatementDataType, i, dataType));
          }
          break;
        case DOUBLE:
          if (thenStatement instanceof LiteralTransformFunction) {
            DataType literalDataType =
                LiteralTransformFunction.inferLiteralDataType((LiteralTransformFunction) thenStatement);
            switch (literalDataType) {
              case INT:
              case LONG:
              case FLOAT:
              case DOUBLE:
                break;
              default:
                dataType = literalDataType;
            }
            break;
          }
          switch (thenStatementDataType) {
            case INT:
            case FLOAT:
            case LONG:
            case DOUBLE:
              break;
            case STRING:
              dataType = thenStatementDataType;
              break;
            default:
              throw new IllegalStateException(String
                  .format("Incompatible expression type: %s in the THEN clause of index: %d, main type: %s",
                      thenStatementDataType, i, dataType));
          }
          break;
        case STRING:
          if (thenStatement instanceof LiteralTransformFunction) {
            break;
          }
          switch (thenStatementDataType) {
            case INT:
            case FLOAT:
            case LONG:
            case DOUBLE:
            case STRING:
              break;
            default:
              throw new IllegalStateException(String
                  .format("Incompatible expression type: %s in the THEN clause of index: %d, main type: %s",
                      thenStatementDataType, i, dataType));
          }
          break;
        default:
          if (thenStatementDataType != dataType) {
            throw new IllegalStateException(String
                .format("Incompatible expression type: %s in the THEN clause of index: %d, main type: %s",
                    thenStatementDataType, i, dataType));
          }
      }
    }
    return new TransformResultMetadata(dataType, true, false);
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  /**
   * Evaluate the ProjectionBlock for the WHEN statements, returns an array with the
   * index(1 to N) of matched WHEN clause, 0 means nothing matched, so go to ELSE.
   */
  private int[] getSelectedArray(ProjectionBlock projectionBlock) {
    if (_selectedResults == null) {
      _selectedResults = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    } else {
      Arrays.fill(_selectedResults, 0);
    }
    int numWhenStatements = _whenStatements.size();
    for (int i = 0; i < numWhenStatements; i++) {
      TransformFunction whenStatement = _whenStatements.get(i);
      int[] conditions = whenStatement.transformToIntValuesSV(projectionBlock);
      for (int j = 0; j < conditions.length; j++) {
        if (_selectedResults[j] == 0 && conditions[j] == 1) {
          _selectedResults[j] = i + 1;
        }
      }
    }
    return _selectedResults;
  }

  @Override
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.INT) {
      return super.transformToIntValuesSV(projectionBlock);
    }
    int[] selected = getSelectedArray(projectionBlock);
    if (_intResults == null) {
      _intResults = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    int numElseThenStatements = _elseThenStatements.size();
    for (int i = 0; i < numElseThenStatements; i++) {
      TransformFunction transformFunction = _elseThenStatements.get(i);
      int[] intValues = transformFunction.transformToIntValuesSV(projectionBlock);
      int numDocs = projectionBlock.getNumDocs();
      for (int j = 0; j < numDocs; j++) {
        if (selected[j] == i) {
          _intResults[j] = intValues[j];
        }
      }
    }
    return _intResults;
  }

  @Override
  public long[] transformToLongValuesSV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.LONG) {
      return super.transformToLongValuesSV(projectionBlock);
    }
    int[] selected = getSelectedArray(projectionBlock);
    if (_longResults == null) {
      _longResults = new long[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    int numElseThenStatements = _elseThenStatements.size();
    for (int i = 0; i < numElseThenStatements; i++) {
      TransformFunction transformFunction = _elseThenStatements.get(i);
      long[] longValues = transformFunction.transformToLongValuesSV(projectionBlock);
      int numDocs = projectionBlock.getNumDocs();
      for (int j = 0; j < numDocs; j++) {
        if (selected[j] == i) {
          _longResults[j] = longValues[j];
        }
      }
    }
    return _longResults;
  }

  @Override
  public float[] transformToFloatValuesSV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.FLOAT) {
      return super.transformToFloatValuesSV(projectionBlock);
    }
    int[] selected = getSelectedArray(projectionBlock);
    if (_floatResults == null) {
      _floatResults = new float[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    int numElseThenStatements = _elseThenStatements.size();
    for (int i = 0; i < numElseThenStatements; i++) {
      TransformFunction transformFunction = _elseThenStatements.get(i);
      float[] floatValues = transformFunction.transformToFloatValuesSV(projectionBlock);
      int numDocs = projectionBlock.getNumDocs();
      for (int j = 0; j < numDocs; j++) {
        if (selected[j] == i) {
          _floatResults[j] = floatValues[j];
        }
      }
    }
    return _floatResults;
  }

  @Override
  public double[] transformToDoubleValuesSV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.DOUBLE) {
      return super.transformToDoubleValuesSV(projectionBlock);
    }
    int[] selected = getSelectedArray(projectionBlock);
    if (_doubleResults == null) {
      _doubleResults = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    int numElseThenStatements = _elseThenStatements.size();
    for (int i = 0; i < numElseThenStatements; i++) {
      TransformFunction transformFunction = _elseThenStatements.get(i);
      double[] doubleValues = transformFunction.transformToDoubleValuesSV(projectionBlock);
      int numDocs = projectionBlock.getNumDocs();
      for (int j = 0; j < numDocs; j++) {
        if (selected[j] == i) {
          _doubleResults[j] = doubleValues[j];
        }
      }
    }
    return _doubleResults;
  }

  @Override
  public String[] transformToStringValuesSV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.STRING) {
      return super.transformToStringValuesSV(projectionBlock);
    }
    int[] selected = getSelectedArray(projectionBlock);
    if (_stringResults == null) {
      _stringResults = new String[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    int numElseThenStatements = _elseThenStatements.size();
    for (int i = 0; i < numElseThenStatements; i++) {
      TransformFunction transformFunction = _elseThenStatements.get(i);
      String[] stringValues = transformFunction.transformToStringValuesSV(projectionBlock);
      int numDocs = projectionBlock.getNumDocs();
      for (int j = 0; j < numDocs; j++) {
        if (selected[j] == i) {
          _stringResults[j] = stringValues[j];
        }
      }
    }
    return _stringResults;
  }

  @Override
  public byte[][] transformToBytesValuesSV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.BYTES) {
      return super.transformToBytesValuesSV(projectionBlock);
    }
    int[] selected = getSelectedArray(projectionBlock);
    if (_bytesResults == null) {
      _bytesResults = new byte[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
    }
    int numElseThenStatements = _elseThenStatements.size();
    for (int i = 0; i < numElseThenStatements; i++) {
      TransformFunction transformFunction = _elseThenStatements.get(i);
      byte[][] bytesValues = transformFunction.transformToBytesValuesSV(projectionBlock);
      int numDocs = projectionBlock.getNumDocs();
      for (int j = 0; j < numDocs; j++) {
        if (selected[j] == i) {
          _bytesResults[j] = bytesValues[j];
        }
      }
    }
    return _bytesResults;
  }
}
