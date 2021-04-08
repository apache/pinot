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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.util.ArrayCopyUtils;
import org.apache.pinot.segment.spi.datasource.DataSource;
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
  private int[] _selectedResults;
  private int[] _intResults;
  private long[] _longResults;
  private float[] _floatResults;
  private double[] _doubleResults;
  private String[] _stringResults;

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
    getResultMetadata();
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    if (_resultMetadata != null) {
      return _resultMetadata;
    }
    FieldSpec.DataType dataType = _elseThenStatements.get(0).getResultMetadata().getDataType();
    boolean isSingleValueField = _elseThenStatements.get(0).getResultMetadata().isSingleValue();
    for (int i = 1; i < _elseThenStatements.size(); i++) {
      TransformFunction transformFunction = _elseThenStatements.get(i);
      TransformResultMetadata resultMetadata = transformFunction.getResultMetadata();
      if (resultMetadata.isSingleValue() != isSingleValueField) {
        throw new IllegalStateException(
            String.format("Mixed Single/Multi Value results in expression types in THEN Clause [%s].", resultMetadata));
      }
      switch (dataType) {
        case INT:
          if (transformFunction instanceof LiteralTransformFunction) {
            dataType = LiteralTransformFunction.inferLiteralDataType((LiteralTransformFunction) transformFunction);
            break;
          }
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
          if (transformFunction instanceof LiteralTransformFunction) {
            FieldSpec.DataType literalDataType =
                LiteralTransformFunction.inferLiteralDataType((LiteralTransformFunction) transformFunction);
            switch (literalDataType) {
              case INT:
              case LONG:
                break;
              case FLOAT:
              case DOUBLE:
                dataType = FieldSpec.DataType.DOUBLE;
                break;
              default:
                dataType = FieldSpec.DataType.STRING;
                break;
            }
            break;
          }
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
          if (transformFunction instanceof LiteralTransformFunction) {
            FieldSpec.DataType literalDataType =
                LiteralTransformFunction.inferLiteralDataType((LiteralTransformFunction) transformFunction);
            switch (literalDataType) {
              case INT:
              case FLOAT:
                break;
              case LONG:
              case DOUBLE:
                dataType = FieldSpec.DataType.DOUBLE;
                break;
              case STRING:
                dataType = FieldSpec.DataType.STRING;
                break;
              default:
                dataType = literalDataType;
            }
            break;
          }
          switch (resultMetadata.getDataType()) {
            case INT:
            case FLOAT:
              break;
            case LONG:
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
        case DOUBLE:
          if (transformFunction instanceof LiteralTransformFunction) {
            FieldSpec.DataType literalDataType =
                LiteralTransformFunction.inferLiteralDataType((LiteralTransformFunction) transformFunction);
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
          if (transformFunction instanceof LiteralTransformFunction) {
            break;
          }
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
    _resultMetadata = new TransformResultMetadata(dataType, true, false);
    return _resultMetadata;
  }

  /**
   * Evaluate the ProjectionBlock for the WHEN statements, returns an array with the
   * index(1 to N) of matched WHEN clause, 0 means nothing matched, so go to ELSE.
   *
   * @param projectionBlock
   * @return
   */
  private int[] getSelectedArray(ProjectionBlock projectionBlock) {
    if (_selectedResults == null) {
      _selectedResults = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    } else {
      Arrays.fill(_selectedResults, 0);
    }
    for (int i = 0; i < _numberWhenStatements; i++) {
      TransformFunction transformFunction = _whenStatements.get(i);
      int[] conditions = transformFunction.transformToIntValuesSV(projectionBlock);
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
    if (_resultMetadata.getDataType() != FieldSpec.DataType.INT) {
      return super.transformToIntValuesSV(projectionBlock);
    }
    int[] selected = getSelectedArray(projectionBlock);
    if (_intResults == null) {
      _intResults = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    for (int i = 0; i < _elseThenStatements.size(); i++) {
      TransformFunction transformFunction = _elseThenStatements.get(i);
      int blockNumDocs = projectionBlock.getNumDocs();
      int[] evalInts = transformFunction.transformToIntValuesSV(projectionBlock);
      for (int j = 0; j < blockNumDocs; j++) {
        if (selected[j] == i) {
          _intResults[j] = evalInts[j];
        }
      }
    }
    return _intResults;
  }

  @Override
  public long[] transformToLongValuesSV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType() != FieldSpec.DataType.LONG) {
      return super.transformToLongValuesSV(projectionBlock);
    }
    int[] selected = getSelectedArray(projectionBlock);
    if (_longResults == null) {
      _longResults = new long[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    for (int i = 0; i < _elseThenStatements.size(); i++) {
      TransformFunction transformFunction = _elseThenStatements.get(i);
      FieldSpec.DataType dataType = transformFunction.getResultMetadata().getDataType();
      int blockNumDocs = projectionBlock.getNumDocs();
      long[] evalLongs;
      if (dataType == FieldSpec.DataType.LONG) {
        evalLongs = transformFunction.transformToLongValuesSV(projectionBlock);
      } else {
        evalLongs = new long[blockNumDocs];
        // dataType can only be INT
        ArrayCopyUtils.copy(transformFunction.transformToIntValuesSV(projectionBlock), evalLongs, blockNumDocs);
      }
      for (int j = 0; j < blockNumDocs; j++) {
        if (selected[j] == i) {
          _longResults[j] = evalLongs[j];
        }
      }
    }
    return _longResults;
  }

  @Override
  public float[] transformToFloatValuesSV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType() != FieldSpec.DataType.FLOAT) {
      return super.transformToFloatValuesSV(projectionBlock);
    }
    int[] selected = getSelectedArray(projectionBlock);
    if (_floatResults == null) {
      _floatResults = new float[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    for (int i = 0; i < _elseThenStatements.size(); i++) {
      TransformFunction transformFunction = _elseThenStatements.get(i);
      FieldSpec.DataType dataType = transformFunction.getResultMetadata().getDataType();
      int blockNumDocs = projectionBlock.getNumDocs();
      float[] evalFloats;
      if (dataType == FieldSpec.DataType.FLOAT) {
        evalFloats = transformFunction.transformToFloatValuesSV(projectionBlock);
      } else {
        evalFloats = new float[blockNumDocs];
        // dataType can only be INT
        ArrayCopyUtils.copy(transformFunction.transformToIntValuesSV(projectionBlock), evalFloats, blockNumDocs);
      }
      for (int j = 0; j < blockNumDocs; j++) {
        if (selected[j] == i) {
          _floatResults[j] = evalFloats[j];
        }
      }
    }
    return _floatResults;
  }

  @Override
  public double[] transformToDoubleValuesSV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType() != FieldSpec.DataType.DOUBLE) {
      return super.transformToDoubleValuesSV(projectionBlock);
    }
    int[] selected = getSelectedArray(projectionBlock);
    if (_doubleResults == null) {
      _doubleResults = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    for (int i = 0; i < _elseThenStatements.size(); i++) {
      TransformFunction transformFunction = _elseThenStatements.get(i);
      FieldSpec.DataType dataType = transformFunction.getResultMetadata().getDataType();
      int blockNumDocs = projectionBlock.getNumDocs();
      double[] evalDoubles;
      if (dataType == FieldSpec.DataType.DOUBLE) {
        evalDoubles = transformFunction.transformToDoubleValuesSV(projectionBlock);
      } else {
        evalDoubles = new double[blockNumDocs];
        switch (dataType) {
          case INT:
            ArrayCopyUtils.copy(transformFunction.transformToIntValuesSV(projectionBlock), evalDoubles, blockNumDocs);
            break;
          case LONG:
            ArrayCopyUtils.copy(transformFunction.transformToLongValuesSV(projectionBlock), evalDoubles, blockNumDocs);
            break;
          case FLOAT:
            ArrayCopyUtils.copy(transformFunction.transformToFloatValuesSV(projectionBlock), evalDoubles, blockNumDocs);
            break;
          default:
            throw new IllegalStateException(String
                .format("Cannot convert result type [%s] to [DOUBLE] for transform function [%s]", dataType,
                    transformFunction));
        }
      }
      for (int j = 0; j < blockNumDocs; j++) {
        if (selected[j] == i) {
          _doubleResults[j] = evalDoubles[j];
        }
      }
    }
    return _doubleResults;
  }

  @Override
  public String[] transformToStringValuesSV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType() != FieldSpec.DataType.STRING) {
      return super.transformToStringValuesSV(projectionBlock);
    }
    int[] selected = getSelectedArray(projectionBlock);
    if (_stringResults == null) {
      _stringResults = new String[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    for (int i = 0; i < _elseThenStatements.size(); i++) {
      TransformFunction transformFunction = _elseThenStatements.get(i);
      FieldSpec.DataType dataType = transformFunction.getResultMetadata().getDataType();
      int blockNumDocs = projectionBlock.getNumDocs();
      String[] evalStrings;
      if (dataType == FieldSpec.DataType.STRING) {
        evalStrings = transformFunction.transformToStringValuesSV(projectionBlock);
      } else {
        evalStrings = new String[blockNumDocs];
        switch (dataType) {
          case INT:
            ArrayCopyUtils.copy(transformFunction.transformToIntValuesSV(projectionBlock), evalStrings, blockNumDocs);
            break;
          case LONG:
            ArrayCopyUtils.copy(transformFunction.transformToLongValuesSV(projectionBlock), evalStrings, blockNumDocs);
            break;
          case FLOAT:
            ArrayCopyUtils.copy(transformFunction.transformToFloatValuesSV(projectionBlock), evalStrings, blockNumDocs);
            break;
          case DOUBLE:
            ArrayCopyUtils
                .copy(transformFunction.transformToDoubleValuesSV(projectionBlock), evalStrings, blockNumDocs);
            break;
          case BYTES:
            ArrayCopyUtils.copy(transformFunction.transformToBytesValuesSV(projectionBlock), evalStrings, blockNumDocs);
            break;
          default:
            throw new IllegalStateException(String
                .format("Cannot convert result type [%s] to [STRING] for transform function [%s]", dataType,
                    transformFunction));
        }
      }
      for (int j = 0; j < blockNumDocs; j++) {
        if (selected[j] == i) {
          _stringResults[j] = evalStrings[j];
        }
      }
    }
    return _stringResults;
  }
}
