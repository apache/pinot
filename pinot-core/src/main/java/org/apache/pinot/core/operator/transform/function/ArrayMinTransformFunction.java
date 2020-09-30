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

import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * The ArrayMinTransformFunction class implements array_min function for multi-valued columns
 *
 * Sample queries:
 * SELECT COUNT(*) FROM table WHERE array_min(mvColumn) > 2
 * SELECT COUNT(*) FROM table GROUP BY array_min(mvColumn)
 * SELECT SUM(array_min(mvColumn)) FROM table
 */
public class ArrayMinTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "array_min";

  private int[] _intValuesSV;
  private long[] _longValuesSV;
  private float[] _floatValuesSV;
  private double[] _doubleValuesSV;
  private String[] _stringValuesSV;
  private TransformFunction _argument;
  private TransformResultMetadata _resultMetadata;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    // Check that there is only 1 argument
    if (arguments.size() != 1) {
      throw new IllegalArgumentException("Exactly 1 argument is required for ARRAY_MIN transform function");
    }

    // Check that the argument is a multi-valued column or transform function
    TransformFunction firstArgument = arguments.get(0);
    if (firstArgument instanceof LiteralTransformFunction || firstArgument.getResultMetadata().isSingleValue()) {
      throw new IllegalArgumentException(
          "The argument of ARRAY_MIN transform function must be a multi-valued column or a transform function");
    }
    _resultMetadata = new TransformResultMetadata(firstArgument.getResultMetadata().getDataType(), true, false);
    _argument = firstArgument;
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Override
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {
    if (_argument.getResultMetadata().getDataType() != FieldSpec.DataType.INT) {
      return super.transformToIntValuesSV(projectionBlock);
    }
    if (_intValuesSV == null) {
      _intValuesSV = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    int length = projectionBlock.getNumDocs();
    int[][] intValuesMV = _argument.transformToIntValuesMV(projectionBlock);
    for (int i = 0; i < length; i++) {
      int minRes = Integer.MAX_VALUE;
      for (int j = 0; j < intValuesMV[i].length; j++) {
        minRes = Math.min(minRes, intValuesMV[i][j]);
      }
      _intValuesSV[i] = minRes;
    }
    return _intValuesSV;
  }

  @Override
  public long[] transformToLongValuesSV(ProjectionBlock projectionBlock) {
    if (_argument.getResultMetadata().getDataType() != FieldSpec.DataType.LONG) {
      return super.transformToLongValuesSV(projectionBlock);
    }
    if (_longValuesSV == null) {
      _longValuesSV = new long[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    int length = projectionBlock.getNumDocs();
    long[][] longValuesMV = _argument.transformToLongValuesMV(projectionBlock);
    for (int i = 0; i < length; i++) {
      long minRes = Long.MAX_VALUE;
      for (int j = 0; j < longValuesMV[i].length; j++) {
        minRes = Math.min(minRes, longValuesMV[i][j]);
      }
      _longValuesSV[i] = minRes;
    }
    return _longValuesSV;
  }

  @Override
  public float[] transformToFloatValuesSV(ProjectionBlock projectionBlock) {
    if (_argument.getResultMetadata().getDataType() != FieldSpec.DataType.FLOAT) {
      return super.transformToFloatValuesSV(projectionBlock);
    }
    if (_floatValuesSV == null) {
      _floatValuesSV = new float[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    int length = projectionBlock.getNumDocs();
    float[][] floatValuesMV = _argument.transformToFloatValuesMV(projectionBlock);
    for (int i = 0; i < length; i++) {
      float minRes = Float.POSITIVE_INFINITY;
      for (int j = 0; j < floatValuesMV[i].length; j++) {
        minRes = Math.min(minRes, floatValuesMV[i][j]);
      }
      _floatValuesSV[i] = minRes;
    }
    return _floatValuesSV;
  }

  @Override
  public double[] transformToDoubleValuesSV(ProjectionBlock projectionBlock) {
    if (_argument.getResultMetadata().getDataType() != FieldSpec.DataType.DOUBLE) {
      return super.transformToDoubleValuesSV(projectionBlock);
    }
    if (_doubleValuesSV == null) {
      _doubleValuesSV = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    int length = projectionBlock.getNumDocs();
    double[][] doubleValuesMV = _argument.transformToDoubleValuesMV(projectionBlock);
    for (int i = 0; i < length; i++) {
      double minRes = Double.POSITIVE_INFINITY;
      for (int j = 0; j < doubleValuesMV[i].length; j++) {
        minRes = Math.min(minRes, doubleValuesMV[i][j]);
      }
      _doubleValuesSV[i] = minRes;
    }
    return _doubleValuesSV;
  }

  @Override
  public String[] transformToStringValuesSV(ProjectionBlock projectionBlock) {
    if (_argument.getResultMetadata().getDataType() != FieldSpec.DataType.STRING) {
      return super.transformToStringValuesSV(projectionBlock);
    }
    if (_stringValuesSV == null) {
      _stringValuesSV = new String[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    int length = projectionBlock.getNumDocs();
    String[][] stringValuesMV = _argument.transformToStringValuesMV(projectionBlock);
    for (int i = 0; i < length; i++) {
      String minRes = null;
      for (int j = 0; j < stringValuesMV[i].length; j++) {
        if (StringUtils.compare(minRes, stringValuesMV[i][j]) > 0) {
          minRes = stringValuesMV[i][j];
        }
      }
      _stringValuesSV[i] = minRes;
    }
    return _stringValuesSV;
  }
}
