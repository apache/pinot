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
import org.apache.pinot.core.util.ArrayCopyUtils;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * The ArrayMaxTransformFunction class implements array_max function for multi-valued columns
 *
 * Sample queries:
 * SELECT COUNT(*) FROM table WHERE array_max(mvColumn) > 2
 * SELECT COUNT(*) FROM table GROUP BY array_max(mvColumn)
 * SELECT SUM(array_max(mvColumn)) FROM table
 */
public class ArrayMaxTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "array_max";

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
      throw new IllegalArgumentException("Exactly 1 argument is required for ARRAY_MAX transform function");
    }

    // Check that the argument is a multi-valued column or transform function
    TransformFunction firstArgument = arguments.get(0);
    if (firstArgument instanceof LiteralTransformFunction || firstArgument.getResultMetadata().isSingleValue()) {
      throw new IllegalArgumentException(
          "The argument of ARRAY_MAX transform function must be a multi-valued column or a transform function");
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
      int maxRes = Integer.MIN_VALUE;
      for (int j = 0; j < intValuesMV[i].length; j++) {
        maxRes = Math.max(maxRes, intValuesMV[i][j]);
      }
      _intValuesSV[i] = maxRes;
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
      long maxRes = Long.MIN_VALUE;
      for (int j = 0; j < longValuesMV[i].length; j++) {
        maxRes = Math.max(maxRes, longValuesMV[i][j]);
      }
      _longValuesSV[i] = maxRes;
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
      float maxRes = Float.NEGATIVE_INFINITY;
      for (int j = 0; j < floatValuesMV[i].length; j++) {
        maxRes = Math.max(maxRes, floatValuesMV[i][j]);
      }
      _floatValuesSV[i] = maxRes;
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
      double maxRes = Double.NEGATIVE_INFINITY;
      for (int j = 0; j < doubleValuesMV[i].length; j++) {
        maxRes = Math.max(maxRes, doubleValuesMV[i][j]);
      }
      _doubleValuesSV[i] = maxRes;
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
      String maxRes = null;
      for (int j = 0; j < stringValuesMV[i].length; j++) {
        if (StringUtils.compare(maxRes, stringValuesMV[i][j]) < 0) {
          maxRes = stringValuesMV[i][j];
        }
      }
      _stringValuesSV[i] = maxRes;
    }
    return _stringValuesSV;
  }
}
