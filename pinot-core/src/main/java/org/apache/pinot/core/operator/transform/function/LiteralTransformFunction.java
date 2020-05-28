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

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.apache.pinot.spi.utils.BytesUtils;


/**
 * The <code>LiteralTransformFunction</code> class is a special transform function which is a wrapper on top of a
 * LITERAL, and only supports {@link #getLiteral()}.
 */
public class LiteralTransformFunction implements TransformFunction {
  private final String _literal;
  private String[] _result;
  private int[] _intResult;
  private long[] _longResult;
  private float[] _floatResult;
  private double[] _doubleResult;
  private byte[][] _bytesResult;

  public LiteralTransformFunction(String literal) {
    _literal = literal;
  }

  public String getLiteral() {
    return _literal;
  }

  @Override
  public String getName() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return BaseTransformFunction.STRING_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public Dictionary getDictionary() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[] transformToDictIdsSV(ProjectionBlock projectionBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[][] transformToDictIdsMV(ProjectionBlock projectionBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {
    if (_intResult == null) {
      _intResult = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
      Arrays.fill(_intResult, Double.valueOf(_literal).intValue());
    }
    return _intResult;
  }

  @Override
  public long[] transformToLongValuesSV(ProjectionBlock projectionBlock) {
    if (_longResult == null) {
      _longResult = new long[DocIdSetPlanNode.MAX_DOC_PER_CALL];
      // Use BigDecimal here as: Long.parseLong(String) doesn't parse floats,
      // Double.valueOf(String) loses precision for large Long value.
      Arrays.fill(_longResult, new BigDecimal(_literal).longValue());
    }
    return _longResult;
  }

  @Override
  public float[] transformToFloatValuesSV(ProjectionBlock projectionBlock) {
    if (_floatResult == null) {
      _floatResult = new float[DocIdSetPlanNode.MAX_DOC_PER_CALL];
      Arrays.fill(_floatResult, Double.valueOf(_literal).floatValue());
    }
    return _floatResult;
  }

  @Override
  public double[] transformToDoubleValuesSV(ProjectionBlock projectionBlock) {
    if (_doubleResult == null) {
      _doubleResult = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL];
      Arrays.fill(_doubleResult, Double.valueOf(_literal));
    }
    return _doubleResult;
  }

  @Override
  public String[] transformToStringValuesSV(ProjectionBlock projectionBlock) {
    if (_result == null) {
      _result = new String[DocIdSetPlanNode.MAX_DOC_PER_CALL];
      Arrays.fill(_result, _literal);
    }
    return _result;
  }

  @Override
  public byte[][] transformToBytesValuesSV(ProjectionBlock projectionBlock) {
    if (_bytesResult == null) {
      _bytesResult = new byte[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
      Arrays.fill(_bytesResult, BytesUtils.toBytes(_literal));
    }
    return _bytesResult;
  }

  @Override
  public int[][] transformToIntValuesMV(ProjectionBlock projectionBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long[][] transformToLongValuesMV(ProjectionBlock projectionBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float[][] transformToFloatValuesMV(ProjectionBlock projectionBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double[][] transformToDoubleValuesMV(ProjectionBlock projectionBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String[][] transformToStringValuesMV(ProjectionBlock projectionBlock) {
    throw new UnsupportedOperationException();
  }
}
