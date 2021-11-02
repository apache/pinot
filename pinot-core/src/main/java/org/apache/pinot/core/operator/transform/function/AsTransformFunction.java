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
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;


public class AsTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "as";

  private TransformFunction _transformFunction;
  private TransformResultMetadata _resultMetadata;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    if (arguments.size() != 1) {
      throw new IllegalArgumentException("Exactly 1 arguments is required for AS transform function");
    }
    _transformFunction = arguments.get(0);
    _resultMetadata = _transformFunction.getResultMetadata();
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Override
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {
    return _transformFunction.transformToIntValuesSV(projectionBlock);
  }

  @Override
  public long[] transformToLongValuesSV(ProjectionBlock projectionBlock) {
    return _transformFunction.transformToLongValuesSV(projectionBlock);
  }

  @Override
  public float[] transformToFloatValuesSV(ProjectionBlock projectionBlock) {
    return _transformFunction.transformToFloatValuesSV(projectionBlock);
  }

  @Override
  public double[] transformToDoubleValuesSV(ProjectionBlock projectionBlock) {
    return _transformFunction.transformToDoubleValuesSV(projectionBlock);
  }

  @Override
  public String[] transformToStringValuesSV(ProjectionBlock projectionBlock) {
    return _transformFunction.transformToStringValuesSV(projectionBlock);
  }

  @Override
  public int[][] transformToIntValuesMV(ProjectionBlock projectionBlock) {
    return _transformFunction.transformToIntValuesMV(projectionBlock);
  }

  @Override
  public long[][] transformToLongValuesMV(ProjectionBlock projectionBlock) {
    return _transformFunction.transformToLongValuesMV(projectionBlock);
  }

  @Override
  public float[][] transformToFloatValuesMV(ProjectionBlock projectionBlock) {
    return _transformFunction.transformToFloatValuesMV(projectionBlock);
  }

  @Override
  public double[][] transformToDoubleValuesMV(ProjectionBlock projectionBlock) {
    return _transformFunction.transformToDoubleValuesMV(projectionBlock);
  }

  @Override
  public String[][] transformToStringValuesMV(ProjectionBlock projectionBlock) {
    return _transformFunction.transformToStringValuesMV(projectionBlock);
  }
}
