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


public class CastTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "cast";

  private TransformFunction _transformFunction;
  private String _toFormat;
  private TransformResultMetadata _resultMetadata;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    // Check that there are more than 1 arguments
    if (arguments.size() != 2) {
      throw new IllegalArgumentException("Exactly 2 arguments are required for CAST transform function");
    }

    _transformFunction = arguments.get(0);
    TransformFunction castFormatTransformFunction = arguments.get(1);

    if (castFormatTransformFunction instanceof LiteralTransformFunction) {
      _toFormat = ((LiteralTransformFunction)castFormatTransformFunction).getLiteral().toUpperCase();
      switch (_toFormat) {
        case "INT":
        case "INTEGER":
        case "LONG":
          _resultMetadata = LONG_SV_NO_DICTIONARY_METADATA;
          break;
        case "FLOAT":
        case "DOUBLE":
          _resultMetadata = DOUBLE_SV_NO_DICTIONARY_METADATA;
          break;
        case "STRING":
        case "VARCHAR":
          _resultMetadata = STRING_SV_NO_DICTIONARY_METADATA;
          break;
        default:
          throw new IllegalArgumentException("Unable to cast expression to type - " + _toFormat);
      }
    } else {
      throw new IllegalArgumentException("Invalid cast to type - " + castFormatTransformFunction.getName());
    }
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
}
