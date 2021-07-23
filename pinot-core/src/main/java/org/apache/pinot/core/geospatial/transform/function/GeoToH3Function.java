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
package org.apache.pinot.core.geospatial.transform.function;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.operator.transform.function.BaseTransformFunction;
import org.apache.pinot.core.operator.transform.function.LiteralTransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.segment.local.utils.GeometrySerializer;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.data.FieldSpec;
import org.locationtech.jts.geom.Geometry;


/**
 *  A function that returns the H3 index address of a given geolocation.
 */
public class GeoToH3Function extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "geoToH3";
  private TransformFunction _firstArgument;
  private TransformFunction _secondArgument;
  private TransformFunction _thirdArgument;
  private long[] _results;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    Preconditions.checkArgument(arguments.size() == 3 || arguments.size() == 2,
        "Transform function %s requires 2 or 3 arguments", getName());
    if (arguments.size() == 3) {
      TransformFunction transformFunction = arguments.get(0);
      Preconditions.checkArgument(transformFunction.getResultMetadata().isSingleValue(),
          "First argument must be single-valued for transform function: %s", getName());
      Preconditions.checkArgument(transformFunction.getResultMetadata().getDataType().getStoredType().isNumeric()
          || transformFunction instanceof LiteralTransformFunction, "The first argument must be numeric");
      _firstArgument = transformFunction;
      transformFunction = arguments.get(1);
      Preconditions.checkArgument(transformFunction.getResultMetadata().isSingleValue(),
          "Second argument must be single-valued for transform function: %s", getName());
      Preconditions.checkArgument(transformFunction.getResultMetadata().getDataType().getStoredType().isNumeric()
          || transformFunction instanceof LiteralTransformFunction, "The second argument must be numeric");
      _secondArgument = transformFunction;
      transformFunction = arguments.get(2);
      Preconditions.checkArgument(transformFunction.getResultMetadata().isSingleValue(),
          "Third argument must be single-valued for transform function: %s", getName());
      Preconditions.checkArgument(transformFunction.getResultMetadata().getDataType().getStoredType().isNumeric()
          || transformFunction instanceof LiteralTransformFunction, "The third argument must be numeric");
      _thirdArgument = transformFunction;
    } else {
      TransformFunction transformFunction = arguments.get(0);
      Preconditions.checkArgument(transformFunction.getResultMetadata().isSingleValue(),
          "First argument must be single-valued for transform function: %s", getName());
      Preconditions.checkArgument(transformFunction.getResultMetadata().getDataType() == FieldSpec.DataType.BYTES,
          "The first argument must be bytes");
      _firstArgument = transformFunction;
      transformFunction = arguments.get(1);
      Preconditions.checkArgument(transformFunction.getResultMetadata().isSingleValue(),
          "Second argument must be single-valued for transform function: %s", getName());
      Preconditions.checkArgument(transformFunction.getResultMetadata().getDataType().getStoredType().isNumeric()
          || transformFunction instanceof LiteralTransformFunction, "The second argument must be numeric");
      _secondArgument = transformFunction;
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return LONG_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public long[] transformToLongValuesSV(ProjectionBlock projectionBlock) {
    if (_results == null) {
      _results = new long[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }

    if (_thirdArgument == null) {
      byte[][] geoValues = _firstArgument.transformToBytesValuesSV(projectionBlock);
      int[] resValues = _secondArgument.transformToIntValuesSV(projectionBlock);
      for (int i = 0; i < projectionBlock.getNumDocs(); i++) {
        Geometry geometry = GeometrySerializer.deserialize(geoValues[i]);
        _results[i] = ScalarFunctions.geoToH3(geometry.getCoordinate().x, geometry.getCoordinate().y, resValues[i]);
      }
    } else {
      double[] lonValues = _firstArgument.transformToDoubleValuesSV(projectionBlock);
      double[] latValues = _secondArgument.transformToDoubleValuesSV(projectionBlock);
      int[] resValues = _thirdArgument.transformToIntValuesSV(projectionBlock);
      for (int i = 0; i < projectionBlock.getNumDocs(); i++) {
        _results[i] = ScalarFunctions.geoToH3(lonValues[i], latValues[i], resValues[i]);
      }
    }

    return _results;
  }
}
