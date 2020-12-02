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
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.geospatial.GeometryUtils;
import org.apache.pinot.core.geospatial.serde.GeometrySerializer;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.operator.transform.function.BaseTransformFunction;
import org.apache.pinot.core.operator.transform.function.LiteralTransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.spi.data.FieldSpec;
import org.locationtech.jts.geom.Geometry;

import java.util.List;
import java.util.Map;


/**
 * Function that checks the containment of the two geo-spatial objects. It returns true if and only if no points of the
 * second geometry lie in the exterior of the first geometry, and at least one point of the interior of the first
 * geometry lies in the interior of the second geometry.
 */
public class StContainsFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "ST_Contains";
  private TransformFunction _firstArgument;
  private TransformFunction _secondArgument;
  private int[] _results;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    Preconditions
        .checkArgument(arguments.size() == 2, "2 arguments are required for transform function: %s", getName());
    TransformFunction transformFunction = arguments.get(0);
    Preconditions.checkArgument(transformFunction.getResultMetadata().isSingleValue(),
        "First argument must be single-valued for transform function: %s", getName());
    Preconditions.checkArgument(transformFunction.getResultMetadata().getDataType() == FieldSpec.DataType.BYTES
        || transformFunction instanceof LiteralTransformFunction, "The first argument must be of bytes type");
    _firstArgument = transformFunction;
    transformFunction = arguments.get(1);
    Preconditions.checkArgument(transformFunction.getResultMetadata().isSingleValue(),
        "Second argument must be single-valued for transform function: %s", getName());
    Preconditions.checkArgument(transformFunction.getResultMetadata().getDataType() == FieldSpec.DataType.BYTES
        || transformFunction instanceof LiteralTransformFunction, "The second argument must be of bytes type");
    _secondArgument = transformFunction;
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return INT_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {
    if (_results == null) {
      _results = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    byte[][] firstValues = _firstArgument.transformToBytesValuesSV(projectionBlock);
    byte[][] secondValues = _secondArgument.transformToBytesValuesSV(projectionBlock);
    for (int i = 0; i < projectionBlock.getNumDocs(); i++) {
      Geometry firstGeometry = GeometrySerializer.deserialize(firstValues[i]);
      Geometry secondGeometry = GeometrySerializer.deserialize(secondValues[i]);
      if (GeometryUtils.isGeography(firstGeometry) || GeometryUtils.isGeography(secondGeometry)) {
        throw new RuntimeException(String.format("%s is available for Geometry objects only", FUNCTION_NAME));
      }
      _results[i] = firstGeometry.contains(secondGeometry) ? 1 : 0;
    }
    return _results;
  }
}
