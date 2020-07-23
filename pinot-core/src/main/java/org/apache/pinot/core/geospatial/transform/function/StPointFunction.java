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
import org.apache.pinot.common.function.annotations.ScalarFunction;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.geospatial.GeometryUtils;
import org.apache.pinot.core.geospatial.serde.GeometrySerializer;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.operator.transform.function.BaseTransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;


/**
 * Function that returns a geometry type point object with the given coordinate values.
 */
public class StPointFunction extends BaseTransformFunction {
  private static final Logger LOGGER = LoggerFactory.getLogger(StPointFunction.class);

  public static final String FUNCTION_NAME = "ST_Point";
  private TransformFunction _firstArgument;
  private TransformFunction _secondArgument;
  private byte[][] _results;

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
    _firstArgument = transformFunction;
    transformFunction = arguments.get(1);
    Preconditions.checkArgument(transformFunction.getResultMetadata().isSingleValue(),
        "Second argument must be single-valued for transform function: %s", getName());
    _secondArgument = transformFunction;
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return BYTES_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public byte[][] transformToBytesValuesSV(ProjectionBlock projectionBlock) {
    if (_results == null) {
      _results = new byte[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
    }
    double[] firstValues = _firstArgument.transformToDoubleValuesSV(projectionBlock);
    double[] secondValues = _secondArgument.transformToDoubleValuesSV(projectionBlock);
    for (int i = 0; i < projectionBlock.getNumDocs(); i++) {
      _results[i] = ScalarFunctions.stPoint(firstValues[i], secondValues[i]);
    }
    return _results;
  }

}
