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

import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.local.utils.GeometryUtils;
import org.locationtech.jts.geom.Geometry;


/**
 * Function that checks the containment of the two geo-spatial objects. It returns true if and only if no points of the
 * second geometry lie in the exterior of the first geometry, and at least one point of the interior of the first
 * geometry lies in the interior of the second geometry.
 */
public class StContainsFunction extends BaseBinaryGeoTransformFunction {
  public static final String FUNCTION_NAME = "ST_Contains";

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return INT_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    return transformGeometryToIntValuesSV(valueBlock);
  }

  @Override
  public int transformGeometryToInt(Geometry firstGeometry, Geometry secondGeometry) {
    if (GeometryUtils.isGeography(firstGeometry) != GeometryUtils.isGeography(secondGeometry)) {
      throw new RuntimeException("The first and second arguments shall either all be geometry or all geography");
    }
    // TODO: to fully support Geography contains operation.
    return firstGeometry.contains(secondGeometry) ? 1 : 0;
  }
}
