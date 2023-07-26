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
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.segment.local.utils.GeometrySerializer;
import org.apache.pinot.segment.local.utils.GeometryUtils;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;


/**
 * Constructor function for polygon object from text.
 */
public class StPolygonFunction extends ConstructFromTextFunction {
  public static final String FUNCTION_NAME = "ST_Polygon";

  @Override
  protected WKTReader getWKTReader() {
    return GeometryUtils.GEOMETRY_WKT_READER;
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public byte[][] transformToBytesValuesSV(ValueBlock valueBlock) {
    if (_results == null) {
      _results = new byte[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
    }
    String[] argumentValues = _transformFunction.transformToStringValuesSV(valueBlock);
    int length = valueBlock.getNumDocs();
    for (int i = 0; i < length; i++) {
      try {
        Geometry geometry = _reader.read(argumentValues[i]);
        Preconditions.checkArgument(geometry instanceof Polygon, "The geometry object must be polygon");
        _results[i] = GeometrySerializer.serialize(geometry);
      } catch (ParseException e) {
        new RuntimeException(String.format("Failed to parse geometry from string: %s", argumentValues[i]));
      }
    }
    return _results;
  }
}
