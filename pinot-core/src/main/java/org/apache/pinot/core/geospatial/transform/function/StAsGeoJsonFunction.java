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
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.operator.transform.function.BaseTransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.segment.local.utils.GeometrySerializer;
import org.apache.pinot.segment.local.utils.GeometryUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.locationtech.jts.geom.Geometry;


/**
 * Returns the GEOJson representation of the geometry object.
 */
public class StAsGeoJsonFunction extends BaseTransformFunction {

  public static final String FUNCTION_NAME = "ST_AsGeoJSON";

  private TransformFunction _transformFunction;

  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    super.init(arguments, columnContextMap);
    Preconditions.checkArgument(arguments.size() == 1,
        "Exactly 1 argument is required for transform function: " + FUNCTION_NAME);

    TransformFunction transformFunction = arguments.get(0);
    Preconditions.checkArgument(transformFunction.getResultMetadata().isSingleValue(),
        "Argument must be single-valued for transform function: " + FUNCTION_NAME);
    Preconditions.checkArgument(transformFunction.getResultMetadata().getDataType() == FieldSpec.DataType.BYTES,
        "The argument must be of bytes type");

    _transformFunction = transformFunction;
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return STRING_SV_NO_DICTIONARY_METADATA;
  }

  public String[] transformToStringValuesSV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    initStringValuesSV(numDocs);
    byte[][] values = _transformFunction.transformToBytesValuesSV(valueBlock);
    // use single buffer instead of allocating separate StringBuffer per row
    StringBuilderWriter buffer = new StringBuilderWriter();

    try {
      for (int i = 0; i < numDocs; i++) {
        Geometry geometry = GeometrySerializer.deserialize(values[i]);
        GeometryUtils.GEO_JSON_WRITER.write(geometry, buffer);
        _stringValuesSV[i] = buffer.getString();
        buffer.clear();
      }
    } catch (IOException ioe) {
      // should never happen
      throw new RuntimeException(ioe);
    }
    return _stringValuesSV;
  }
}
