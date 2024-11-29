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
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.operator.transform.function.BaseTransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.segment.local.utils.GeometrySerializer;
import org.apache.pinot.spi.data.FieldSpec;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.geojson.GeoJsonReader;


/**
 * An abstract class for implementing the geo constructor functions from GEO JSON.
 */
abstract class ConstructFromGeoJsonFunction extends BaseTransformFunction {

  protected TransformFunction _transformFunction;
  protected GeoJsonReader _reader;

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    super.init(arguments, columnContextMap);

    Preconditions.checkArgument(arguments.size() == 1,
        "Exactly 1 argument is required for transform function: " + getName());

    TransformFunction transformFunction = arguments.get(0);

    Preconditions.checkArgument(transformFunction.getResultMetadata().isSingleValue(),
        "The argument must be single-valued for transform function: " + getName());
    Preconditions.checkArgument(transformFunction.getResultMetadata().getDataType() == FieldSpec.DataType.STRING,
        "The argument must be of string type");

    _transformFunction = transformFunction;
    _reader = getGeoJsonReader();
  }

  abstract protected GeoJsonReader getGeoJsonReader();

  @Override
  public TransformResultMetadata getResultMetadata() {
    return BYTES_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public byte[][] transformToBytesValuesSV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    initBytesValuesSV(numDocs);
    String[] argumentValues = _transformFunction.transformToStringValuesSV(valueBlock);
    // use single reader instead of allocating separate instance per row
    StringReader reader = new StringReader();
    for (int i = 0; i < numDocs; i++) {
      try {
        reader.setString(argumentValues[i]);
        Geometry geometry = _reader.read(reader);
        _bytesValuesSV[i] = GeometrySerializer.serialize(geometry);
      } catch (ParseException e) {
        throw new RuntimeException(String.format("Failed to parse geometry from string: %s", argumentValues[i]));
      }
    }
    return _bytesValuesSV;
  }
}
