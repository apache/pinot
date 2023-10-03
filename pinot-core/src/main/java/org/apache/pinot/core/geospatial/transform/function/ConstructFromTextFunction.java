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
import org.apache.pinot.common.Utils;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.operator.transform.function.BaseTransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.segment.local.utils.GeometrySerializer;
import org.apache.pinot.spi.data.FieldSpec;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;


/**
 * An abstract class for implementing the geo constructor functions from text.
 */
abstract class ConstructFromTextFunction extends BaseTransformFunction {
  protected TransformFunction _transformFunction;
  protected byte[][] _results;
  protected WKTReader _reader;

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    super.init(arguments, columnContextMap);
    Preconditions.checkArgument(arguments.size() == 1, "Exactly 1 argument is required for transform function: %s",
        getName());
    TransformFunction transformFunction = arguments.get(0);
    Preconditions.checkArgument(transformFunction.getResultMetadata().isSingleValue(),
        "The argument must be single-valued for transform function: %s", getName());
    Preconditions.checkArgument(transformFunction.getResultMetadata().getDataType() == FieldSpec.DataType.STRING,
        "The argument must be of string type");
    _transformFunction = transformFunction;
    _reader = getWKTReader();
  }

  abstract protected WKTReader getWKTReader();

  @Override
  public TransformResultMetadata getResultMetadata() {
    return BYTES_SV_NO_DICTIONARY_METADATA;
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
        _results[i] = GeometrySerializer.serialize(geometry);
      } catch (ParseException e) {
        Utils.rethrowException(
            new RuntimeException(String.format("Failed to parse geometry from string: %s", argumentValues[i])));
      }
    }
    return _results;
  }
}
