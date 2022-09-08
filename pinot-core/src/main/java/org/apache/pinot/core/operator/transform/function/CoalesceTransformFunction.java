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

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.RoaringBitmap;


/**
 * The <code>CoalesceTransformFunction</code> implements the Coalesce operator.
 *
 * The results are in String format for first non-null value in the argument list.
 * If all arguments are null, return a 'null' string.
 * Note: arguments have to be column names and numeric or string types.
 *
 * Expected result:
 * Coalesce(nullColumn, columnA): columnA
 * Coalesce(columnA, nullColumn): nullColumn
 * Coalesce(nullColumnA, nullColumnB): "null"
 *
 * Note this operator only takes column names for now.
 * SQL Syntax:
 *    Coalesce(columnA, columnB)
 */
public class CoalesceTransformFunction extends BaseTransformFunction {
  private String[] _results;
  private TransformFunction[] _transformFunctions;

  /**
   * Returns a bit map of corresponding column.
   * Returns an empty bitmap by default if null option is disabled.
   */
  private static RoaringBitmap[] getNullBitMaps(ProjectionBlock projectionBlock,
      TransformFunction[] transformFunctions) {
    RoaringBitmap[] roaringBitmaps = new RoaringBitmap[transformFunctions.length];
    for (int i = 0; i < roaringBitmaps.length; i++) {
      TransformFunction func = transformFunctions[i];
      String columnName = ((IdentifierTransformFunction) func).getColumnName();
      RoaringBitmap nullBitmap = projectionBlock.getBlockValueSet(columnName).getNullBitmap();
      roaringBitmaps[i] = nullBitmap;
    }
    return roaringBitmaps;
  }

  @Override
  public String getName() {
    return TransformFunctionType.COALESCE.getName();
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    int argSize = arguments.size();
    _transformFunctions = new TransformFunction[argSize];
    for (int i = 0; i < argSize; i++) {
      TransformFunction func = arguments.get(i);
      Preconditions.checkArgument(func instanceof IdentifierTransformFunction,
          "Only column names are supported in COALESCE.");
      FieldSpec.DataType dataType = func.getResultMetadata().getDataType().getStoredType();
      Preconditions.checkArgument(dataType.isNumeric() || dataType == FieldSpec.DataType.STRING,
          "Only numeric value and string are supported in COALESCE.");
      _transformFunctions[i] = func;
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return STRING_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public String[] transformToStringValuesSV(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();
    if (_results == null || _results.length < length) {
      _results = new String[length];
    }

    RoaringBitmap[] nullBitMaps = getNullBitMaps(projectionBlock, _transformFunctions);
    int[] docIds = projectionBlock.getDocIds();
    int width = _transformFunctions.length;
    String[][] data = new String[width][length];
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < width; j++) {
        // Consider value as null when null option is disabled.
        if (nullBitMaps[j] == null || nullBitMaps[j].contains(i)) {
          continue;
        }
        if (data[j][i] == null) {
          data[j] = _transformFunctions[j].transformToStringValuesSV(projectionBlock);
        }
        // TODO: Return result as a generic type.
        _results[i] = data[j][i];
        break;
      }
      // TODO: Differentiate between null string and null value.
      if (_results[i] == null) {
        _results[i] = "null";
      }
    }
    return _results;
  }
}
