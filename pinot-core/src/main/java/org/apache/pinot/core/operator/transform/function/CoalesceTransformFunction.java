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
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;


/**
 * The <code>CoalesceTransformFunction</code> implements the Coalesce operator.
 *
 * The results are in String format for first non-null value in the argument list.
 * If all arguments are null, return a 'null' string.
 * Note: arguments have to be column names.
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
  private NullValueVectorReader[] _nullValueReaders;
  private TransformFunction[] _transformFunctions;

  @Override
  public String getName() {
    return TransformFunctionType.COALESCE.getName();
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    int argSize = arguments.size();
    _nullValueReaders = new NullValueVectorReader[argSize];
    _transformFunctions = new TransformFunction[argSize];
    for (int i = 0; i < argSize; i++) {
      TransformFunction func = arguments.get(i);
      if (!(func instanceof IdentifierTransformFunction)) {
        throw new IllegalArgumentException("Only column names are supported in COALESCE.");
      }
      String columnName = ((IdentifierTransformFunction) func).getColumnName();
      NullValueVectorReader nullValueVectorReader = dataSourceMap.get(columnName).getNullValueVector();
      _nullValueReaders[i] = nullValueVectorReader;
      _transformFunctions[i] = func;
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return STRING_MV_NO_DICTIONARY_METADATA;
  }

  @Override
  public String[] transformToStringValuesSV(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();
    if (_results == null || _results.length < length) {
      _results = new String[length];
    }

    int[] docIds = projectionBlock.getDocIds();
    int width = _transformFunctions.length;
    String[][] data = new String[width][length];
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < width; j++) {
        // Consider value as null when null option is disabled.
        if (_nullValueReaders != null && _nullValueReaders[j].isNull(docIds[i])) {
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
