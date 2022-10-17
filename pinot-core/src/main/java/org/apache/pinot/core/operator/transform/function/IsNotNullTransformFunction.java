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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.roaringbitmap.PeekableIntIterator;


public class IsNotNullTransformFunction extends BaseTransformFunction {

  private int[] _results;
  private PeekableIntIterator _nullValueVectorIterator;

  @Override
  public String getName() {
    return TransformFunctionType.IS_NOT_NULL.getName();
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    Preconditions.checkArgument(arguments.size() == 1,
        "Exact 1 argument is required for IS_NOT_NULL operator function");
    TransformFunction transformFunction = arguments.get(0);
    if (!(transformFunction instanceof IdentifierTransformFunction)) {
      throw new IllegalArgumentException(
          "Only column names are supported in IS_NOT_NULL. Support for functions is planned for future release");
    }
    String columnName = ((IdentifierTransformFunction) transformFunction).getColumnName();
    NullValueVectorReader nullValueVectorReader = dataSourceMap.get(columnName).getNullValueVector();
    if (nullValueVectorReader != null) {
      _nullValueVectorIterator = nullValueVectorReader.getNullBitmap().getIntIterator();
    } else {
      _nullValueVectorIterator = null;
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return BOOLEAN_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();
    if (_results == null || _results.length < length) {
      _results = new int[length];
    }

    int[] docIds = projectionBlock.getDocIds();

    Arrays.fill(_results, 1);
    if (_nullValueVectorIterator != null) {
      int currentDocIdIndex = 0;
      while (_nullValueVectorIterator.hasNext() & currentDocIdIndex < length) {
        _nullValueVectorIterator.advanceIfNeeded(docIds[currentDocIdIndex]);
        if (_nullValueVectorIterator.hasNext()) {
          currentDocIdIndex = Arrays.binarySearch(docIds, currentDocIdIndex, length, _nullValueVectorIterator.next());
          if (currentDocIdIndex >= 0) {
            _results[currentDocIdIndex] = 0;
            currentDocIdIndex++;
          } else {
            currentDocIdIndex = -currentDocIdIndex - 1;
          }
        }
      }
    }

    return _results;
  }
}
