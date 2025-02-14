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
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.NativeMutableTextIndex;
import org.apache.pinot.segment.local.segment.index.readers.text.NativeTextIndexReader;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/*
 * This class implements TEXT_MATCH() as a transform function.
 */
public class TextMatchTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "textMatch";
  private String _predicate;
  private TextIndexReader _textIndexReader;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    super.init(arguments, columnContextMap);

    TransformFunction columnArg = arguments.get(0);
    if (!(columnArg instanceof IdentifierTransformFunction && columnArg.getResultMetadata().isSingleValue())) {
      throw new IllegalArgumentException(
          "The first argument of TEXT_MATCH transform function must be a single-valued column");
    }
    String columnName = ((IdentifierTransformFunction) columnArg).getColumnName();
    DataSource dataSource = columnContextMap.get(columnName).getDataSource();
    if (dataSource == null) {
      throw new IllegalArgumentException("Cannot apply TEXT_MATCH on column: " + columnName + " without text index");
    }
    TextIndexReader indexReader = dataSource.getTextIndex();
    if (indexReader == null) {
      throw new IllegalArgumentException("Cannot apply TEXT_MATCH on column: " + columnName + " without text index");
    }
    if (indexReader instanceof NativeTextIndexReader
        || indexReader instanceof NativeMutableTextIndex) {
      throw new UnsupportedOperationException(
          "TEXT_MATCH is not supported on column: " + columnName + " with native text index");
    }

    TransformFunction predicate = arguments.get(1);
    if (!(predicate instanceof LiteralTransformFunction && predicate.getResultMetadata().isSingleValue())) {
      throw new IllegalArgumentException(
          "The second argument of TEXT_MATCH transform function must be a single-valued string literal");
    }

    _predicate = ((LiteralTransformFunction) predicate).getStringLiteral();
    _textIndexReader = indexReader;
  }

  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initIntValuesSV(length);

    int[] docIds = valueBlock.getDocIds();
    MutableRoaringBitmap indexDocIds = _textIndexReader.getDocIds(_predicate);

    for (int i = 0; i < length; i++) {
      if (indexDocIds.contains(docIds[i])) {
        _intValuesSV[i] = 1;
      }
    }

    return _intValuesSV;
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return BOOLEAN_SV_NO_DICTIONARY_METADATA;
  }
}
