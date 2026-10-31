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
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.MultiColumnTextIndexReader;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/*
 * This class implements TEXT_MATCH() as a transform function.
 */
public class TextMatchTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "textMatch";
  private String _predicate;
  private String _options;
  private TextIndexReader _textIndexReader;
  private TransformFunction _columnArg;
  private Dictionary _dictionary;
  private String _column;

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
      indexReader = dataSource.getMultiColumnTextIndex();
      _column = columnName;
    }
    if (indexReader == null) {
      throw new IllegalArgumentException("Cannot apply TEXT_MATCH on column: " + columnName + " without text index");
    }

    TransformFunction predicate = arguments.get(1);
    if (!(predicate instanceof LiteralTransformFunction && predicate.getResultMetadata().isSingleValue())) {
      throw new IllegalArgumentException(
          "The second argument of TEXT_MATCH transform function must be a single-valued string literal");
    }

    // Verify that the second parameter is actually a string literal
    LiteralTransformFunction literalPredicate = (LiteralTransformFunction) predicate;
    if (literalPredicate.getResultMetadata().getDataType() != DataType.STRING) {
      throw new IllegalArgumentException(
          "The second argument of TEXT_MATCH transform function must be a string literal, got: "
              + literalPredicate.getResultMetadata().getDataType());
    }

    _predicate = literalPredicate.getStringLiteral();

    // Handle optional third parameter for options
    if (arguments.size() > 2) {
      TransformFunction options = arguments.get(2);
      if (!(options instanceof LiteralTransformFunction && options.getResultMetadata().isSingleValue())) {
        throw new IllegalArgumentException(
            "The third argument of TEXT_MATCH transform function must be a single-valued string literal");
      }

      // Verify that the third parameter is actually a string literal
      LiteralTransformFunction literalOptions = (LiteralTransformFunction) options;
      if (literalOptions.getResultMetadata().getDataType() != DataType.STRING) {
        throw new IllegalArgumentException(
            "The third argument of TEXT_MATCH transform function must be a string literal, got: "
                + literalOptions.getResultMetadata().getDataType());
      }

      _options = literalOptions.getStringLiteral();
    } else {
      _options = null;
    }

    if (indexReader.isBuildOnDictionary() && _options != null) {
      throw new IllegalArgumentException(
          "TEXT_MATCH options are not supported for a dictionary-based text index on column: " + columnName);
    }
    _textIndexReader = indexReader;
    _columnArg = columnArg;
    _dictionary = dataSource.getDictionary();
  }

  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initZeroFillingIntValuesSV(length);

    int[] ids;
    ImmutableRoaringBitmap matchingIds;
    if (_textIndexReader.isBuildOnDictionary()) {
      matchingIds = _textIndexReader.getDictIds(_predicate);
      if (_columnArg.getDictionary() == null) {
        // A standalone dictionary can coexist with a raw forward index.
        String[] values = _columnArg.transformToStringValuesSV(valueBlock);
        for (int i = 0; i < length; i++) {
          if (matchingIds.contains(_dictionary.indexOf(values[i]))) {
            _intValuesSV[i] = 1;
          }
        }
        return _intValuesSV;
      }
      ids = _columnArg.transformToDictIdsSV(valueBlock);
    } else {
      ids = valueBlock.getDocIds();
      matchingIds = _textIndexReader.isMultiColumn()
          ? ((MultiColumnTextIndexReader) _textIndexReader).getDocIds(_column, _predicate, _options)
          : _textIndexReader.getDocIds(_predicate, _options);
    }

    for (int i = 0; i < length; i++) {
      if (matchingIds.contains(ids[i])) {
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
