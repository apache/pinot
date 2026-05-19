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
package org.apache.pinot.core.operator;

import javax.annotation.Nullable;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.spi.data.FieldSpec.DataType;


public class ColumnContext {
  private final DataType _dataType;
  private final boolean _isSingleValue;
  private final Dictionary _dictionary;
  private final boolean _dictionaryEncoded;
  private final DataSource _dataSource;

  private ColumnContext(DataType dataType, boolean isSingleValue, @Nullable Dictionary dictionary,
      boolean dictionaryEncoded, @Nullable DataSource dataSource) {
    _dataType = dataType;
    _isSingleValue = isSingleValue;
    _dictionary = dictionary;
    _dictionaryEncoded = dictionaryEncoded;
    _dataSource = dataSource;
  }

  public DataType getDataType() {
    return _dataType;
  }

  public boolean isSingleValue() {
    return _isSingleValue;
  }

  /// Returns the column's dictionary file if one exists, regardless of whether the forward index can answer
  /// dictionary-id reads. Callers that need to select between a dict-id read path and a value read path MUST gate
  /// on {@link #isDictionaryEncoded()} rather than {@code getDictionary() != null} — a column declared as
  /// {@code EncodingType.RAW} with an explicit {@code dictionaryIndex} returns a non-null dictionary here but its
  /// forward index throws on {@link ForwardIndexReader#readDictIds}.
  @Nullable
  public Dictionary getDictionary() {
    return _dictionary;
  }

  /// Returns {@code true} if the column's forward index is dictionary-encoded and the dict-id read path
  /// ({@link org.apache.pinot.core.common.BlockValSet#getDictionaryIdsSV()}) is callable. A column with
  /// {@code EncodingType.RAW} + an explicit {@code dictionaryIndex} returns {@code false} here even though
  /// {@link #getDictionary()} is non-null.
  public boolean isDictionaryEncoded() {
    return _dictionaryEncoded;
  }

  @Nullable
  public DataSource getDataSource() {
    return _dataSource;
  }

  public static ColumnContext fromDataSource(DataSource dataSource) {
    DataSourceMetadata dataSourceMetadata = dataSource.getDataSourceMetadata();
    Dictionary dictionary = dataSource.getDictionary();
    ForwardIndexReader<?> forwardIndex = dataSource.getForwardIndex();
    // Dict-id reads require both a dictionary AND a dict-encoded forward index. A column with EncodingType.RAW +
    // dictionaryIndex has the dictionary but a RAW forward index; a column with a disabled forward index (dict +
    // inverted/range only) has no forward index at all. Both must take the value/index-based path.
    boolean dictEncoded = dictionary != null && forwardIndex != null && forwardIndex.isDictionaryEncoded();
    return new ColumnContext(dataSourceMetadata.getDataType(), dataSourceMetadata.isSingleValue(), dictionary,
        dictEncoded, dataSource);
  }

  public static ColumnContext fromTransformFunction(TransformFunction transformFunction) {
    TransformResultMetadata resultMetadata = transformFunction.getResultMetadata();
    Dictionary dictionary = transformFunction.getDictionary();
    // Transform functions that expose a dictionary always build it themselves, so the dict-id read path is callable
    // whenever the dictionary is present.
    return new ColumnContext(resultMetadata.getDataType(), resultMetadata.isSingleValue(), dictionary,
        dictionary != null, null);
  }
}
