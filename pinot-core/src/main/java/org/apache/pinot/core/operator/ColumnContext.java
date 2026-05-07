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
  private final DataSource _dataSource;

  private ColumnContext(DataType dataType, boolean isSingleValue, @Nullable Dictionary dictionary,
      @Nullable DataSource dataSource) {
    _dataType = dataType;
    _isSingleValue = isSingleValue;
    _dictionary = dictionary;
    _dataSource = dataSource;
  }

  public DataType getDataType() {
    return _dataType;
  }

  public boolean isSingleValue() {
    return _isSingleValue;
  }

  @Nullable
  public Dictionary getDictionary() {
    return _dictionary;
  }

  @Nullable
  public DataSource getDataSource() {
    return _dataSource;
  }

  public static ColumnContext fromDataSource(DataSource dataSource) {
    DataSourceMetadata dataSourceMetadata = dataSource.getDataSourceMetadata();
    // Only expose the dictionary when dict-id reads from the forward index are cheap. Columns with a shared
    // standalone dictionary on a RAW forward index would otherwise force every dict-id consumer (group key
    // generators, distinct executors, identifier transforms) into per-row Dictionary#indexOf lookups; hiding
    // the dictionary at the source routes them all to the raw-value path uniformly. Callers that legitimately
    // need the dictionary (e.g. iterating it directly in DictionaryBasedDistinctOperator) can still get it
    // via getDataSource().getDictionary().
    ForwardIndexReader<?> forwardIndex = dataSource.getForwardIndex();
    Dictionary dictionary = forwardIndex != null && forwardIndex.isDictionaryEncoded()
        ? dataSource.getDictionary() : null;
    return new ColumnContext(dataSourceMetadata.getDataType(), dataSourceMetadata.isSingleValue(),
        dictionary, dataSource);
  }

  public static ColumnContext fromTransformFunction(TransformFunction transformFunction) {
    TransformResultMetadata resultMetadata = transformFunction.getResultMetadata();
    return new ColumnContext(resultMetadata.getDataType(), resultMetadata.isSingleValue(),
        transformFunction.getDictionary(), null);
  }
}
