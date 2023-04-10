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
    return new ColumnContext(dataSourceMetadata.getDataType(), dataSourceMetadata.isSingleValue(),
        dataSource.getDictionary(), dataSource);
  }

  public static ColumnContext fromTransformFunction(TransformFunction transformFunction) {
    TransformResultMetadata resultMetadata = transformFunction.getResultMetadata();
    return new ColumnContext(resultMetadata.getDataType(), resultMetadata.isSingleValue(),
        transformFunction.getDictionary(), null);
  }
}
