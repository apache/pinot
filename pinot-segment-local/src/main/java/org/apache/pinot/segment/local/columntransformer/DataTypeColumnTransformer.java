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
package org.apache.pinot.segment.local.columntransformer;

import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.common.utils.ThrottledLogger;
import org.apache.pinot.segment.local.utils.DataTypeTransformerUtils;
import org.apache.pinot.spi.columntransformer.ColumnTransformer;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.ColumnReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DataTypeColumnTransformer implements ColumnTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataTypeColumnTransformer.class);

  private final PinotDataType _destDataType;
  private final ColumnReader _columnReader;
  private final boolean _continueOnError;
  private final ThrottledLogger _throttledLogger;

  /**
   * @param fieldSpec - The field spec for the column being created in Pinot.
   * @param columnReader - The column reader to read the source data.
   */
  public DataTypeColumnTransformer(TableConfig tableConfig, FieldSpec fieldSpec, ColumnReader columnReader) {
    _destDataType = PinotDataType.getPinotDataTypeForIngestion(fieldSpec);
    _columnReader = columnReader;
    IngestionConfig ingestionConfig = tableConfig.getIngestionConfig();
    _continueOnError = ingestionConfig != null && ingestionConfig.isContinueOnError();
    _throttledLogger = new ThrottledLogger(LOGGER, ingestionConfig);
  }

  @Override
  public boolean isNoOp() {
    // If source and destination data types are primitive types and the same, no transformation is needed.
    if (_columnReader.isSingleValue()) {
      if (_columnReader.isInt()) {
        return _destDataType.equals(PinotDataType.INTEGER);
      } else if (_columnReader.isLong()) {
        return _destDataType.equals(PinotDataType.LONG);
      } else if (_columnReader.isFloat()) {
        return _destDataType.equals(PinotDataType.FLOAT);
      } else if (_columnReader.isDouble()) {
        return _destDataType.equals(PinotDataType.DOUBLE);
      } else if (_columnReader.isString()) {
        return _destDataType.equals(PinotDataType.STRING);
      }
    } else {
      if (_columnReader.isInt()) {
        return _destDataType.equals(PinotDataType.INTEGER_ARRAY);
      } else if (_columnReader.isLong()) {
        return _destDataType.equals(PinotDataType.LONG_ARRAY);
      } else if (_columnReader.isFloat()) {
        return _destDataType.equals(PinotDataType.FLOAT_ARRAY);
      } else if (_columnReader.isDouble()) {
        return _destDataType.equals(PinotDataType.DOUBLE_ARRAY);
      } else if (_columnReader.isString()) {
        return _destDataType.equals(PinotDataType.STRING_ARRAY);
      }
    }
    // For other types, because there is no overhead to cast to Object, always call transform() which handles all cases
    return false;
  }

  @Override
  public Object transform(Object value) {
    String columnName = _columnReader.getColumnName();
    try {
      return DataTypeTransformerUtils.transformValue(columnName, value, _destDataType);
    } catch (Exception e) {
      if (!_continueOnError) {
        throw new RuntimeException("Caught exception while transforming data type for column: " + columnName, e);
      }
      _throttledLogger.warn("Caught exception while transforming data type for column: " + columnName, e);
      return null;
    }
  }
}
