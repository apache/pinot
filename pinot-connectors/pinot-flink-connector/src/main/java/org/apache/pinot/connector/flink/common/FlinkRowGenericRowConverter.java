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
package org.apache.pinot.connector.flink.common;

import java.util.Map;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.pinot.spi.data.readers.GenericRow;


/** Converts {@link Row} type data into {@link GenericRow} format. */
public class FlinkRowGenericRowConverter implements PinotGenericRowConverter<Row> {

  private final RowTypeInfo _rowTypeInfo;
  private final String[] _fieldNames;

  public FlinkRowGenericRowConverter(RowTypeInfo rowTypeInfo) {
    _rowTypeInfo = rowTypeInfo;
    _fieldNames = rowTypeInfo.getFieldNames();
  }

  @Override
  public GenericRow convertToRow(Row value) {
    GenericRow row = new GenericRow();
    for (int i = 0; i < value.getArity(); i++) {
      row.putValue(_fieldNames[i], value.getField(i));
    }
    return row;
  }

  @Override
  public Row convertFromRow(GenericRow row) {
    Row value = new Row(_fieldNames.length);
    for (Map.Entry<String, Object> e : row.getFieldToValueMap().entrySet()) {
      value.setField(_rowTypeInfo.getFieldIndex(e.getKey()), e.getValue());
    }
    return value;
  }
}
