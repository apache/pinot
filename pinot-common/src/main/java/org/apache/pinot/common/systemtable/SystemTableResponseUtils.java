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
package org.apache.pinot.common.systemtable;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/**
 * Utilities for building broker responses for system table providers.
 */
public final class SystemTableResponseUtils {
  private SystemTableResponseUtils() {
  }

  public static BrokerResponseNative buildBrokerResponse(String tableName, Schema schema,
      List<String> projectionColumns, List<GenericRow> rows, int totalRows) {
    DataSchema dataSchema = buildDataSchema(schema, projectionColumns);
    List<Object[]> resultRows = new ArrayList<>();
    for (GenericRow row : rows) {
      Object[] values = new Object[projectionColumns.size()];
      for (int i = 0; i < projectionColumns.size(); i++) {
        values[i] = row.getValue(projectionColumns.get(i));
      }
      resultRows.add(values);
    }
    BrokerResponseNative brokerResponse = new BrokerResponseNative();
    brokerResponse.setResultTable(new ResultTable(dataSchema, resultRows));
    brokerResponse.setNumDocsScanned(resultRows.size());
    brokerResponse.setNumEntriesScannedPostFilter(totalRows);
    brokerResponse.setTotalDocs(totalRows);
    brokerResponse.setTablesQueried(Set.of(TableNameBuilder.extractRawTableName(tableName)));
    return brokerResponse;
  }

  private static DataSchema buildDataSchema(Schema schema, List<String> projectionColumns) {
    String[] columnNames = new String[projectionColumns.size()];
    DataSchema.ColumnDataType[] columnTypes = new DataSchema.ColumnDataType[projectionColumns.size()];
    for (int i = 0; i < projectionColumns.size(); i++) {
      String column = projectionColumns.get(i);
      FieldSpec fieldSpec = schema.getFieldSpecFor(column);
      DataSchema.ColumnDataType columnDataType =
          fieldSpec != null
              ? DataSchema.ColumnDataType.fromDataType(fieldSpec.getDataType(), fieldSpec.isSingleValueField())
              : DataSchema.ColumnDataType.STRING;
      columnNames[i] = column;
      columnTypes[i] = columnDataType;
    }
    return new DataSchema(columnNames, columnTypes);
  }
}
