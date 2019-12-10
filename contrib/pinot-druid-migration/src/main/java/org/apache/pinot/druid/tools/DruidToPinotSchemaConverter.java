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
package org.apache.pinot.druid.tools;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.Indexed;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.Schema;


/**
 * The DruidToPinotSchemaConverter is a tool that takes a Druid segment and creates a Pinot schema with the
 * segment's information.
 */
public class DruidToPinotSchemaConverter {
  // TODO: Consider adding "columnsToInclude"
  //  - for example, if you don't want "count" in the schema, you can specify the columns you do/don't want to include
  public static Schema createSchema(String schemaName, String druidSegmentPath)
      throws IOException {
    File druidSegment = new File(druidSegmentPath);
    QueryableIndex index = DruidSegmentUtils.createIndex(druidSegment);
    QueryableIndexStorageAdapter adapter = new QueryableIndexStorageAdapter(index);

    Schema.SchemaBuilder schemaBuilder = new Schema.SchemaBuilder();
    schemaBuilder.setSchemaName(schemaName);

    Indexed<String> dimensions = adapter.getAvailableDimensions();
    Iterable<String> metrics = adapter.getAvailableMetrics();

    for (String dimension : dimensions) {
      ColumnCapabilities columnCapabilities = adapter.getColumnCapabilities(dimension);
      boolean isSingleValueField = !columnCapabilities.hasMultipleValues();
      try {
        FieldSpec.DataType type = getPinotDataType(columnCapabilities.getType());
        if (isSingleValueField) {
          schemaBuilder.addSingleValueDimension(dimension, type, null);
        } else {
          schemaBuilder.addMultiValueDimension(dimension, type, null);
        }

      } catch(UnsupportedOperationException e) {
        System.out.println(e.getMessage() + "; Skipping column " + dimension);
      }
    }

    for (String metric : metrics) {
      ColumnCapabilities columnCapabilities = adapter.getColumnCapabilities(metric);
      try {
        FieldSpec.DataType type = getPinotDataType(columnCapabilities.getType());
        schemaBuilder.addMetric(metric, type);
      } catch(UnsupportedOperationException e) {
        System.out.println(e.getMessage() + "; Skipping column " + metric);
      }
    }
    // Both ingoing and outgoing granularitySpec are the same
    schemaBuilder.addTime(ColumnHolder.TIME_COLUMN_NAME, TimeUnit.MILLISECONDS, FieldSpec.DataType.LONG,
                          ColumnHolder.TIME_COLUMN_NAME, TimeUnit.MILLISECONDS, FieldSpec.DataType.LONG);
    return schemaBuilder.build();
  }

  private static FieldSpec.DataType getPinotDataType(ValueType druidType) {
    switch (druidType) {
      case STRING:
        return FieldSpec.DataType.STRING;
      case LONG:
        return FieldSpec.DataType.LONG;
      case FLOAT:
        return FieldSpec.DataType.FLOAT;
      case DOUBLE:
        return FieldSpec.DataType.DOUBLE;
      case COMPLEX:
      default:
        throw new UnsupportedOperationException("Pinot does not support Druid ValueType " + druidType.name());
    }
  }
}
