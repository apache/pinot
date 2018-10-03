/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.datasource.pinot.resultset;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ThirdEyeResultSetSerializer extends StdSerializer<ThirdEyeResultSet> {
  public static final String GROUP_COLUMN_NAMES_FIELD = "groupColumnNames";
  public static final String METRIC_COLUMN_NAMES_FIELD = "metricColumnNames";
  public static final String ROWS_FIELD = "rows";

  public ThirdEyeResultSetSerializer() {
    this(null);
  }

  public ThirdEyeResultSetSerializer(Class<ThirdEyeResultSet> t) {
    super(t);
  }

  @Override
  public void serialize(ThirdEyeResultSet thirdEyeResultSet, JsonGenerator jsonGenerator,
      SerializerProvider serializerProvider) throws IOException {
    jsonGenerator.writeStartObject();

    // Metadata: group column names
    int groupColumnCount = thirdEyeResultSet.getGroupKeyLength();
    List<String> groupColumnNames = new ArrayList<>(groupColumnCount);
    for (int idx = 0; idx < groupColumnCount; idx++) {
      groupColumnNames.add(thirdEyeResultSet.getGroupKeyColumnName(idx));
    }
    jsonGenerator.writeObjectField(GROUP_COLUMN_NAMES_FIELD, groupColumnNames);
    // Metadata: metric column names
    int metricColumnCount = thirdEyeResultSet.getColumnCount();
    List<String> metricColumnNames = new ArrayList<>(metricColumnCount);
    for (int idx = 0; idx < metricColumnCount; idx++) {
      metricColumnNames.add(thirdEyeResultSet.getColumnName(idx));
    }
    jsonGenerator.writeObjectField(METRIC_COLUMN_NAMES_FIELD, metricColumnNames);
    // Data: data in rows of Strings
    int rowCount = thirdEyeResultSet.getRowCount();
    List<String[]> rows = new ArrayList<>();
    int totalColumnCount = groupColumnCount + metricColumnCount;
    for (int rowIdx = 0; rowIdx < rowCount; rowIdx++) {
      String[] newRow = new String[totalColumnCount];
      rows.add(newRow);
      for (int columnIdx = 0; columnIdx < groupColumnCount; columnIdx++) {
        newRow[columnIdx] = thirdEyeResultSet.getGroupKeyColumnValue(rowIdx, columnIdx);
      }
      for (int columnIdx = 0; columnIdx < metricColumnCount; columnIdx++) {
        newRow[columnIdx + groupColumnCount] = thirdEyeResultSet.getString(rowIdx, columnIdx);
      }
    }
    jsonGenerator.writeObjectField(ROWS_FIELD, rows);
    jsonGenerator.writeEndObject();
  }
}
