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
package com.linkedin.pinot.core.data.readers;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.RandomStringUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class LimitStringLengthRecordReaderTest {
  private static final String SV_COLUMN = "testSVColumn";
  private static final String MV_COLUMN = "testMVColumn";
  private static final int LENGTH_LIMIT = 10;
  private static final int MAX_LENGTH = 20;
  private static final int NUM_ROWS = 100;
  private static final int NUM_MULTI_VALUES = 5;

  @Test
  public void testTrimString() throws IOException {
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension(SV_COLUMN, FieldSpec.DataType.STRING)
        .addMultiValueDimension(MV_COLUMN, FieldSpec.DataType.STRING)
        .build();
    schema.getFieldSpecFor(SV_COLUMN).setStringLengthLimit(LENGTH_LIMIT);
    schema.getFieldSpecFor(MV_COLUMN).setStringLengthLimit(LENGTH_LIMIT);

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    long expectedNumStringsTrimmed = 0;
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = new GenericRow();
      String singleValue = RandomStringUtils.random(MAX_LENGTH);
      if (singleValue.length() > LENGTH_LIMIT) {
        expectedNumStringsTrimmed++;
      }
      row.putField(SV_COLUMN, singleValue);
      Object[] multiValue = new Object[NUM_MULTI_VALUES];
      for (int j = 0; j < NUM_MULTI_VALUES; j++) {
        String value = RandomStringUtils.random(MAX_LENGTH);
        if (value.length() > LENGTH_LIMIT) {
          expectedNumStringsTrimmed++;
        }
        multiValue[j] = value;
      }
      row.putField(MV_COLUMN, multiValue);

      rows.add(row);
    }

    try (LimitStringLengthRecordReader recordReader = new LimitStringLengthRecordReader(
        new GenericRowRecordReader(rows, schema))) {
      while (recordReader.hasNext()) {
        GenericRow next = recordReader.next();
        assertTrue(((String) next.getValue(SV_COLUMN)).length() <= LENGTH_LIMIT);
        Object[] multiValue = (Object[]) next.getValue(MV_COLUMN);
        assertEquals(multiValue.length, NUM_MULTI_VALUES);
        for (int i = 0; i < NUM_MULTI_VALUES; i++) {
          assertTrue(((String) multiValue[i]).length() <= LENGTH_LIMIT);
        }
      }
      assertEquals(recordReader.getNumStringsTrimmed(), expectedNumStringsTrimmed);
    }
  }
}
