/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
import org.testng.Assert;


public abstract class RecordReaderTest {
  protected static final String[] COLUMNS = {"INT_SV", "INT_MV"};
  protected static final Schema SCHEMA = new Schema.SchemaBuilder().addMetric(COLUMNS[0], FieldSpec.DataType.INT)
      .addMultiValueDimension(COLUMNS[1], FieldSpec.DataType.INT, -1)
      .build();
  protected static final Object[][] RECORDS = {{5, new int[]{10, 15, 20}}, {25, new int[]{30, 35, 40}}, {null, null}};
  private static final Object[] DEFAULT_VALUES = {0, new int[]{-1}};

  protected static void checkValue(RecordReader recordReader) throws Exception {
    for (Object[] expectedRecord : RECORDS) {
      GenericRow actualRecord = recordReader.next();
      int numColumns = COLUMNS.length;
      for (int i = 0; i < numColumns; i++) {
        if (expectedRecord[i] != null) {
          Assert.assertEquals(actualRecord.getValue(COLUMNS[i]), expectedRecord[i]);
        } else {
          Assert.assertEquals(actualRecord.getValue(COLUMNS[i]), DEFAULT_VALUES[i]);
        }
      }
    }
    Assert.assertFalse(recordReader.hasNext());
  }
}