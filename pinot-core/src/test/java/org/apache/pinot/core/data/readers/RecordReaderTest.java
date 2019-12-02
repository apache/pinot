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
package org.apache.pinot.core.data.readers;

import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.core.data.recordtransformer.CompositeTransformer;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.testng.Assert;


public abstract class RecordReaderTest {
  protected static final String[] COLUMNS = {"INT_SV", "INT_MV"};
  protected static final Schema SCHEMA = new Schema.SchemaBuilder().addMetric(COLUMNS[0], FieldSpec.DataType.INT)
      .addMultiValueDimension(COLUMNS[1], FieldSpec.DataType.INT, -1).build();
  protected static final Object[][] RECORDS = {{5, new int[]{10, 15, 20}}, {25, new int[]{30, 35, 40}}, {null, null}};
  private static final Object[] DEFAULT_VALUES = {0, new int[]{-1}};

  protected static void checkValue(RecordReader recordReader)
      throws Exception {
    CompositeTransformer defaultTransformer = CompositeTransformer.getDefaultTransformer(SCHEMA);
    for (Object[] expectedRecord : RECORDS) {
      GenericRow actualRecord = recordReader.next();
      GenericRow transformedRecord = defaultTransformer.transform(actualRecord);

      int numColumns = COLUMNS.length;
      for (int i = 0; i < numColumns; i++) {
        if (expectedRecord[i] != null) {
          Assert.assertEquals(transformedRecord.getValue(COLUMNS[i]), expectedRecord[i]);
        } else {
          Assert.assertEquals(transformedRecord.getValue(COLUMNS[i]), DEFAULT_VALUES[i]);
        }
      }
    }
    Assert.assertFalse(recordReader.hasNext());
  }
}
