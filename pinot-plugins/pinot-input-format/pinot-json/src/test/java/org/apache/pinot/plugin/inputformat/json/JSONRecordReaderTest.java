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
package org.apache.pinot.plugin.inputformat.json;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.FileWriter;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.AbstractRecordReaderTest;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class JSONRecordReaderTest extends AbstractRecordReaderTest {
  private static final String PRECISE_DECIMAL = "12345678901234567890.12345678901234567890";
  private static final double EPSILON = 1e-6d;

  @Override
  protected RecordReader createRecordReader(File file)
      throws Exception {
    JSONRecordReader recordReader = new JSONRecordReader();
    recordReader.init(file, _sourceFields, null);
    return recordReader;
  }

  @Override
  protected void writeRecordsToFile(List<Map<String, Object>> recordsToWrite)
      throws Exception {
    try (FileWriter fileWriter = new FileWriter(_dataFile)) {
      for (Map<String, Object> r : recordsToWrite) {
        ObjectNode jsonRecord = JsonUtils.newObjectNode();
        for (String key : r.keySet()) {
          jsonRecord.set(key, JsonUtils.objectToJsonNode(r.get(key)));
        }
        fileWriter.write(jsonRecord.toString());
      }
    }
  }

  @Override
  protected String getDataFileName() {
    return "data.json";
  }

  @Test
  public void testRecordReaderPreservesBigDecimalPrecision()
      throws Exception {
    File dataFile = new File(_tempDir, "big_decimal_data.json");
    try (FileWriter fileWriter = new FileWriter(dataFile)) {
      fileWriter.write("{\"decimalMetric\":" + PRECISE_DECIMAL + ",\"doubleMetric\":1.25}");
    }

    try (JSONRecordReader recordReader = new JSONRecordReader()) {
      recordReader.init(dataFile, Set.of("decimalMetric", "doubleMetric"), null);
      GenericRow actualRecord = recordReader.next();

      Assert.assertEquals(actualRecord.getValue("decimalMetric"), new BigDecimal(PRECISE_DECIMAL));
      Assert.assertEquals(((BigDecimal) actualRecord.getValue("doubleMetric")).doubleValue(), 1.25d);
      Assert.assertFalse(recordReader.hasNext());
    }
  }

  @Override
  protected void checkValue(RecordReader recordReader, List<Map<String, Object>> expectedRecordsMap,
      List<Object[]> expectedPrimaryKeys)
      throws Exception {
    for (int i = 0; i < expectedRecordsMap.size(); i++) {
      Map<String, Object> expectedRecord = expectedRecordsMap.get(i);
      GenericRow actualRecord = recordReader.next();
      for (FieldSpec fieldSpec : _pinotSchema.getAllFieldSpecs()) {
        String fieldSpecName = fieldSpec.getName();
        if (fieldSpec.isSingleValueField()) {
          assertRecordValueEquals(actualRecord.getValue(fieldSpecName), expectedRecord.get(fieldSpecName));
        } else {
          Object[] actualRecords = (Object[]) actualRecord.getValue(fieldSpecName);
          List expectedRecords = (List) expectedRecord.get(fieldSpecName);
          Assert.assertEquals(actualRecords.length, expectedRecords.size());
          for (int j = 0; j < actualRecords.length; j++) {
            assertRecordValueEquals(actualRecords[j], expectedRecords.get(j));
          }
        }
      }
      PrimaryKey primaryKey = actualRecord.getPrimaryKey(getPrimaryKeyColumns());
      Assert.assertEquals(primaryKey.getValues(), expectedPrimaryKeys.get(i));
    }
    Assert.assertFalse(recordReader.hasNext());
  }

  private static void assertRecordValueEquals(Object actualValue, Object expectedValue) {
    boolean isFloating = expectedValue instanceof Float || expectedValue instanceof Double
        || actualValue instanceof Float || actualValue instanceof Double
        || (actualValue instanceof BigDecimal && (expectedValue instanceof Float || expectedValue instanceof Double));
    if (isFloating) {
      Assert.assertEquals(((Number) actualValue).doubleValue(), ((Number) expectedValue).doubleValue(), EPSILON);
    } else {
      Assert.assertEquals(actualValue.toString(), expectedValue.toString());
    }
  }
}
