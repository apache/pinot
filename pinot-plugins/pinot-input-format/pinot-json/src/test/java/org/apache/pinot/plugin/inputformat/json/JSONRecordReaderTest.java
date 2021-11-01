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
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.AbstractRecordReaderTest;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


// TODO DDC adding conversion of all JSON numeric values to BigDecimal breaks this test. Needs fixing
public class JSONRecordReaderTest extends AbstractRecordReaderTest {
  private final File _dateFile = new File(_tempDir, "data.json");

  @Override
  protected RecordReader createRecordReader()
      throws Exception {
    JSONRecordReader recordReader = new JSONRecordReader();
    recordReader.init(_dateFile, _sourceFields, null);
    return recordReader;
  }

  @Override
  protected void writeRecordsToFile(List<Map<String, Object>> recordsToWrite)
      throws Exception {
    try (FileWriter fileWriter = new FileWriter(_dateFile)) {
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
  protected void checkValue(RecordReader recordReader, List<Map<String, Object>> expectedRecordsMap,
      List<Object[]> expectedPrimaryKeys)
      throws Exception {
    for (int i = 0; i < expectedRecordsMap.size(); i++) {
      Map<String, Object> expectedRecord = expectedRecordsMap.get(i);
      GenericRow actualRecord = recordReader.next();
      for (FieldSpec fieldSpec : _pinotSchema.getAllFieldSpecs()) {
        String fieldSpecName = fieldSpec.getName();
        if (fieldSpec.isSingleValueField()) {
          Assert.assertEquals(actualRecord.getValue(fieldSpecName).toString(),
              expectedRecord.get(fieldSpecName).toString());
        } else {
          Object[] actualRecords = (Object[]) actualRecord.getValue(fieldSpecName);
          List expectedRecords = (List) expectedRecord.get(fieldSpecName);
          Assert.assertEquals(actualRecords.length, expectedRecords.size());
          for (int j = 0; j < actualRecords.length; j++) {
            Assert.assertEquals(actualRecords[j].toString(), expectedRecords.get(j).toString());
          }
        }
      }
      PrimaryKey primaryKey = actualRecord.getPrimaryKey(getPrimaryKeyColumns());
      Assert.assertEquals(primaryKey.getValues(), expectedPrimaryKeys.get(i));
    }
    Assert.assertFalse(recordReader.hasNext());
  }
}
