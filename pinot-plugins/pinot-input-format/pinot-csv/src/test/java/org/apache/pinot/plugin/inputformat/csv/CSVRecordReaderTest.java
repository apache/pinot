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
package org.apache.pinot.plugin.inputformat.csv;

import java.io.File;
import java.io.FileWriter;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.AbstractRecordReaderTest;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractor;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.testng.Assert;


public class CSVRecordReaderTest extends AbstractRecordReaderTest {
  private static char CSV_MULTI_VALUE_DELIMITER = '\t';
  private final File _dataFile = new File(_tempDir, "data.csv");

  @Override
  protected RecordReader createRecordReader(RecordReaderConfig readerConfig)
      throws Exception {
    CSVRecordReader csvRecordReader = new CSVRecordReader();
    csvRecordReader.init(_dataFile, getPinotSchema(), readerConfig);
    return csvRecordReader;
  }

  @Override
  protected RecordReaderConfig createRecordReaderConfig() {
    CSVRecordReaderConfig csvRecordReaderConfig = new CSVRecordReaderConfig();
    csvRecordReaderConfig.setMultiValueDelimiter(CSV_MULTI_VALUE_DELIMITER);
    return csvRecordReaderConfig;
  }

  @Override
  protected void writeRecordsToFile(List<Map<String, Object>> recordsToWrite)
      throws Exception {

    Schema pinotSchema = getPinotSchema();
    String[] columns = pinotSchema.getColumnNames().toArray(new String[0]);
    try (FileWriter fileWriter = new FileWriter(_dataFile);
        CSVPrinter csvPrinter = new CSVPrinter(fileWriter, CSVFormat.DEFAULT.withHeader(columns))) {

      for (Map<String, Object> r : recordsToWrite) {
        Object[] record = new Object[columns.length];
        for (int i = 0; i < columns.length; i++) {
          if (pinotSchema.getFieldSpecFor(columns[i]).isSingleValueField()) {
            record[i] = r.get(columns[i]);
          } else {
            record[i] = StringUtils.join(((List) r.get(columns[i])).toArray(), CSV_MULTI_VALUE_DELIMITER);
          }
        }
        csvPrinter.printRecord(record);
      }
    }
  }

  @Override
  protected void checkValue(RecordReader recordReader, RecordExtractor recordExtractor, List<Map<String, Object>> expectedRecordsMap)
      throws Exception {
    GenericRow reuse = new GenericRow();
    for (Map<String, Object> expectedRecord : expectedRecordsMap) {
      Object next = recordReader.next();
      GenericRow actualRecord = recordExtractor.extract(next, reuse);
      org.apache.pinot.spi.data.Schema pinotSchema = recordReader.getSchema();
      for (FieldSpec fieldSpec : pinotSchema.getAllFieldSpecs()) {
        String fieldSpecName = fieldSpec.getName();
        if (fieldSpec.isSingleValueField()) {
          Assert.assertEquals(actualRecord.getValue(fieldSpecName).toString(), expectedRecord.get(fieldSpecName).toString());
        } else {
          List expectedRecords = (List) expectedRecord.get(fieldSpecName);
          if (expectedRecords.size() == 1) {
            Assert.assertEquals(actualRecord.getValue(fieldSpecName).toString(), expectedRecords.get(0).toString());
          } else {
            Object[] actualRecords = (Object[]) actualRecord.getValue(fieldSpecName);
            Assert.assertEquals(actualRecords.length, expectedRecords.size());
            for (int i = 0; i < actualRecords.length; i++) {
              Assert.assertEquals(actualRecords[i].toString(), expectedRecords.get(i).toString());
            }
          }
        }
      }
    }
    Assert.assertFalse(recordReader.hasNext());
  }
}
