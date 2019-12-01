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

import java.io.File;
import java.io.FileWriter;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.AbstractRecordReaderTest;
import org.apache.pinot.spi.data.readers.RecordReader;


public class CSVRecordReaderTest extends AbstractRecordReaderTest {
  private static char CSV_MULTI_VALUE_DELIMITER = '\t';
  private final File _dataFile = new File(_tempDir, "data.csv");

  @Override
  protected RecordReader createRecordReader()
      throws Exception {
    CSVRecordReaderConfig csvRecordReaderConfig = new CSVRecordReaderConfig();
    csvRecordReaderConfig.setMultiValueDelimiter(CSV_MULTI_VALUE_DELIMITER);
    return new CSVRecordReader(_dataFile, getPinotSchema(), csvRecordReaderConfig);
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
}
