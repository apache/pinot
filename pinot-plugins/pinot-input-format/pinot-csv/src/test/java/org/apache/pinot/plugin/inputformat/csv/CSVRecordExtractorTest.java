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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.data.readers.AbstractRecordExtractorTest;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests the {@link CSVRecordExtractor} using a schema containing groovy transform functions
 */
public class CSVRecordExtractorTest extends AbstractRecordExtractorTest {
  private static final char CSV_MULTI_VALUE_DELIMITER = ';';
  private final File _dataFile = new File(_tempDir, "events.csv");

  /**
   * Create a CSVRecordReader
   */
  @Override
  protected RecordReader createRecordReader(Set<String> fieldsToRead)
      throws IOException {
    CSVRecordReaderConfig csvRecordReaderConfig = new CSVRecordReaderConfig();
    csvRecordReaderConfig.setMultiValueDelimiter(CSV_MULTI_VALUE_DELIMITER);
    CSVRecordReader csvRecordReader = new CSVRecordReader();
    csvRecordReader.init(_dataFile, fieldsToRead, csvRecordReaderConfig);
    return csvRecordReader;
  }

  /**
   * Create a CSV input file using the input records
   */
  @Override
  public void createInputFile()
      throws IOException {
    String[] header = _sourceFieldNames.toArray(new String[0]);
    try (FileWriter fileWriter = new FileWriter(_dataFile); CSVPrinter csvPrinter = new CSVPrinter(fileWriter,
        CSVFormat.DEFAULT.withHeader(header))) {

      for (Map<String, Object> inputRecord : _inputRecords) {
        Object[] record = new Object[header.length];
        for (int i = 0; i < header.length; i++) {
          Object value = inputRecord.get(header[i]);
          if (value instanceof Collection) {
            record[i] = StringUtils.join(((List) value).toArray(), CSV_MULTI_VALUE_DELIMITER);
          } else {
            record[i] = value;
          }
        }
        csvPrinter.printRecord(record);
      }
    }
  }

  @Override
  protected void checkValue(Map<String, Object> inputRecord, GenericRow genericRow) {
    for (Map.Entry<String, Object> entry : inputRecord.entrySet()) {
      String columnName = entry.getKey();
      Object expectedValue = entry.getValue();
      Object actualValue = genericRow.getValue(columnName);
      if (expectedValue instanceof Collection) {
        List expectedArray = (List) expectedValue;
        if (expectedArray.size() == 1) {
          // in CSV, cannot differentiate between array with single element vs actual single element
          Assert.assertEquals(actualValue, String.valueOf(expectedArray.get(0)));
        } else {
          Object[] actualArray = (Object[]) actualValue;
          for (int j = 0; j < actualArray.length; j++) {
            Assert.assertEquals(actualArray[j], String.valueOf(expectedArray.get(j)));
          }
        }
      } else {
        Assert.assertEquals(actualValue, expectedValue == null ? null : String.valueOf(expectedValue));
      }
    }
  }

  /**
   * Check if we can parse a CSV file that has escaped comma characters within fields.
   */
  @Test
  public void testEscapeCharacterInCSV()
    throws Exception {
    // Create CSV config with backslash as escape character.
    CSVRecordReaderConfig csvRecordReaderConfig = new CSVRecordReaderConfig();
    csvRecordReaderConfig.setEscapeCharacter('\\');

    // Create a CSV file where records have two values and the second value contains an escaped comma.
    File escapedFile = new File(_tempDir, "escape.csv");
    BufferedWriter writer = new BufferedWriter(new FileWriter(escapedFile));
    writer.write("first,second\n");
    writer.write("string1, string2\\, string3");
    writer.close();

    // Try to parse CSV file with escaped comma.
    CSVRecordReader csvRecordReader = new CSVRecordReader();
    HashSet<String> fieldsToRead = new HashSet<>();
    fieldsToRead.add("first");
    fieldsToRead.add("second");
    csvRecordReader.init(escapedFile, fieldsToRead, csvRecordReaderConfig);
    GenericRow genericRow = new GenericRow();
    csvRecordReader.rewind();

    // check if parsing succeeded.
    Assert.assertTrue(csvRecordReader.hasNext());
    csvRecordReader.next(genericRow);
    Assert.assertEquals(genericRow.getValue("first"), "string1");
    Assert.assertEquals(genericRow.getValue("second"), " string2, string3");
  }
}
