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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.AbstractRecordReaderTest;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class CSVRecordReaderTest extends AbstractRecordReaderTest {
  private static final char CSV_MULTI_VALUE_DELIMITER = '\t';
  private static final CSVRecordReaderConfig[] NULL_AND_EMPTY_CONFIGS = new CSVRecordReaderConfig[]{
      null, new CSVRecordReaderConfig()
  };

  @Override
  protected RecordReader createRecordReader(File file)
      throws Exception {
    CSVRecordReaderConfig readerConfig = new CSVRecordReaderConfig();
    readerConfig.setMultiValueDelimiter(CSV_MULTI_VALUE_DELIMITER);
    CSVRecordReader recordReader = new CSVRecordReader();
    recordReader.init(file, _sourceFields, readerConfig);
    return recordReader;
  }

  @Override
  protected void writeRecordsToFile(List<Map<String, Object>> records)
      throws Exception {
    Schema schema = getPinotSchema();
    String[] columns = schema.getColumnNames().toArray(new String[0]);
    int numColumns = columns.length;
    try (CSVPrinter csvPrinter = new CSVPrinter(new FileWriter(_dataFile),
        CSVFormat.Builder.create().setHeader(columns).build())) {
      for (Map<String, Object> record : records) {
        Object[] values = new Object[numColumns];
        for (int i = 0; i < numColumns; i++) {
          if (schema.getFieldSpecFor(columns[i]).isSingleValueField()) {
            values[i] = record.get(columns[i]);
          } else {
            values[i] = StringUtils.join(((List<?>) record.get(columns[i])).toArray(), CSV_MULTI_VALUE_DELIMITER);
          }
        }
        csvPrinter.printRecord(values);
      }
    }
  }

  @Override
  protected String getDataFileName() {
    return "data.csv";
  }

  @Override
  protected void checkValue(RecordReader recordReader, List<Map<String, Object>> expectedRecords,
      List<Object[]> expectedPrimaryKeys)
      throws Exception {
    int numRecords = expectedRecords.size();
    for (int i = 0; i < numRecords; i++) {
      Map<String, Object> expectedRecord = expectedRecords.get(i);
      GenericRow actualRecord = recordReader.next();
      for (FieldSpec fieldSpec : _pinotSchema.getAllFieldSpecs()) {
        String column = fieldSpec.getName();
        if (fieldSpec.isSingleValueField()) {
          assertEquals(actualRecord.getValue(column).toString(), expectedRecord.get(column).toString());
        } else {
          List<?> expectedValues = (List<?>) expectedRecord.get(column);
          if (expectedValues.size() == 1) {
            assertEquals(actualRecord.getValue(column).toString(), expectedValues.get(0).toString());
          } else {
            Object[] actualValues = (Object[]) actualRecord.getValue(column);
            assertEquals(actualValues.length, expectedValues.size());
            for (int j = 0; j < actualValues.length; j++) {
              assertEquals(actualValues[j].toString(), expectedValues.get(j).toString());
            }
          }
        }
        Object[] expectedPrimaryKey = expectedPrimaryKeys.get(i);
        Object[] actualPrimaryKey = actualRecord.getPrimaryKey(getPrimaryKeyColumns()).getValues();
        for (int j = 0; j < actualPrimaryKey.length; j++) {
          assertEquals(actualPrimaryKey[j].toString(), expectedPrimaryKey[j].toString());
        }
      }
    }
    assertFalse(recordReader.hasNext());
  }

  @Test
  public void testInvalidDelimiterInHeader()
      throws IOException {
    CSVRecordReaderConfig readerConfig = new CSVRecordReaderConfig();
    readerConfig.setMultiValueDelimiter(CSV_MULTI_VALUE_DELIMITER);
    readerConfig.setHeader("col1;col2;col3;col4;col5;col6;col7;col8;col9;col10");
    try (CSVRecordReader recordReader = new CSVRecordReader()) {
      assertThrows(IllegalStateException.class, () -> recordReader.init(_dataFile, null, readerConfig));
    }
  }

  @Test
  public void testValidDelimiterInHeader()
      throws IOException {
    CSVRecordReaderConfig readerConfig = new CSVRecordReaderConfig();
    readerConfig.setMultiValueDelimiter(CSV_MULTI_VALUE_DELIMITER);
    readerConfig.setHeader("col1,col2,col3,col4,col5,col6,col7,col8,col9,col10");
    try (CSVRecordReader recordReader = new CSVRecordReader()) {
      recordReader.init(_dataFile, null, readerConfig);
      assertEquals(recordReader.getColumns(),
          List.of("col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9", "col10"));
      assertTrue(recordReader.hasNext());
    }
  }

  @Test
  public void testReadingDataFileBasic()
      throws IOException {
    File dataFile = getDataFile("dataFileBasic.csv");
    for (CSVRecordReaderConfig readerConfig : NULL_AND_EMPTY_CONFIGS) {
      validate(dataFile, readerConfig, List.of(
          createMap("id", "100", "name", "John"),
          createMap("id", "101", "name", "Jane"),
          createMap("id", "102", "name", "Alice"),
          createMap("id", "103", "name", "Bob")
      ));
    }
  }

  @Test
  public void testReadingDataFileWithSingleColumn()
      throws IOException {
    File dataFile = getDataFile("dataFileWithSingleColumn.csv");
    for (CSVRecordReaderConfig readerConfig : NULL_AND_EMPTY_CONFIGS) {
      validate(dataFile, readerConfig, List.of(
          createMap("name", "John"),
          createMap("name", "Jane"),
          createMap("name", "Jen")
      ));
    }

    CSVRecordReaderConfig readerConfig = new CSVRecordReaderConfig();
    readerConfig.setHeader("firstName,lastName,id");
    readerConfig.setSkipHeader(true);
    validate(dataFile, readerConfig, List.of(
        createMap("firstName", "John", "lastName", null, "id", null),
        createMap("firstName", "Jane", "lastName", null, "id", null),
        createMap("firstName", "Jen", "lastName", null, "id", null)
    ));
  }

  @Test
  public void testReadingDataFileWithInvalidHeader()
      throws IOException {
    File dataFile = getDataFile("dataFileWithInvalidHeader.csv");
    for (CSVRecordReaderConfig readerConfig : NULL_AND_EMPTY_CONFIGS) {
      try (CSVRecordReader recordReader = new CSVRecordReader()) {
        assertThrows(IllegalStateException.class, () -> recordReader.init(dataFile, null, readerConfig));
      }
    }

    CSVRecordReaderConfig readerConfig = new CSVRecordReaderConfig();
    readerConfig.setHeader("firstName,lastName,id");
    readerConfig.setSkipHeader(true);
    validate(dataFile, readerConfig, List.of(
        createMap("firstName", "John", "lastName", "Doe", "id", "100"),
        createMap("firstName", "Jane", "lastName", "Doe", "id", "101"),
        createMap("firstName", "Jen", "lastName", "Doe", "id", "102")
    ));
  }

  @Test
  public void testReadingDataFileWithAlternateDelimiter()
      throws IOException {
    File dataFile = getDataFile("dataFileWithAlternateDelimiter.csv");
    CSVRecordReaderConfig readerConfig = new CSVRecordReaderConfig();
    readerConfig.setDelimiter('|');
    validate(dataFile, readerConfig, List.of(
        createMap("id", "100", "firstName", "John", "lastName", "Doe"),
        createMap("id", "101", "firstName", "Jane", "lastName", "Doe"),
        createMap("id", "102", "firstName", "Jen", "lastName", "Doe")
    ));
  }

  @Test
  public void testReadingDataFileWithSurroundingSpaces()
      throws IOException {
    File dataFile = getDataFile("dataFileWithSurroundingSpaces.csv");
    for (CSVRecordReaderConfig readerConfig : NULL_AND_EMPTY_CONFIGS) {
      validate(dataFile, readerConfig, List.of(
          createMap("firstName", "John", "lastName", "Doe", "id", "100"),
          createMap("firstName", "Jane", "lastName", "Doe", "id", "101"),
          createMap("firstName", "Jen", "lastName", "Doe", "id", "102")
      ));
    }

    CSVRecordReaderConfig readerConfig = new CSVRecordReaderConfig();
    readerConfig.setIgnoreSurroundingSpaces(false);
    validate(dataFile, readerConfig, List.of(
        createMap(" firstName ", "John  ", " lastName ", " Doe", " id", "100"),
        createMap(" firstName ", "Jane", " lastName ", " Doe", " id", "  101"),
        createMap(" firstName ", "Jen", " lastName ", "Doe ", " id", "102")
    ));
  }

  @Test
  public void testReadingDataFileWithQuotes()
      throws IOException {
    File dataFile = getDataFile("dataFileWithQuotes.csv");
    for (CSVRecordReaderConfig readerConfig : NULL_AND_EMPTY_CONFIGS) {
      validate(dataFile, readerConfig, List.of(
          createMap("key", "key00", "num0", "12.3", "num1", "8.42"),
          createMap("key", "key01", "num0", null, "num1", "7.1"),
          createMap("key", "key02", "num0", null, "num1", "16.81"),
          createMap("key", "key03", "num0", null, "num1", "7.12")
      ));
    }
  }

  @Test
  public void testReadingDataFileWithCustomNull()
      throws IOException {
    File dataFile = getDataFile("dataFileWithCustomNull.csv");
    CSVRecordReaderConfig readerConfig = new CSVRecordReaderConfig();
    readerConfig.setNullStringValue("NULL");
    validate(dataFile, readerConfig, List.of(
        createMap("id", "100", "name", null),
        createMap("id", null, "name", "Jane"),
        createMap("id", null, "name", null),
        createMap("id", null, "name", null)
    ));
  }

  @Test
  public void testReadingDataFileWithCommentedLines()
      throws IOException {
    File dataFile = getDataFile("dataFileWithCommentedLines.csv");
    for (CSVRecordReaderConfig readerConfig : NULL_AND_EMPTY_CONFIGS) {
      // Verify first row
      validate(dataFile, readerConfig, 5, List.of(createMap("id", "# ignore line#1", "name", null)));
    }

    CSVRecordReaderConfig readerConfig = new CSVRecordReaderConfig();
    readerConfig.setCommentMarker('#');
    validate(dataFile, readerConfig, List.of(
        createMap("id", "100", "name", "Jane"),
        createMap("id", "101", "name", "John"),
        createMap("id", "102", "name", "Sam")
    ));
  }

  @Test
  public void testReadingDataFileWithEmptyLines()
      throws IOException {
    File dataFile = getDataFile("dataFileWithEmptyLines.csv");
    for (CSVRecordReaderConfig readerConfig : NULL_AND_EMPTY_CONFIGS) {
      validate(dataFile, readerConfig, 5);
    }

    CSVRecordReaderConfig readerConfig = new CSVRecordReaderConfig();
    readerConfig.setIgnoreEmptyLines(false);
    validate(dataFile, readerConfig, 8);
  }

  @Test
  public void testReadingDataFileWithEscapedQuotes()
      throws IOException {
    File dataFile = getDataFile("dataFileWithEscapedQuotes.csv");
    for (CSVRecordReaderConfig readerConfig : NULL_AND_EMPTY_CONFIGS) {
      validate(dataFile, readerConfig, List.of(
          createMap("\\\"id\\\"", "\\\"100\\\"", "\\\"name\\\"", "\\\"Jane\\\""),
          createMap("\\\"id\\\"", "\\\"101\\\"", "\\\"name\\\"", "\\\"John\\\"")
      ));
    }

    CSVRecordReaderConfig readerConfig = new CSVRecordReaderConfig();
    readerConfig.setEscapeCharacter('\\');
    validate(dataFile, readerConfig, List.of(
        createMap("\"id\"", "\"100\"", "\"name\"", "\"Jane\""),
        createMap("\"id\"", "\"101\"", "\"name\"", "\"John\"")
    ));
  }

  @Test
  public void testReadingDataFileWithNoHeader()
      throws IOException {
    File dataFile = getDataFile("dataFileWithNoHeader.csv");
    for (CSVRecordReaderConfig readerConfig : NULL_AND_EMPTY_CONFIGS) {
      validate(dataFile, readerConfig, List.of(
          createMap("100", "101", "Jane", "John"),
          createMap("100", "102", "Jane", "Sam")
      ));
    }

    CSVRecordReaderConfig readerConfig = new CSVRecordReaderConfig();
    readerConfig.setHeader("id,name");
    validate(dataFile, readerConfig, List.of(
        createMap("id", "100", "name", "Jane"),
        createMap("id", "101", "name", "John"),
        createMap("id", "102", "name", "Sam")
    ));
  }

  @Test
  public void testReadingDataFileWithNoHeaderAndEmptyValues()
      throws IOException {
    File dataFile = getDataFile("dataFileWithNoHeaderAndEmptyValues.csv");
    for (CSVRecordReaderConfig readerConfig : NULL_AND_EMPTY_CONFIGS) {
      validate(dataFile, readerConfig, List.of(
          createMap("key00", "key01", "12.3", null, "8.42", "7.1"),
          createMap("key00", "key02", "12.3", null, "8.42", "16.81"),
          createMap("key00", "key03", "12.3", null, "8.42", "7.12")
      ));
    }

    CSVRecordReaderConfig readerConfig = new CSVRecordReaderConfig();
    readerConfig.setHeader("key,num0,num1");
    validate(dataFile, readerConfig, List.of(
        createMap("key", "key00", "num0", "12.3", "num1", "8.42"),
        createMap("key", "key01", "num0", null, "num1", "7.1"),
        createMap("key", "key02", "num0", null, "num1", "16.81"),
        createMap("key", "key03", "num0", null, "num1", "7.12")
    ));
  }

  @Test
  public void testReadingDataFileWithNoRecords()
      throws IOException {
    File dataFile = getDataFile("dataFileWithNoRecords.csv");
    for (CSVRecordReaderConfig readerConfig : NULL_AND_EMPTY_CONFIGS) {
      validate(dataFile, readerConfig, 0);
    }

    CSVRecordReaderConfig readerConfig = new CSVRecordReaderConfig();
    readerConfig.setHeader("id,name");
    readerConfig.setSkipHeader(true);
    validate(dataFile, readerConfig, 0);
  }

  @Test
  public void testReadingDataFileEmpty()
      throws IOException {
    File dataFile = getDataFile("dataFileEmpty.csv");
    for (CSVRecordReaderConfig readerConfig : NULL_AND_EMPTY_CONFIGS) {
      validate(dataFile, readerConfig, 0);
    }

    CSVRecordReaderConfig readerConfig = new CSVRecordReaderConfig();
    readerConfig.setHeader("id,name");
    validate(dataFile, readerConfig, 0);
  }

  @Test
  public void testReadingDataFileWithMultiLineValues()
      throws IOException {
    File dataFile = getDataFile("dataFileWithMultiLineValues.csv");
    for (CSVRecordReaderConfig readerConfig : NULL_AND_EMPTY_CONFIGS) {
      validate(dataFile, readerConfig, List.of(
          createMap("id", "100", "name", "John\n101,Jane"),
          createMap("id", "102", "name", "Alice")
      ));
    }
  }

  @Test
  public void testReadingDataFileWithUnparseableFirstLine()
      throws IOException {
    File dataFile = getDataFile("dataFileWithUnparseableFirstLine.csv");
    for (CSVRecordReaderConfig readerConfig : NULL_AND_EMPTY_CONFIGS) {
      try (CSVRecordReader recordReader = new CSVRecordReader()) {
        assertThrows(IOException.class, () -> recordReader.init(dataFile, null, readerConfig));
      }
    }
  }

  @Test
  public void testLineIteratorReadingDataFileWithUnparseableLine()
      throws IOException {
    File dataFile = getDataFile("dataFileWithUnparseableLine.csv");
    for (CSVRecordReaderConfig readerConfig : NULL_AND_EMPTY_CONFIGS) {
      try (CSVRecordReader recordReader = new CSVRecordReader()) {
        recordReader.init(dataFile, null, readerConfig);
        testUnparseableLine(recordReader);
        recordReader.rewind();
        testUnparseableLine(recordReader);
      }
    }

    CSVRecordReaderConfig readerConfig = new CSVRecordReaderConfig();
    readerConfig.setStopOnError(true);
    try (CSVRecordReader recordReader = new CSVRecordReader()) {
      recordReader.init(dataFile, null, readerConfig);
      testUnparseableLineStopOnError(recordReader);
      recordReader.rewind();
      testUnparseableLineStopOnError(recordReader);
    }
  }

  private void testUnparseableLine(CSVRecordReader recordReader)
      throws IOException {
    // First line is parseable
    assertTrue(recordReader.hasNext());
    assertEquals(recordReader.next().getFieldToValueMap(), createMap("id", "100", "name", "John"));
    // Second line is unparseable, should throw exception when next() is called, and being skipped
    assertTrue(recordReader.hasNext());
    assertThrows(UncheckedIOException.class, recordReader::next);
    // Third line is parseable
    assertTrue(recordReader.hasNext());
    assertEquals(recordReader.next().getFieldToValueMap(), createMap("id", "102", "name", "Alice"));
    // 3 lines in total
    assertFalse(recordReader.hasNext());
  }

  private void testUnparseableLineStopOnError(CSVRecordReader recordReader)
      throws IOException {
    // First line is parseable
    assertTrue(recordReader.hasNext());
    assertEquals(recordReader.next().getFieldToValueMap(), createMap("id", "100", "name", "John"));
    // Second line is unparseable, stop here
    assertFalse(recordReader.hasNext());
  }

  @Test
  public void testLineIteratorReadingDataFileWithUnparseableLastLine()
      throws IOException {
    File dataFile = getDataFile("dataFileWithUnparseableLastLine.csv");
    for (CSVRecordReaderConfig readerConfig : NULL_AND_EMPTY_CONFIGS) {
      try (CSVRecordReader recordReader = new CSVRecordReader()) {
        recordReader.init(dataFile, null, readerConfig);
        testUnparseableLastLine(recordReader);
        recordReader.rewind();
        testUnparseableLastLine(recordReader);
      }
    }

    CSVRecordReaderConfig readerConfig = new CSVRecordReaderConfig();
    readerConfig.setStopOnError(true);
    try (CSVRecordReader recordReader = new CSVRecordReader()) {
      recordReader.init(dataFile, null, readerConfig);
      testUnparseableLastLineStopOnError(recordReader);
      recordReader.rewind();
      testUnparseableLastLineStopOnError(recordReader);
    }
  }

  private void testUnparseableLastLine(CSVRecordReader recordReader)
      throws IOException {
    // First line is parseable
    assertTrue(recordReader.hasNext());
    assertEquals(recordReader.next().getFieldToValueMap(), createMap("id", "100", "name", "John"));
    // Second line is unparseable, should throw exception when next() is called, and being skipped
    assertTrue(recordReader.hasNext());
    assertThrows(UncheckedIOException.class, recordReader::next);
    // 2 lines in total
    assertFalse(recordReader.hasNext());
  }

  private void testUnparseableLastLineStopOnError(CSVRecordReader recordReader)
      throws IOException {
    // First line is parseable
    assertTrue(recordReader.hasNext());
    assertEquals(recordReader.next().getFieldToValueMap(), createMap("id", "100", "name", "John"));
    // Second line is unparseable, stop here
    assertFalse(recordReader.hasNext());
  }

  @Test
  public void testReadingDataFileWithPartialLastRow()
      throws IOException {
    File dataFile = getDataFile("dataFileWithPartialLastRow.csv");
    for (CSVRecordReaderConfig readerConfig : NULL_AND_EMPTY_CONFIGS) {
      try (CSVRecordReader recordReader = new CSVRecordReader()) {
        recordReader.init(dataFile, null, readerConfig);
        testPartialLastRow(recordReader);
        recordReader.rewind();
        testPartialLastRow(recordReader);
      }
    }

    CSVRecordReaderConfig readerConfig = new CSVRecordReaderConfig();
    readerConfig.setStopOnError(true);
    try (CSVRecordReader recordReader = new CSVRecordReader()) {
      recordReader.init(dataFile, null, readerConfig);
      testPartialLastRowStopOnError(recordReader);
      recordReader.rewind();
      testPartialLastRowStopOnError(recordReader);
    }
  }

  private void testPartialLastRow(CSVRecordReader recordReader)
      throws IOException {
    assertTrue(recordReader.hasNext());
    assertEquals(recordReader.next().getFieldToValueMap(),
        createMap("id", "100", "firstName", "jane", "lastName", "doe", "appVersion", "1.0.0", "active", "yes"));
    assertTrue(recordReader.hasNext());
    assertEquals(recordReader.next().getFieldToValueMap(),
        createMap("id", "101", "firstName", "john", "lastName", "doe", "appVersion", "1.0.1", "active", "yes"));
    assertTrue(recordReader.hasNext());
    assertThrows(UncheckedIOException.class, recordReader::next);
    assertFalse(recordReader.hasNext());
  }

  private void testPartialLastRowStopOnError(CSVRecordReader recordReader)
      throws IOException {
    assertTrue(recordReader.hasNext());
    assertEquals(recordReader.next().getFieldToValueMap(),
        createMap("id", "100", "firstName", "jane", "lastName", "doe", "appVersion", "1.0.0", "active", "yes"));
    assertTrue(recordReader.hasNext());
    assertEquals(recordReader.next().getFieldToValueMap(),
        createMap("id", "101", "firstName", "john", "lastName", "doe", "appVersion", "1.0.1", "active", "yes"));
    assertFalse(recordReader.hasNext());
  }

  @Test
  public void testLineIteratorReadingDataFileWithMultipleCombinations()
      throws IOException {
    File dataFile = getDataFile("dataFileWithMultipleCombinations.csv");
    CSVRecordReaderConfig readerConfig = new CSVRecordReaderConfig();
    readerConfig.setCommentMarker('#');
    readerConfig.setEscapeCharacter('\\');
    try (CSVRecordReader recordReader = new CSVRecordReader()) {
      recordReader.init(dataFile, null, readerConfig);
      testCombinations(recordReader);
      recordReader.rewind();
      testCombinations(recordReader);
    }

    readerConfig.setStopOnError(true);
    try (CSVRecordReader recordReader = new CSVRecordReader()) {
      recordReader.init(dataFile, null, readerConfig);
      testCombinationsStopOnError(recordReader);
      recordReader.rewind();
      testCombinationsStopOnError(recordReader);
    }
  }

  private void testCombinations(CSVRecordReader recordReader)
      throws IOException {
    assertTrue(recordReader.hasNext());
    assertEquals(recordReader.next().getFieldToValueMap(), createMap("id", "100", "name", "John"));
    assertTrue(recordReader.hasNext());
    assertEquals(recordReader.next().getFieldToValueMap(), createMap("id", "101", "name", "Jane"));
    assertTrue(recordReader.hasNext());
    assertEquals(recordReader.next().getFieldToValueMap(), createMap("id", "102", "name", "Jerry"));
    assertTrue(recordReader.hasNext());
    assertEquals(recordReader.next().getFieldToValueMap(), createMap("id", "103", "name", "Suzanne"));
    // NOTE: Here we need to skip twice because the first line is a comment line
    assertTrue(recordReader.hasNext());
    assertThrows(UncheckedIOException.class, recordReader::next);
    assertTrue(recordReader.hasNext());
    assertThrows(UncheckedIOException.class, recordReader::next);
    assertTrue(recordReader.hasNext());
    assertEquals(recordReader.next().getFieldToValueMap(), createMap("id", "105", "name", "Zack\nZack"));
    assertTrue(recordReader.hasNext());
    assertEquals(recordReader.next().getFieldToValueMap(), createMap("id", "\"106\"", "name", "\"Ze\""));
    assertTrue(recordReader.hasNext());
    assertEquals(recordReader.next().getFieldToValueMap(), createMap("id", "107", "name", "Zu"));
    assertFalse(recordReader.hasNext());
  }

  private void testCombinationsStopOnError(CSVRecordReader recordReader)
      throws IOException {
    assertTrue(recordReader.hasNext());
    assertEquals(recordReader.next().getFieldToValueMap(), createMap("id", "100", "name", "John"));
    assertTrue(recordReader.hasNext());
    assertEquals(recordReader.next().getFieldToValueMap(), createMap("id", "101", "name", "Jane"));
    assertTrue(recordReader.hasNext());
    assertEquals(recordReader.next().getFieldToValueMap(), createMap("id", "102", "name", "Jerry"));
    assertTrue(recordReader.hasNext());
    assertEquals(recordReader.next().getFieldToValueMap(), createMap("id", "103", "name", "Suzanne"));
    assertFalse(recordReader.hasNext());
  }

  private File getDataFile(String fileName) {
    return new File(ClassLoader.getSystemResource(fileName).getFile());
  }

  private void validate(File dataFile, @Nullable CSVRecordReaderConfig readerConfig, int expectedNumRows,
      @Nullable List<Map<String, Object>> expectedRows)
      throws IOException {
    List<GenericRow> genericRows = new ArrayList<>(expectedNumRows);

    try (CSVRecordReader recordReader = new CSVRecordReader()) {
      recordReader.init(dataFile, null, readerConfig);
      while (recordReader.hasNext()) {
        genericRows.add(recordReader.next());
      }
      assertEquals(genericRows.size(), expectedNumRows);

      // Rewind the reader and read again
      recordReader.rewind();
      for (GenericRow row : genericRows) {
        GenericRow genericRow = recordReader.next();
        assertEquals(genericRow, row);
      }
      assertFalse(recordReader.hasNext());
    }

    if (expectedRows != null) {
      int rowId = 0;
      for (Map<String, Object> expectedRow : expectedRows) {
        GenericRow genericRow = genericRows.get(rowId++);
        assertEquals(genericRow.getFieldToValueMap(), expectedRow);
      }
    }
  }

  private void validate(File dataFile, @Nullable CSVRecordReaderConfig readerConfig, int expectedNumRows)
      throws IOException {
    validate(dataFile, readerConfig, expectedNumRows, null);
  }

  private void validate(File dataFile, @Nullable CSVRecordReaderConfig readerConfig,
      List<Map<String, Object>> expectedRows)
      throws IOException {
    validate(dataFile, readerConfig, expectedRows.size(), expectedRows);
  }

  private static Map<String, Object> createMap(String... keyValues) {
    Map<String, Object> map = new HashMap<>();
    for (int i = 0; i < keyValues.length; i += 2) {
      map.put(keyValues[i], keyValues[i + 1]);
    }
    return map;
  }
}
