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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.pinot.spi.data.readers.RecordReaderUtils;


public class CSVDefaultHeaderUtil {

  private CSVDefaultHeaderUtil() {
  }

  private static final String DEFAULT_CSV_COLUMN_PREFIX = "col_";
  private static final int MAX_CSV_SAMPLE_ROWS = 100;

  static class ColumnValueAttributes {
    private Type _dataType;
    private int _length;

    enum Type {
      STRING, NUMBER, ARRAY
    }

    ColumnValueAttributes(Object value, Character multiValueDelimiter) {
      _dataType = Type.STRING;
      if (multiValueDelimiter != null && value.toString().contains(String.valueOf(multiValueDelimiter))) {
        _dataType = Type.ARRAY;
        return;
      } else if (NumberUtils.isParsable(value.toString())) {
        _dataType = Type.NUMBER;
      }
      _length = value.toString().length();
    }

    public Type getDataType() {
      return _dataType;
    }

    public int getLength() {
      return _length;
    }
  }

  /**
   * Detects the types for each column based on the row values. If any column is of a single type (e.g. number), except
   * for the first row, then the first row is presumed to be a header. If the type of the column is assumed to
   * be a string, the length of the strings within the body is the determining factor. Specifically, if all the rows
   * except the first are the same length, it's a header.
   *
   * If no header is detected, then the header property within the CSVRecordReaderConfig will be set with the following
   * e.g. {col_0, col_1, col_2, ..}. If a header is detected within the file, then the header property will not
   * be set and will default to use the first row of the file.
   *
   * Motivation for this logic is taken from: https://github.com/python/cpython/blob/main/Lib/csv.py
   *
   * Returns the default header when no header is detected. If the header is detected, then returns null.
   */
  public static String getDefaultHeader(File csvFile, CSVFormat format, Character delimiter,
      Character multiValueDelimiter)
      throws IOException {
    // Assume there is no header and the default header based on number of columns. We need the default header or
    // else the CSVRecordReader may not read the values correctly when there are columns with the same values.
    int numColumns = 0;
    int rowCounter = 0;
    List<CSVRecord> records = new ArrayList<>();
    String[] originalHeader = format.getHeader();
    try (BufferedReader bufferedReader = RecordReaderUtils.getBufferedReader(csvFile)) {
      format.withHeader(null);
      CSVParser parser = format.parse(bufferedReader);
      Iterator<CSVRecord> iterator = parser.iterator();
      while (iterator.hasNext() && rowCounter < MAX_CSV_SAMPLE_ROWS + 1) {
        CSVRecord row = iterator.next();
        records.add(row);
        int curNumColumns = row.size();
        if (curNumColumns > numColumns) {
          numColumns = curNumColumns;
        }
        rowCounter++;
      }
    }

    if (records.size() == 0) {
      throw new IllegalStateException("CSV file is empty. csvFile=" + csvFile.getAbsolutePath());
    }

    String[] defaultColumnNames = new String[numColumns];
    for (int i = 0; i < numColumns; i++) {
      defaultColumnNames[i] = DEFAULT_CSV_COLUMN_PREFIX + i;
    }

    HashMap<String, ColumnValueAttributes> csvHeaderAttributes = new HashMap<>(numColumns);
    HashMap<String, ColumnValueAttributes> csvBodyAttributes = new HashMap<>(numColumns);
    Iterator<CSVRecord> iterator = records.iterator();
    CSVRecord header = iterator.next();
    for (int i = 0; i < header.size(); i++) {
      csvHeaderAttributes.put(defaultColumnNames[i], new ColumnValueAttributes(header.get(i), multiValueDelimiter));
      // Initialize the body attributes with null values at first
      csvBodyAttributes.put(defaultColumnNames[i], null);
    }

    while (iterator.hasNext() && rowCounter < MAX_CSV_SAMPLE_ROWS) {
      CSVRecord row = iterator.next();
      // Skip rows that have irregular number of columns
      if (row.size() != numColumns) {
        continue;
      }
      for (int i = 0; i < row.size(); i++) {
        String fieldName = DEFAULT_CSV_COLUMN_PREFIX + i;
        String columnValue = row.get(i);

        if (!csvBodyAttributes.containsKey(fieldName) || columnValue == null) {
          continue;
        }
        ColumnValueAttributes csvBodyAttribute = csvBodyAttributes.get(fieldName);
        ColumnValueAttributes curColumnAttributes = new ColumnValueAttributes(columnValue, multiValueDelimiter);
        if (csvBodyAttribute == null) {
          csvBodyAttributes.put(fieldName, curColumnAttributes);
        } else {
          // If column type is String and length does not match, remove from consideration
          if (csvBodyAttribute.getDataType().equals(ColumnValueAttributes.Type.STRING)
              && csvBodyAttribute.getLength() != curColumnAttributes.getLength()) {
            csvBodyAttributes.remove(fieldName);
          } else {
            // If data type does not match, remove from consideration
            if (!csvBodyAttribute.getDataType().equals(curColumnAttributes.getDataType())) {
              csvBodyAttributes.remove(fieldName);
            }
          }
        }
      }
    }

    // Compare first row with the rest of the values and vote on whether it's a header.
    int hasHeader = 0;
    for (Map.Entry<String, ColumnValueAttributes> entry : csvBodyAttributes.entrySet()) {
      ColumnValueAttributes headerAttributes = csvHeaderAttributes.get(entry.getKey());
      if (entry.getValue().getDataType().equals(ColumnValueAttributes.Type.STRING)) {
        // Compare lengths for String case
        if (headerAttributes.getLength() != entry.getValue().getLength()) {
          hasHeader++;
        } else {
          hasHeader--;
        }
      } else {
        // Compare types
        if (!headerAttributes.getDataType().equals(entry.getValue().getDataType())) {
          hasHeader++;
        } else {
          hasHeader--;
        }
      }
    }

    if (hasHeader > 0) {
      // reset the csv format to the original header.
      format.withHeader(originalHeader);
      return null;
    } else {
      return StringUtils.join(defaultColumnNames, delimiter);
    }
  }
}
