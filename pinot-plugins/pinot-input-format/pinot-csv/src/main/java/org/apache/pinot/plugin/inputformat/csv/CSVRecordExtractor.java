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

import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.data.readers.BaseRecordExtractor;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;


/**
 * Extractor for CSV records
 */
public class CSVRecordExtractor extends BaseRecordExtractor<CSVRecord> {

  private Character _multiValueDelimiter = null;
  private Set<String> _fields;

  @Override
  public void init(Set<String> fields, RecordExtractorConfig recordExtractorConfig) {
    CSVRecordExtractorConfig csvRecordExtractorConfig = (CSVRecordExtractorConfig) recordExtractorConfig;
    if (fields == null || fields.isEmpty()) {
      _fields = csvRecordExtractorConfig.getColumnNames();
    } else {
      _fields = Set.copyOf(fields);
    }
    _multiValueDelimiter = csvRecordExtractorConfig.getMultiValueDelimiter();
  }

  @Override
  public GenericRow extract(CSVRecord from, GenericRow to) {
    for (String fieldName : _fields) {
      String value = from.isSet(fieldName) ? from.get(fieldName) : null;
      to.putValue(fieldName, convert(value));
    }
    return to;
  }

  private Object convert(@Nullable String value) {
    if (value == null || StringUtils.isEmpty(value)) {
      return null;
      // NOTE about CSV behavior for empty string e.g. foo,bar,,zoo or foo,bar,"",zoo. These both are equivalent to a
      // CSVParser
      // Empty string has to be treated as null, as this could be a column of any data type.
      // This could be incorrect for STRING dataType, as "" could be a legit entry, different than null.
    }

    // If the delimiter is not set, then return the value as is
    if (_multiValueDelimiter == null) {
      return value;
    }

    // Performance optimization: Use fast string.contains() instead of slow regex
    if (value.contains(String.valueOf(_multiValueDelimiter))) {
      // Performance optimization: Use manual split instead of regex for maximum speed
      String delimiter = String.valueOf(_multiValueDelimiter);
      int delimiterLength = delimiter.length();
      int lastIndex = 0;
      int count = 0;

      // Count delimiters to pre-allocate array
      for (int i = 0; i < value.length() - delimiterLength + 1; i++) {
        if (value.regionMatches(i, delimiter, 0, delimiterLength)) {
          count++;
        }
      }

      if (count > 0) {
        // Create array with exact size needed
        Object[] array = new Object[count + 1];
        int arrayIndex = 0;

        // Manual split for maximum performance
        for (int i = 0; i < value.length() - delimiterLength + 1; i++) {
          if (value.regionMatches(i, delimiter, 0, delimiterLength)) {
            array[arrayIndex++] = value.substring(lastIndex, i);
            lastIndex = i + delimiterLength;
          }
        }
        // Add the last part
        array[arrayIndex] = value.substring(lastIndex);

        return array;
      }
    }
    return value;
  }
}
