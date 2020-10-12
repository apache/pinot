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
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractor;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;


/**
 * Extractor for CSV records
 */
public class CSVRecordExtractor implements RecordExtractor<CSVRecord> {

  private char _multiValueDelimiter;
  private Set<String> _fields;

  @Override
  public void init(Set<String> fields, RecordExtractorConfig recordExtractorConfig) {
    _fields = fields;
    _multiValueDelimiter = ((CSVRecordExtractorConfig) recordExtractorConfig).getMultiValueDelimiter();
  }

  @Override
  public GenericRow extract(CSVRecord from, GenericRow to) {
    for (String fieldName : _fields) {
      String value = from.isSet(fieldName) ? from.get(fieldName) : null;
      to.putValue(fieldName, convert(value));
    }
    return to;
  }

  @Override
  @Nullable
  public Object convert(@Nullable Object value) {
    String stringValue = (String) value;
    if (stringValue == null || StringUtils.isEmpty(stringValue)) {
      return null;
      // NOTE about CSV behavior for empty string e.g. foo,bar,,zoo or foo,bar,"",zoo. These both are equivalent to a CSVParser
      // Empty string has to be treated as null, as this could be a column of any data type.
      // This could be incorrect for STRING dataType, as "" could be a legit entry, different than null.
    } else {
      String[] stringValues = StringUtils.split(stringValue, _multiValueDelimiter);
      int numValues = stringValues.length;
      // NOTE about CSV behavior for multi value column - cannot distinguish between multi value column with just 1 entry vs single value
      // MV column with single value will be treated as single value until DataTypeTransformer.
      // Transform functions on such columns will have to handle the special case.
      if (numValues > 1) {
        Object[] array = new Object[numValues];
        int index = 0;
        for (String stringVal : stringValues) {
          array[index++] = stringVal;
        }
        return array;
      } else {
        return stringValue;
      }
    }
  }
}
