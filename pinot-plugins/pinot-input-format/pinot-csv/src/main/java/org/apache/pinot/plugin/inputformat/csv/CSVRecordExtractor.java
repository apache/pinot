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

import java.util.List;
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

  @Override
  public void init(RecordExtractorConfig recordExtractorConfig) {
    _multiValueDelimiter = ((CSVRecordExtractorConfig) recordExtractorConfig).getMultiValueDelimiter();
  }

  @Override
  public GenericRow extract(List<String> sourceFieldNames, CSVRecord from, GenericRow to) {
    for (String fieldName : sourceFieldNames) {
      String value = from.isSet(fieldName) ? from.get(fieldName) : null;
      if (StringUtils.isEmpty(value)) {
        value = null;
        // FIXME: forcibly treating empty STRING as null is wrong. defaultNullValue could be something other than "".
        //  But what choice do we have in CSV?
      }
      to.putValue(fieldName, value);
      if (value != null) {
        String[] stringValues = StringUtils.split(value, _multiValueDelimiter);
        int numValues = stringValues.length;
        // FIXME: MV column with single value will be treated as single value until DataTypeTransformer.
        //  What about transform function on MV column? The function will have to handle this.
        //  Again, what choice do we have in CSV
        if (numValues > 1) {
          Object[] array = new Object[numValues];
          int index = 0;
          for (String stringValue : stringValues) {
              array[index++] = stringValue;
          }
          to.putValue(fieldName, array);
        }
      }
    }
    return to;
  }
}
