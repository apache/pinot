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

import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.readers.BaseRecordExtractor;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;


/**
 * Extractor for JSON records
 */
public class JSONRecordExtractor extends BaseRecordExtractor<Map<String, Object>> {

  private Set<String> _fields;
  private boolean _extractAll = false;

  @Override
  public void init(Set<String> fields, @Nullable RecordExtractorConfig recordExtractorConfig) {
    _fields = fields;
    if (fields == null || fields.isEmpty()) {
      _extractAll = true;
    }
  }

  @Override
  public GenericRow extract(Map<String, Object> from, GenericRow to) {
    if (_extractAll) {
      from.forEach((fieldName, value) -> to.putValue(fieldName, convert(value)));
    } else {
      for (String fieldName : _fields) {
        Object value = from.get(fieldName);
        // NOTE about JSON behavior - cannot distinguish between INT/LONG and FLOAT/DOUBLE.
        // DataTypeTransformer fixes it.
        Object convertedValue = convert(value);
        to.putValue(fieldName, convertedValue);
      }
    }
    return to;
  }
}
