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
package org.apache.pinot.plugin.inputformat.avro;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.pinot.spi.data.readers.AbstractDefaultRecordExtractor;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;


/**
 * Extractor for Avro Records
 */
public class AvroRecordExtractor extends AbstractDefaultRecordExtractor<GenericRecord, GenericRecord> {
  private Set<String> _fields;
  private boolean _extractAll = false;

  @Override
  public void init(@Nullable Set<String> fields, @Nullable RecordExtractorConfig recordExtractorConfig) {
    _fields = fields;
    if (fields == null || fields.isEmpty()) {
      _extractAll = true;
    }
  }

  @Override
  public GenericRow extract(GenericRecord from, GenericRow to) {
    if (_extractAll) {
      List<Schema.Field> fields = from.getSchema().getFields();
      for (Schema.Field field : fields) {
        String fieldName = field.name();
        to.putValue(fieldName, convert(from.get(fieldName)));
      }
    } else {
      for (String fieldName : _fields) {
        to.putValue(fieldName, convert(from.get(fieldName)));
      }
    }
    return to;
  }

  /**
   * Returns whether the object is an Avro GenericRecord.
   */
  @Override
  protected boolean isInstanceOfRecord(Object value) {
    return value instanceof GenericRecord;
  }

  /**
   * Handles the conversion of every field of the Avro GenericRecord.
   */
  @Override
  @Nullable
  protected Object convertRecord(GenericRecord record) {
    List<Schema.Field> fields = record.getSchema().getFields();
    if (fields.isEmpty()) {
      return null;
    }

    Map<Object, Object> convertedMap = new HashMap<>();
    for (Schema.Field field : fields) {
      String fieldName = field.name();
      convertedMap.put(fieldName, convert(record.get(fieldName)));
    }
    return convertedMap;
  }
}
