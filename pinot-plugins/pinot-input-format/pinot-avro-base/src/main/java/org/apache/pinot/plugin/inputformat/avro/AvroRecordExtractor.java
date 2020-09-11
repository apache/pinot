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

import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractor;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;


/**
 * Extractor for Avro Records
 */
public class AvroRecordExtractor implements RecordExtractor<GenericRecord> {
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
  public GenericRow extract(GenericRecord from, GenericRow to) {
    if (_extractAll) {
      List<Schema.Field> fields = from.getSchema().getFields();
      fields.forEach(field -> {
        String fieldName = field.name();
        to.putValue(fieldName, AvroUtils.convert(from.get(fieldName)));
      });
    } else {
      _fields.forEach(fieldName -> to.putValue(fieldName, AvroUtils.convert(from.get(fieldName))));
    }
    return to;
  }
}
