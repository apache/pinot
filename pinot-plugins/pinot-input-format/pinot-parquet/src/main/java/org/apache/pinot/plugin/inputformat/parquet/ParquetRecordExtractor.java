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
package org.apache.pinot.plugin.inputformat.parquet;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.avro.generic.GenericRecord;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractor;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;
import org.apache.pinot.spi.data.readers.RecordReaderUtils;


/**
 * Extractor for Parquet records
 */
public class ParquetRecordExtractor implements RecordExtractor<GenericRecord> {

  private List<String> _fields;

  @Override
  public void init(List<String> fields, @Nullable RecordExtractorConfig recordExtractorConfig) {
    _fields = fields;
  }

  @Override
  public GenericRow extract(GenericRecord from, GenericRow to) {
    for (String fieldName : _fields) {
      Object value = from.get(fieldName);
      Object convertedValue = RecordReaderUtils.convert(value);
      to.putValue(fieldName, convertedValue);
    }
    return to;
  }
}
