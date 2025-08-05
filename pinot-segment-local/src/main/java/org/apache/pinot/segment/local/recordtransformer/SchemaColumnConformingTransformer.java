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
package org.apache.pinot.segment.local.recordtransformer;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.recordtransformer.RecordTransformer;

/**
 * The {@code SchemaColumnComformingTransformer} class will transform rows
 * to conform to the schema columns. Transformed rows should only have those columns
 * which are present in the schema
 */
public class SchemaColumnConformingTransformer implements RecordTransformer {

  final List<String> _schemaColumns;

  public SchemaColumnConformingTransformer(final Schema schema) {
    if (schema == null) {
      _schemaColumns = null;
      return;
    }
    _schemaColumns = schema.getAllFieldSpecs().stream()
        .map(FieldSpec::getName)
        .collect(Collectors.toList());
  }

  @Override
  public boolean isNoOp() {
    return _schemaColumns == null;
  }

  @Override
  public void transform(GenericRow record) {
    final List<String> nullValueFieldsToRemove = record.getNullValueFields().stream()
        .filter(entry -> !_schemaColumns.contains(entry))
        .collect(Collectors.toList());
    nullValueFieldsToRemove.forEach(record::removeNullValueField);

    final List<String> fieldKeysToRemove = record.getFieldToValueMap().keySet().stream()
        .filter(key -> !_schemaColumns.contains(key))
        .collect(Collectors.toList());
    fieldKeysToRemove.forEach(record::removeValue);
  }
}
