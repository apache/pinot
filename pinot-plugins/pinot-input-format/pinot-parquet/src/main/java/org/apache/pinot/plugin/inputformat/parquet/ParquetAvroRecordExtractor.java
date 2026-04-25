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

import java.util.Set;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.pinot.plugin.inputformat.avro.AvroRecordExtractor;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;


/// Extracts Pinot rows from Avro [org.apache.avro.generic.GenericRecord]s materialized by parquet-avro.
///
/// The reader sets `parquet.avro.add-list-element-records=false` in the Hadoop configuration, which tells
/// parquet-avro to flatten the standard Parquet 3-level LIST encoding
/// (`<list-rep> group <name> (LIST) { repeated group list { <elem-type> element; } }`) directly to an Avro
/// `array<elem-type>`. This matches Apache Arrow's Parquet reader behavior and means there is no LIST wrapper
/// to strip on the Pinot side — user-defined records like `array<record<UserTag, [element]>>` round-trip
/// cleanly because the file's Avro schema is honored as-is, and hand-authored Parquet `LIST<T>` surfaces as
/// flat values without a wrapper artifact.
public class ParquetAvroRecordExtractor extends AvroRecordExtractor {

  @Override
  public void init(@Nullable Set<String> fields, @Nullable RecordExtractorConfig recordExtractorConfig) {
    super.init(fields, recordExtractorConfig);
  }

  @Override
  protected Object transformValue(Object value, Schema.Field field) {
    return handleDeprecatedTypes(convert(value), field);
  }

  Object handleDeprecatedTypes(Object value, Schema.Field field) {
    Schema.Type avroColumnType = field.schema().getType();
    if (avroColumnType == Schema.Type.UNION) {
      Schema nonNullSchema = null;
      for (Schema childFieldSchema : field.schema().getTypes()) {
        if (childFieldSchema.getType() != Schema.Type.NULL) {
          if (nonNullSchema == null) {
            nonNullSchema = childFieldSchema;
          } else {
            throw new IllegalStateException("More than one non-null schema in UNION schema");
          }
        }
      }
      assert nonNullSchema != null;

      // NOTE:
      // INT96 is deprecated. We convert to long as we do in the native parquet extractor.
      // See org.apache.parquet.avro.AvroSchemaConverter about how INT96 is converted into Avro schema.
      // We have to rely on the doc to determine whether a field is INT96.
      if (nonNullSchema.getType() == Schema.Type.FIXED && nonNullSchema.getFixedSize() == 12
          && "INT96 represented as byte[12]".equals(nonNullSchema.getDoc())) {
        return ParquetNativeRecordExtractor.convertInt96ToLong((byte[]) value);
      }
    }
    return value;
  }
}
