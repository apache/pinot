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

import org.apache.avro.Schema;
import org.apache.pinot.plugin.inputformat.avro.AvroRecordExtractor;


/// The type matrix is inherited from [AvroRecordExtractor]; the only override is the deprecated INT96 timestamp
/// (which parquet-avro surfaces as `union[null, fixed(12)]` with `doc = "INT96 represented as byte[12]"`)
/// → [java.sql.Timestamp] via [ParquetNativeRecordExtractor#convertInt96ToTimestamp].
public class ParquetAvroRecordExtractor extends AvroRecordExtractor {

  @Override
  protected Object transformValue(Object value, Schema.Field field) {
    return handleDeprecatedTypes(convert(value), field);
  }

  private Object handleDeprecatedTypes(Object value, Schema.Field field) {
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
      // INT96 is deprecated. We convert to `java.sql.Timestamp` as we do in the native parquet extractor.
      // See org.apache.parquet.avro.AvroSchemaConverter about how INT96 is converted into Avro schema.
      // We have to rely on the doc to determine whether a field is INT96.
      if (nonNullSchema.getType() == Schema.Type.FIXED && nonNullSchema.getFixedSize() == 12
          && "INT96 represented as byte[12]".equals(nonNullSchema.getDoc())) {
        return ParquetNativeRecordExtractor.convertInt96ToTimestamp((byte[]) value);
      }
    }
    return value;
  }
}
