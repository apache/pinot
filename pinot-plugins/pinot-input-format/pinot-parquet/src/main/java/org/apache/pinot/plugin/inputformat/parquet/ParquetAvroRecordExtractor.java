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
import org.apache.parquet.schema.PrimitiveType;
import org.apache.pinot.plugin.inputformat.avro.AvroRecordExtractor;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;


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
    if (avroColumnType == org.apache.avro.Schema.Type.UNION) {
      org.apache.avro.Schema nonNullSchema = null;
      for (org.apache.avro.Schema childFieldSchema : field.schema().getTypes()) {
        if (childFieldSchema.getType() != org.apache.avro.Schema.Type.NULL) {
          if (nonNullSchema == null) {
            nonNullSchema = childFieldSchema;
          } else {
            throw new IllegalStateException("More than one non-null schema in UNION schema");
          }
        }
      }

      //INT96 is deprecated. We convert to long as we do in the native parquet extractor.
      if (nonNullSchema.getName().equals(PrimitiveType.PrimitiveTypeName.INT96.name())) {
       return ParquetNativeRecordExtractor.convertInt96ToLong((byte[]) value);
      }
    }
    return value;
  }
}
