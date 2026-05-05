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
import org.apache.avro.generic.GenericFixed;
import org.apache.pinot.plugin.inputformat.avro.AvroRecordExtractor;
import org.apache.pinot.spi.utils.TimestampUtils;


/// The type matrix is inherited from [AvroRecordExtractor]; the only override is the INT96 timestamp
/// (which parquet-avro surfaces as `fixed(12)` with `doc = "INT96 represented as byte[12]"`) → [java.sql.Timestamp]
/// (or `Long` epoch nanos when `extractRawTimeValues` is `true`) via [ParquetUtils#convertInt96ToEpochNanos].
public class ParquetAvroRecordExtractor extends AvroRecordExtractor {
  private static final int INT96_BYTE_SIZE = 12;
  private static final String INT96_DOC = "INT96 represented as byte[12]";

  @Override
  protected Object convertSingleValue(Schema schema, Object value) {
    if (schema.getType() == Schema.Type.FIXED && schema.getFixedSize() == INT96_BYTE_SIZE
        && INT96_DOC.equals(schema.getDoc())) {
      long nanos = ParquetUtils.convertInt96ToEpochNanos(((GenericFixed) value).bytes());
      return _extractRawTimeValues ? nanos : TimestampUtils.fromNanosSinceEpoch(nanos);
    }
    return super.convertSingleValue(schema, value);
  }
}
