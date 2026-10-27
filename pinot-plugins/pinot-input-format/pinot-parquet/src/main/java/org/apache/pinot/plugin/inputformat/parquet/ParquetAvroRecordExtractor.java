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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.pinot.plugin.inputformat.avro.AvroRecordExtractor;
import org.apache.pinot.spi.data.readers.GenericRow;


/// The type matrix is inherited from [AvroRecordExtractor], with two overrides:
/// - INT96 timestamp (which parquet-avro surfaces as `fixed(12)` with `doc = "INT96 represented as byte[12]"`) →
///   `Timestamp` (or `Long` epoch nanos when `extractRawTimeValues` is `true`) via
///   [ParquetUtils].
/// - Top-level unsigned 32-bit integer fields (`INTEGER(32, false)` / legacy `UINT_32`), which parquet-avro surfaces
///   as a signed Avro `int` → `Long` via [Integer#toUnsignedLong]. The field names come from the Parquet file schema
///   (see [ParquetUtils#getUnsignedInt32FieldNames]) since the Avro schema carries no signedness. Unsigned values
///   nested in a struct, list or map are still read as signed ints by this reader; [ParquetNativeRecordReader]
///   widens them at every level.
///
/// Unsigned 64-bit overflow validation requires the original Parquet annotation and is only implemented by
/// [ParquetNativeRecordReader]. This Avro reader still exposes signed `long` bits for unsigned 64-bit fields.
public class ParquetAvroRecordExtractor extends AvroRecordExtractor {
  private static final int INT96_BYTE_SIZE = 12;
  private static final String INT96_DOC = "INT96 represented as byte[12]";

  private final Set<String> _unsignedInt32Fields;

  public ParquetAvroRecordExtractor() {
    this(Set.of());
  }

  public ParquetAvroRecordExtractor(Set<String> unsignedInt32Fields) {
    _unsignedInt32Fields = unsignedInt32Fields;
  }

  @Override
  public GenericRow extract(GenericRecord from, GenericRow to) {
    super.extract(from, to);
    for (String fieldName : _unsignedInt32Fields) {
      Object value = to.getValue(fieldName);
      if (value instanceof Integer) {
        to.putValue(fieldName, Integer.toUnsignedLong((Integer) value));
      }
    }
    return to;
  }

  @Override
  protected Object convertSingleValue(Schema schema, Object value) {
    if (schema.getType() == Schema.Type.FIXED && schema.getFixedSize() == INT96_BYTE_SIZE
        && INT96_DOC.equals(schema.getDoc())) {
      byte[] bytes = ((GenericFixed) value).bytes();
      return _extractRawTimeValues ? ParquetUtils.convertInt96ToEpochNanos(bytes)
          : ParquetUtils.convertInt96ToTimestamp(bytes);
    }
    return super.convertSingleValue(schema, value);
  }
}
