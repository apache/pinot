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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Tests [ParquetAvroRecordExtractor]'s INT96 override — see its class Javadoc. All other Avro behavior is
/// inherited and covered by `AvroRecordExtractorTest`.
public class ParquetAvroRecordExtractorTest {

  private static final String COLUMN = "col";

  @Test
  public void testInt96ConvertedToTimestamp() {
    // parquet-avro surfaces an INT96 column as `union[null, fixed(12)]` with `doc = "INT96 represented as
    // byte[12]"`. The 12-byte value is little-endian: bytes 0..7 = nanos within the day (long), bytes 8..11
    // = Julian day number (int). We pick a known instant and verify the round-trip.
    long epochMillis = 1_649_924_302_123L;
    long millisPerDay = 86_400_000L;
    long dayNumber = epochMillis / millisPerDay;
    long nanosWithinDay = (epochMillis - dayNumber * millisPerDay) * 1_000_000L;
    int julianDay = (int) (dayNumber + ParquetNativeRecordExtractor.JULIAN_DAY_NUMBER_FOR_UNIX_EPOCH);
    byte[] int96 = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN)
        .putLong(nanosWithinDay).putInt(julianDay).array();

    Schema fixedSchema = Schema.createFixed("Int96", "INT96 represented as byte[12]", null, 12);
    Schema fieldSchema = Schema.createUnion(List.of(Schema.create(Schema.Type.NULL), fixedSchema));
    GenericRecord record = singleField(fieldSchema, new GenericData.Fixed(fixedSchema, int96));

    assertEquals(extract(record), new Timestamp(epochMillis));
  }

  @Test
  public void testNonInt96FixedPassesThrough() {
    // A `fixed(12)` field whose doc isn't the INT96 marker is left as raw `byte[]`.
    byte[] bytes = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
    Schema fixedSchema = Schema.createFixed("Plain", null, null, 12);
    GenericRecord record = singleField(fixedSchema, new GenericData.Fixed(fixedSchema, bytes));
    assertEquals((byte[]) extract(record), bytes);
  }

  @Test
  public void testInt96AsBareFixedPassesThrough() {
    // The INT96 conversion only fires when the field schema is a UNION; a bare (non-union) FIXED field, even
    // with the INT96 doc, falls through the convert() path and surfaces as `byte[]` (which then loses the
    // INT96 semantics — caller is responsible for declaring INT96 fields as nullable).
    byte[] bytes = new byte[12];
    Schema fixedSchema = Schema.createFixed("Int96", "INT96 represented as byte[12]", null, 12);
    GenericRecord record = singleField(fixedSchema, new GenericData.Fixed(fixedSchema, bytes));
    assertEquals((byte[]) extract(record), bytes);
  }

  // === Helpers ===

  private static Object extract(GenericRecord record) {
    ParquetAvroRecordExtractor extractor = new ParquetAvroRecordExtractor();
    extractor.init(null, null);
    GenericRow row = new GenericRow();
    extractor.extract(record, row);
    return row.getValue(COLUMN);
  }

  /// Build a single-field [GenericRecord] with `value` set on `fieldSchema`.
  private static GenericRecord singleField(Schema fieldSchema, Object value) {
    Schema record = Schema.createRecord("R", null, null, false);
    record.setFields(List.of(new Field(COLUMN, fieldSchema, null, null)));
    GenericRecord r = new GenericData.Record(record);
    r.put(COLUMN, value);
    return r;
  }
}
