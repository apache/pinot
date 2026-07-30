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
package org.apache.pinot.plugin.inputformat.bson;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.bson.BsonBinaryWriter;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.Decimal128;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Tests [BSONRecordExtractor] — see its class Javadoc for the BSON type → Java output type matrix.
public class BSONRecordExtractorTest {

  private static final String COLUMN = "col";

  // === Scalars — pass through ===

  @Test
  public void testBooleanPreserved() {
    assertEquals(extract(true), true);
    assertEquals(extract(false), false);
  }

  @Test
  public void testIntegerPreserved() {
    assertEquals(extract(42), 42);
  }

  @Test
  public void testLongPreserved() {
    assertEquals(extract(1_588_469_340_000L), 1_588_469_340_000L);
  }

  @Test
  public void testDoublePreserved() {
    assertEquals(extract(1.5d), 1.5d);
  }

  @Test
  public void testStringPreserved() {
    assertEquals(extract("hello"), "hello");
  }

  @Test
  public void testNullPassedThrough() {
    assertNull(extract(null));
  }

  // === Extended BSON types found in normal documents ===

  @Test
  public void testObjectIdConvertedToHexString() {
    ObjectId objectId = new ObjectId("5f2b1c0e1c9d440000a1b2c3");
    assertEquals(extract(objectId), "5f2b1c0e1c9d440000a1b2c3");
  }

  @Test
  public void testDateConvertedToTimestamp() {
    long epochMillis = 1_588_469_340_000L;
    Object result = extract(new Date(epochMillis));
    assertTrue(result instanceof Timestamp);
    assertEquals(result, new Timestamp(epochMillis));
  }

  @Test
  public void testDecimal128ConvertedToBigDecimal() {
    assertEquals(extract(Decimal128.parse("123.456")), new BigDecimal("123.456"));
  }

  @Test
  public void testDecimal128NaNConvertedToNull() {
    // NaN / Infinity are legal Decimal128 values with no BigDecimal representation, so they surface as null.
    assertNull(extract(Decimal128.parse("NaN")));
    assertNull(extract(Decimal128.parse("Infinity")));
    assertNull(extract(Decimal128.parse("-Infinity")));
  }

  @Test
  public void testDecimal128NegativeZeroConvertedToZero() {
    // Negative zero is a legal Decimal128 value that bigDecimalValue() rejects; it is numerically zero. It is
    // negative-zero at any exponent, so "-0.00" must be handled too (it is not equal to Decimal128.NEGATIVE_ZERO).
    assertEquals(extract(Decimal128.parse("-0")), BigDecimal.ZERO);
    assertEquals(extract(Decimal128.parse("-0.00")), BigDecimal.ZERO);
  }

  @Test
  public void testBinaryConvertedToByteArray() {
    byte[] data = {1, 2, 3, 4};
    Object result = extract(new Binary(data));
    assertTrue(result instanceof byte[]);
    assertEquals((byte[]) result, data);
  }

  @Test
  public void testBsonTimestampConvertedToTimestamp() {
    // The internal replication timestamp: the oplog `ts` field and the change-stream `clusterTime` field. Its
    // seconds component becomes a java.sql.Timestamp; the intra-second ordinal counter is dropped.
    Object result = extract(new BsonTimestamp(1_588_469_340, 7));
    assertTrue(result instanceof Timestamp);
    assertEquals(result, new Timestamp(1_588_469_340_000L));
  }

  @Test
  public void testBsonTimestampAfter2038IsNotSignWrapped() {
    // BSON stores the seconds component as an UNSIGNED 32-bit field, but BsonTimestamp#getTime() returns it as
    // a signed int. From 2038-01-19T03:14:08Z onward the high bit is set, so a naive read wraps to a pre-1970
    // date. 2039-01-01T00:00:00Z = 2_177_452_800 seconds, which does not fit a positive signed int.
    long seconds = 2_177_452_800L;
    BsonTimestamp afterY2038 = new BsonTimestamp((int) seconds, 1);
    assertTrue(afterY2038.getTime() < 0, "precondition: the driver reports these seconds as a negative int");

    Object result = extract(afterY2038);
    assertEquals(result, new Timestamp(seconds * 1000L));
  }

  @Test
  public void testUuidBinarySubtypesConvertedToRawBytes() {
    // Subtypes 0x03 (legacy) and 0x04 (standard) are how MongoDB stores UUIDs. The standard DocumentCodec is
    // built with UuidRepresentation.UNSPECIFIED, so it decodes them to Binary -- not java.util.UUID -- and they
    // land as raw 16-byte BYTES. Round-trip through a real encode/decode so this pins the codec's behavior
    // rather than our own hand-built Binary: if a driver upgrade flips the default representation, these
    // columns would silently change from BYTES to a 36-char STRING.
    UUID uuid = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
    byte[] raw = new byte[16];
    ByteBuffer.wrap(raw).putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits());

    Document decoded = roundTrip(new Document()
        .append("standard", new Binary((byte) 0x04, raw))
        .append("legacy", new Binary((byte) 0x03, raw)));

    GenericRow row = new GenericRow();
    newExtractor().extract(decoded, row);
    assertEquals((byte[]) row.getValue("standard"), raw);
    assertEquals((byte[]) row.getValue("legacy"), raw);
  }

  @Test
  public void testExoticTypesFallBackToPinnedToStringForms() {
    // Rare / deprecated types with no Pinot-native representation fall back to value.toString(). Those strings
    // are not a documented contract of org.mongodb:bson, and they get baked into segment data -- so pin them
    // literally. A driver upgrade that reformats them must fail here rather than silently rewrite segments.
    assertEquals(extract(new MaxKey()), "MaxKey");
    assertEquals(extract(new MinKey()), "MinKey");
    assertEquals(extract(new Symbol("sym")), "sym");
    assertEquals(extract(new Code("function(){}")), "Code{code='function(){}'}");

    // Regex and JS code survive an encode/decode round trip as BsonRegularExpression / Code.
    Document decoded = roundTrip(new Document()
        .append("regex", Pattern.compile("^a.*z$"))
        .append("code", new Code("function(){}")));
    GenericRow row = new GenericRow();
    newExtractor().extract(decoded, row);
    assertEquals(row.getValue("regex"), "BsonRegularExpression{pattern='^a.*z$', options=''}");
    assertEquals(row.getValue("code"), "Code{code='function(){}'}");
  }

  // === Array (BSON array) → Object[] ===

  @Test
  public void testIntegerListExtractedAsArray() {
    Object[] result = (Object[]) extract(List.of(10, 20, 30));
    assertEquals(result, new Object[]{10, 20, 30});
  }

  @Test
  public void testStringListExtractedAsArray() {
    Object[] result = (Object[]) extract(List.of("foo", "bar"));
    assertEquals(result, new Object[]{"foo", "bar"});
  }

  @Test
  public void testListWithNullElement() {
    Object[] result = (Object[]) extract(Arrays.asList(1, null, 3));
    assertEquals(result, new Object[]{1, null, 3});
  }

  @Test
  public void testEmptyListExtractedAsEmptyArray() {
    Object[] result = (Object[]) extract(List.of());
    assertEquals(result, new Object[]{});
  }

  @Test
  public void testListOfObjectIdsConvertedElementwise() {
    ObjectId id1 = new ObjectId("5f2b1c0e1c9d440000a1b2c3");
    ObjectId id2 = new ObjectId("5f2b1c0e1c9d440000a1b2c4");
    Object[] result = (Object[]) extract(List.of(id1, id2));
    assertEquals(result, new Object[]{id1.toHexString(), id2.toHexString()});
  }

  // === Embedded document (BSON object) → Map<String, Object> ===

  @Test
  public void testEmbeddedDocumentConvertedToMap() {
    Document embedded = new Document("k1", 1).append("k2", "foo").append("k3", true);
    Map<?, ?> result = (Map<?, ?>) extract(embedded);
    assertEquals(result.size(), 3);
    assertEquals(result.get("k1"), 1);
    assertEquals(result.get("k2"), "foo");
    assertEquals(result.get("k3"), true);
  }

  @Test
  public void testEmbeddedDocumentWithNullValue() {
    Document embedded = new Document("present", 1);
    embedded.put("absent", null);
    Map<?, ?> result = (Map<?, ?>) extract(embedded);
    assertEquals(result.get("present"), 1);
    assertTrue(result.containsKey("absent"));
    assertNull(result.get("absent"));
  }

  @Test
  public void testNestedDocumentAndArray() {
    Document embedded =
        new Document("scalar", 1).append("nested", new Document("sub", 1.1)).append("list", List.of("a", "b"));
    Map<?, ?> result = (Map<?, ?>) extract(embedded);
    assertEquals(result.get("scalar"), 1);
    assertEquals(((Map<?, ?>) result.get("nested")).get("sub"), 1.1);
    assertEquals((Object[]) result.get("list"), new Object[]{"a", "b"});
  }

  // === Helpers ===

  /// Run the extractor against a single-column input document and return the extracted value.
  private static Object extract(@Nullable Object input) {
    Document record = new Document();
    record.put(COLUMN, input);
    GenericRow row = new GenericRow();
    newExtractor().extract(record, row);
    return row.getValue(COLUMN);
  }

  private static BSONRecordExtractor newExtractor() {
    BSONRecordExtractor extractor = new BSONRecordExtractor();
    extractor.init(null, null);
    return extractor;
  }

  /// Encodes and re-decodes a document so the test sees the Java types the standard `DocumentCodec` actually
  /// produces, rather than the ones the test handed in.
  private static Document roundTrip(Document document) {
    BasicOutputBuffer buffer = new BasicOutputBuffer();
    try (BsonBinaryWriter writer = new BsonBinaryWriter(buffer)) {
      new DocumentCodec().encode(writer, document, EncoderContext.builder().build());
    }
    return BSONUtils.decodeDocument(buffer.toByteArray());
  }
}
