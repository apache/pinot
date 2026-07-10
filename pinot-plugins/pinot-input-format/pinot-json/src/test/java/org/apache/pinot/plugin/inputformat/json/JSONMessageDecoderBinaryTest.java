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
package org.apache.pinot.plugin.inputformat.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


/// End-to-end coverage of {@link JSONMessageDecoder} decoding each configured / auto-detected payload format
/// through the shared {@link JSONRecordExtractor} into a {@link GenericRow}.
public class JSONMessageDecoderBinaryTest {

  private static final Set<String> RICH_FIELDS = Set.of("name", "count", "ratio");
  private static final Set<String> SINGLE_FIELD = Set.of("a");

  private static byte[] smile(Map<String, Object> value)
      throws Exception {
    return new ObjectMapper(new SmileFactory()).writeValueAsBytes(value);
  }

  private static byte[] cbor(Map<String, Object> value)
      throws Exception {
    CBORFactory factory = new CBORFactory();
    factory.enable(CBORGenerator.Feature.WRITE_TYPE_HEADER);
    return new ObjectMapper(factory).writeValueAsBytes(value);
  }

  private static byte[] bytes(int... values) {
    byte[] result = new byte[values.length];
    for (int i = 0; i < values.length; i++) {
      result[i] = (byte) values[i];
    }
    return result;
  }

  // Explicit UTF-8 so fixtures do not depend on the platform default charset.
  private static final byte[] TEXT_DOC =
      "{\"name\":\"pinot\",\"count\":7,\"ratio\":2.5}".getBytes(StandardCharsets.UTF_8);

  // Hand-built {"a": 1} fixtures matching the parser unit tests.
  // SQLite: object(size 4){ text("a"), int("1") }.
  private static final byte[] SQLITE_A1 = bytes(0x4C, 0x17, 0x61, 0x13, 0x31);
  // PostgreSQL jsonb_send framing: version byte 1 followed by the text JSON body.
  private static final byte[] POSTGRES_A1 =
      bytes(0x01, '{', '"', 'a', '"', ':', '1', '}');

  private GenericRow decode(Map<String, String> props, Set<String> fields, byte[] payload)
      throws Exception {
    JSONMessageDecoder decoder = new JSONMessageDecoder();
    decoder.init(props, fields, "topic");
    return decoder.decode(payload, new GenericRow());
  }

  private void assertRich(GenericRow row) {
    assertEquals(row.getValue("name"), "pinot");
    assertEquals(row.getValue("count"), 7);
    assertEquals(row.getValue("ratio"), 2.5);
  }

  private Map<String, Object> richDoc() {
    return Map.of("name", "pinot", "count", 7, "ratio", 2.5);
  }

  /// The default is AUTO (per-message detection), not a pinned TEXT format; text JSON must still decode.
  @Test
  public void testUnsetFormatAutoDetectsText()
      throws Exception {
    assertRich(decode(Map.of(), RICH_FIELDS, TEXT_DOC));
  }

  @Test
  public void testConfiguredSmile()
      throws Exception {
    assertRich(decode(Map.of(JSONMessageDecoder.JSON_FORMAT_CONFIG_KEY, "SMILE"), RICH_FIELDS, smile(richDoc())));
  }

  @Test
  public void testConfiguredCbor()
      throws Exception {
    assertRich(decode(Map.of(JSONMessageDecoder.JSON_FORMAT_CONFIG_KEY, "CBOR"), RICH_FIELDS, cbor(richDoc())));
  }

  @Test
  public void testConfiguredSqlite()
      throws Exception {
    GenericRow row = decode(Map.of(JSONMessageDecoder.JSON_FORMAT_CONFIG_KEY, "SQLITE_JSONB"), SINGLE_FIELD,
        SQLITE_A1);
    assertEquals(row.getValue("a"), 1);
  }

  @Test
  public void testConfiguredPostgres()
      throws Exception {
    GenericRow row = decode(Map.of(JSONMessageDecoder.JSON_FORMAT_CONFIG_KEY, "POSTGRES_JSONB"), SINGLE_FIELD,
        POSTGRES_A1);
    assertEquals(row.getValue("a"), 1);
  }

  @Test
  public void testAutoDetectsSmile()
      throws Exception {
    // No jsonFormat configured -> AUTO detects Smile from its header.
    assertRich(decode(Map.of(), RICH_FIELDS, smile(richDoc())));
  }

  @Test
  public void testAutoDetectsCbor()
      throws Exception {
    assertRich(decode(Map.of(), RICH_FIELDS, cbor(richDoc())));
  }

  @Test
  public void testAutoDetectsSqlite()
      throws Exception {
    assertEquals(decode(Map.of(), SINGLE_FIELD, SQLITE_A1).getValue("a"), 1);
  }

  @Test
  public void testAutoDetectsPostgres()
      throws Exception {
    assertEquals(decode(Map.of(), SINGLE_FIELD, POSTGRES_A1).getValue("a"), 1);
  }

  @Test
  public void testAutoStillDecodesText()
      throws Exception {
    assertRich(decode(Map.of(JSONMessageDecoder.JSON_FORMAT_CONFIG_KEY, "AUTO"), RICH_FIELDS,
        TEXT_DOC));
  }

  @Test
  public void testMalformedPayloadIsWrappedInRuntimeException() {
    // A stream decoder routinely meets corrupt messages; decode() must surface them as a RuntimeException
    // rather than leaking the parser's own exception type.
    byte[] badSmile = bytes(0x3A, 0x29, 0x0A, 0x04, 0xFF, 0xFF, 0xFF);
    assertThrows(RuntimeException.class,
        () -> decode(Map.of(JSONMessageDecoder.JSON_FORMAT_CONFIG_KEY, "SMILE"), RICH_FIELDS, badSmile));
    // Same via AUTO, which detects Smile from the header and then fails to parse the body.
    assertThrows(RuntimeException.class, () -> decode(Map.of(), RICH_FIELDS, badSmile));
    // Truncated SQLite object.
    assertThrows(RuntimeException.class,
        () -> decode(Map.of(JSONMessageDecoder.JSON_FORMAT_CONFIG_KEY, "SQLITE_JSONB"), SINGLE_FIELD,
            bytes(0x5C, 0x17)));
  }

  @Test
  public void testSqliteTrailingBytesAreRejectedRatherThanIngestedAsAPartialRow() {
    // A SQLite JSONB payload whose top-level element declares a short size (here an empty object) used to
    // decode to an empty row, silently discarding the trailing "a": 1 -- via AUTO, which claims any payload
    // whose first byte has the OBJECT nibble. It must fail the message instead.
    byte[] shortObjectThenData = bytes(0x0C, 0x17, 0x61, 0x13, 0x31);
    assertThrows(RuntimeException.class, () -> decode(Map.of(), SINGLE_FIELD, shortObjectThenData));
    assertThrows(RuntimeException.class,
        () -> decode(Map.of(JSONMessageDecoder.JSON_FORMAT_CONFIG_KEY, "SQLITE_JSONB"), SINGLE_FIELD,
            shortObjectThenData));
  }

  @Test
  public void testUnsupportedFormatIsRejectedAtInit() {
    JSONMessageDecoder decoder = new JSONMessageDecoder();
    assertThrows(IllegalArgumentException.class,
        () -> decoder.init(Map.of(JSONMessageDecoder.JSON_FORMAT_CONFIG_KEY, "bson"), SINGLE_FIELD, "topic"));
  }
}
