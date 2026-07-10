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
package org.apache.pinot.plugin.inputformat.json.format;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class JsonPayloadFormatTest {

  private static byte[] bytes(int... values) {
    byte[] result = new byte[values.length];
    for (int i = 0; i < values.length; i++) {
      result[i] = (byte) values[i];
    }
    return result;
  }

  /// Explicit UTF-8 so fixtures do not depend on the platform default charset.
  private static byte[] utf8(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  /// PostgreSQL `jsonb_send` framing: version byte 1 followed by the text JSON body.
  private static byte[] postgres(String json) {
    byte[] body = utf8(json);
    byte[] result = new byte[body.length + 1];
    result[0] = 1;
    System.arraycopy(body, 0, result, 1, body.length);
    return result;
  }

  private static byte[] smile(Object value)
      throws Exception {
    return new ObjectMapper(new SmileFactory()).writeValueAsBytes(value);
  }

  private static byte[] cborSelfDescribed(Object value)
      throws Exception {
    CBORFactory factory = new CBORFactory();
    factory.enable(CBORGenerator.Feature.WRITE_TYPE_HEADER);
    return new ObjectMapper(factory).writeValueAsBytes(value);
  }

  private static Map<String, Object> parse(JsonPayloadParser parser, byte[] payload)
      throws Exception {
    return parser.parse(payload, 0, payload.length);
  }

  // ------------------------------------------------------------------------------------------------------
  // Config parsing
  // ------------------------------------------------------------------------------------------------------

  @Test
  public void testFromConfig() {
    // An unset property means TEXT -- the decoder's historical behavior. AUTO is opt-in, never implicit,
    // because detection can claim a corrupt message that text decoding would have rejected.
    assertEquals(JsonPayloadFormat.fromConfig(null), JsonPayloadFormat.TEXT);
    assertEquals(JsonPayloadFormat.fromConfig(""), JsonPayloadFormat.TEXT);
    assertEquals(JsonPayloadFormat.fromConfig("  "), JsonPayloadFormat.TEXT);

    assertEquals(JsonPayloadFormat.fromConfig("text"), JsonPayloadFormat.TEXT);
    assertEquals(JsonPayloadFormat.fromConfig("auto"), JsonPayloadFormat.AUTO);
    assertEquals(JsonPayloadFormat.fromConfig("  Smile "), JsonPayloadFormat.SMILE);
    assertEquals(JsonPayloadFormat.fromConfig("POSTGRES_JSONB"), JsonPayloadFormat.POSTGRES_JSONB);
    // AUTO resolves the concrete format per message, so every format has a non-null parser.
    assertSame(JsonPayloadFormat.AUTO.getParser().getClass(), AutoDetectPayloadParser.class);
  }

  @Test
  public void testFromConfigRejectsUnknownFormatAndKeepsTheCause() {
    IllegalArgumentException e =
        expectThrows(IllegalArgumentException.class, () -> JsonPayloadFormat.fromConfig("bson"));
    // The message names the offending value and the supported set ...
    assertTrue(e.getMessage().contains("bson"), e.getMessage());
    assertTrue(e.getMessage().contains("SQLITE_JSONB"), e.getMessage());
    // ... and the underlying valueOf failure is preserved so the stack trace still points at the parse.
    assertNotNull(e.getCause());
  }

  // ------------------------------------------------------------------------------------------------------
  // Text
  // ------------------------------------------------------------------------------------------------------

  @Test
  public void testText()
      throws Exception {
    JsonPayloadParser parser = JsonPayloadFormat.TEXT.getParser();
    Map<String, Object> map = parse(parser, utf8("  {\"a\": 1, \"b\": \"x\"}"));
    assertEquals(map.get("a"), 1);
    assertEquals(map.get("b"), "x");
    assertTrue(parser.matches(utf8("{\"a\":1}"), 0, 7));
    assertTrue(parser.matches(utf8("  [1,2]"), 0, 7));
    assertFalse(parser.matches(bytes(0xFF), 0, 1));
  }

  // ------------------------------------------------------------------------------------------------------
  // Smile / CBOR (round-tripped through Jackson)
  // ------------------------------------------------------------------------------------------------------

  @Test
  public void testSmile()
      throws Exception {
    Map<String, Object> original = Map.of("name", "pinot", "count", 7, "ratio", 2.5, "ok", true, "tags",
        List.of("a", "b"));
    byte[] encoded = smile(original);

    JsonPayloadParser parser = JsonPayloadFormat.SMILE.getParser();
    assertTrue(parser.matches(encoded, 0, encoded.length));
    assertEquals(parse(parser, encoded), original);
  }

  @Test
  public void testCbor()
      throws Exception {
    Map<String, Object> original = Map.of("name", "pinot", "count", 7, "ratio", 2.5, "ok", true, "nested",
        Map.of("x", 1));
    byte[] encoded = cborSelfDescribed(original);

    JsonPayloadParser parser = JsonPayloadFormat.CBOR.getParser();
    assertTrue(parser.matches(encoded, 0, encoded.length));
    assertEquals(parse(parser, encoded), original);
  }

  @Test
  public void testCborWithoutSelfDescribeTagIsNotAutoDetected()
      throws Exception {
    byte[] noTag = new ObjectMapper(new CBORFactory()).writeValueAsBytes(Map.of("name", "pinot"));
    JsonPayloadParser parser = JsonPayloadFormat.CBOR.getParser();
    // Not detectable without the tag ...
    assertFalse(parser.matches(noTag, 0, noTag.length));
    assertSame(JsonPayloadFormat.detectParser(noTag, 0, noTag.length).getClass(), TextJsonPayloadParser.class);
    // ... but explicitly configuring CBOR still decodes it.
    assertEquals(parse(parser, noTag).get("name"), "pinot");
  }

  @Test
  public void testBinaryFormatsPreserveFloatAndBytes()
      throws Exception {
    // Float and byte[] are scalars the binary formats can carry but text JSON cannot. The parser contract
    // documents that they pass through; guard against a regression that silently upcasts Float -> Double.
    Map<String, Object> original = new HashMap<>();
    original.put("f", 1.5f);
    original.put("bin", new byte[]{1, 2, 3});

    for (byte[] encoded : List.of(smile(original), cborSelfDescribed(original))) {
      Map<String, Object> decoded = parse(JsonPayloadFormat.detectParser(encoded, 0, encoded.length), encoded);
      assertTrue(decoded.get("f") instanceof Float, "expected Float, got " + decoded.get("f").getClass());
      assertEquals(decoded.get("f"), 1.5f);
      assertEquals((byte[]) decoded.get("bin"), new byte[]{1, 2, 3});
    }
  }

  @Test
  public void testJacksonFormatsRejectMalformedPayloads() {
    // Valid Smile header, garbage body.
    byte[] badSmile = bytes(0x3A, 0x29, 0x0A, 0x04, 0xFF, 0xFF, 0xFF);
    assertThrows(Exception.class, () -> parse(JsonPayloadFormat.SMILE.getParser(), badSmile));
    // Self-describe tag then a truncated map.
    byte[] badCbor = bytes(0xD9, 0xD9, 0xF7, 0xBF, 0x63);
    assertThrows(Exception.class, () -> parse(JsonPayloadFormat.CBOR.getParser(), badCbor));
  }

  // ------------------------------------------------------------------------------------------------------
  // PostgreSQL JSONB wire format: version byte + text JSON (jsonb_send / jsonb_recv)
  // ------------------------------------------------------------------------------------------------------

  @Test
  public void testPostgresWireFormat()
      throws Exception {
    JsonPayloadParser parser = JsonPayloadFormat.POSTGRES_JSONB.getParser();
    byte[] payload = postgres("{\"a\":1,\"b\":\"x\",\"c\":1.5,\"d\":true,\"e\":null,\"f\":[1,2]}");
    assertTrue(parser.matches(payload, 0, payload.length));

    Map<String, Object> map = parse(parser, payload);
    // Identical type contract to TEXT, since the body is plain text JSON.
    assertEquals(map.get("a"), 1);
    assertEquals(map.get("b"), "x");
    assertEquals(map.get("c"), 1.5);
    assertEquals(map.get("d"), true);
    assertNull(map.get("e"));
    assertEquals(map.get("f"), List.of(1, 2));
  }

  @Test
  public void testPostgresBigIntegerNarrowing()
      throws Exception {
    JsonPayloadParser parser = JsonPayloadFormat.POSTGRES_JSONB.getParser();
    Map<String, Object> map = parse(parser, postgres("{\"n\":9999999999,\"h\":100000000000000000000}"));
    assertEquals(map.get("n"), 9999999999L);
    assertEquals(map.get("h"), new BigInteger("100000000000000000000"));
  }

  @Test
  public void testPostgresErrors() {
    JsonPayloadParser parser = JsonPayloadFormat.POSTGRES_JSONB.getParser();
    // Unsupported version byte (jsonb_recv rejects anything but 1).
    assertThrows(IllegalArgumentException.class, () -> parse(parser, bytes(0x02, '{', '}')));
    // Version byte only.
    assertThrows(IllegalArgumentException.class, () -> parse(parser, bytes(0x01)));
    // Version byte followed by a non-JSON body is not claimed by detection.
    assertFalse(parser.matches(bytes(0x01, 0xFF, 0xFF), 0, 3));
    // The raw on-disk JsonbContainer layout is NOT the wire format and must not be silently accepted.
    assertFalse(parser.matches(bytes(0x01, 0x01, 0x00, 0x00, 0x20), 0, 5));
  }

  // ------------------------------------------------------------------------------------------------------
  // SQLite JSONB (hand-computed fixtures per https://sqlite.org/jsonb.html)
  // ------------------------------------------------------------------------------------------------------

  @Test
  public void testSqliteScalars()
      throws Exception {
    JsonPayloadParser parser = JsonPayloadFormat.SQLITE_JSONB.getParser();

    // {"a": 1} -> object(size 4){ text("a"), int("1") }
    assertEquals(parse(parser, bytes(0x4C, 0x17, 0x61, 0x13, 0x31)), Map.of("a", 1));
    // {"b": true} -> object(size 3){ text("b"), true(size 0) }
    assertEquals(parse(parser, bytes(0x3C, 0x17, 0x62, 0x01)), Map.of("b", true));
    // {"f2": false} -> object(size 4){ text("f2"), false(size 0) }
    assertEquals(parse(parser, bytes(0x4C, 0x27, 0x66, 0x32, 0x02)), Map.of("f2", false));
    // {"s": "hi"} -> object(size 5){ text("s"), text("hi") }
    assertEquals(parse(parser, bytes(0x5C, 0x17, 0x73, 0x27, 0x68, 0x69)), Map.of("s", "hi"));
    // {"r": "hi"} -> object(size 5){ text("r"), textraw("hi") }
    assertEquals(parse(parser, bytes(0x5C, 0x17, 0x72, 0x2A, 0x68, 0x69)), Map.of("r", "hi"));
    // {"f": 1.5} -> object(size 6){ text("f"), float("1.5") }
    assertEquals(parse(parser, bytes(0x6C, 0x17, 0x66, 0x35, 0x31, 0x2E, 0x35)), Map.of("f", 1.5));
  }

  @Test
  public void testSqliteJson5AndEscapes()
      throws Exception {
    JsonPayloadParser parser = JsonPayloadFormat.SQLITE_JSONB.getParser();

    // {"h": 0x1F} -> object{ text("h"), int5("0x1F") } -> 31
    assertEquals(parse(parser, bytes(0x7C, 0x17, 0x68, 0x44, 0x30, 0x78, 0x31, 0x46)), Map.of("h", 31));
    // {"i": Infinity} -> object{ text("i"), float5("Infinity") }
    assertEquals(parse(parser, bytes(0xBC, 0x17, 0x69, 0x86, 0x49, 0x6E, 0x66, 0x69, 0x6E, 0x69, 0x74, 0x79)),
        Map.of("i", Double.POSITIVE_INFINITY));
    // {"t": "a\nb"} -> object{ text("t"), textj("a\\nb") } -> newline unescaped
    assertEquals(parse(parser, bytes(0x7C, 0x17, 0x74, 0x48, 0x61, 0x5C, 0x6E, 0x62)), Map.of("t", "a\nb"));
    // {"t": "\x41"} -> object{ text("t"), text5("\\x41") } -> "A" (JSON5 hex escape)
    assertEquals(parse(parser, bytes(0x7C, 0x17, 0x74, 0x49, 0x5C, 0x78, 0x34, 0x31)), Map.of("t", "A"));
  }

  @Test
  public void testSqliteTextjRejectsInvalidEscape() {
    JsonPayloadParser parser = JsonPayloadFormat.SQLITE_JSONB.getParser();
    // TEXTJ carries standard JSON escapes only; "\q" is invalid and must not be silently passed through.
    assertThrows(IllegalArgumentException.class, () -> parse(parser, bytes(0x6C, 0x17, 0x74, 0x38, 0x5C, 0x71)));
  }

  @Test
  public void testSqliteNested()
      throws Exception {
    JsonPayloadParser parser = JsonPayloadFormat.SQLITE_JSONB.getParser();

    // {"arr": [1, 2]} -> object(size 9){ text("arr"), array(size 4){ int("1"), int("2") } }
    assertEquals(parse(parser, bytes(0x9C, 0x37, 0x61, 0x72, 0x72, 0x4B, 0x13, 0x31, 0x13, 0x32)),
        Map.of("arr", List.of(1, 2)));

    // {"o": {"x": null}} -> object(size 6){ text("o"), object(size 3){ text("x"), null } }
    Map<String, Object> nested = parse(parser, bytes(0x6C, 0x17, 0x6F, 0x3C, 0x17, 0x78, 0x00));
    //noinspection unchecked
    Map<String, Object> inner = (Map<String, Object>) nested.get("o");
    assertTrue(inner.containsKey("x"));
    assertNull(inner.get("x"));
  }

  @Test
  public void testSqliteIntNarrowing()
      throws Exception {
    assertEquals(sqliteSingleInt("42"), 42);                                                       // fits int
    assertEquals(sqliteSingleInt("9999999999"), 9999999999L);                                      // > int
    assertEquals(sqliteSingleInt("99999999999999999999"), new BigInteger("99999999999999999999")); // > long
  }

  /// Builds `{"n": <digits>}` as SQLite JSONB and returns the decoded value of `n`.
  private static Object sqliteSingleInt(String digits)
      throws Exception {
    byte[] digitBytes = digits.getBytes(StandardCharsets.US_ASCII);
    // An element header carries its size in the high nibble when it fits 0..11, else via descriptor 12.
    boolean wideInt = digitBytes.length > 11;
    int valueElem = (wideInt ? 2 : 1) + digitBytes.length;
    int objPayload = 2 + valueElem;
    boolean wideObj = objPayload > 11;
    byte[] payload = new byte[(wideObj ? 2 : 1) + objPayload];
    int i = 0;
    payload[i++] = (byte) (wideObj ? 0xCC : ((objPayload << 4) | 0x0C));
    if (wideObj) {
      payload[i++] = (byte) objPayload;
    }
    payload[i++] = (byte) 0x17; // text "n" (size 1)
    payload[i++] = (byte) 'n';
    payload[i++] = (byte) (wideInt ? 0xC3 : ((digitBytes.length << 4) | 0x03));
    if (wideInt) {
      payload[i++] = (byte) digitBytes.length;
    }
    System.arraycopy(digitBytes, 0, payload, i, digitBytes.length);
    return parse(JsonPayloadFormat.SQLITE_JSONB.getParser(), payload).get("n");
  }

  @Test
  public void testSqliteTwoByteSizeDescriptor()
      throws Exception {
    JsonPayloadParser parser = JsonPayloadFormat.SQLITE_JSONB.getParser();
    // {"k": "<300 x 'a'>"} forces size descriptor 13 (2-byte big-endian size) for both the text value and the
    // enclosing object.
    int textLen = 300;
    int valueElem = 1 + 2 + textLen;
    int objPayload = 2 + valueElem;
    byte[] payload = new byte[3 + objPayload];
    int i = 0;
    payload[i++] = (byte) 0xDC;                        // OBJECT, size descriptor 13
    payload[i++] = (byte) (objPayload >>> 8);
    payload[i++] = (byte) objPayload;
    payload[i++] = (byte) 0x17;                        // text "k"
    payload[i++] = (byte) 'k';
    payload[i++] = (byte) 0xD7;                        // TEXT, size descriptor 13
    payload[i++] = (byte) (textLen >>> 8);
    payload[i++] = (byte) textLen;
    Arrays.fill(payload, i, i + textLen, (byte) 'a');
    assertEquals(parse(parser, payload).get("k"), "a".repeat(textLen));
  }

  @Test
  public void testSqliteErrors() {
    JsonPayloadParser parser = JsonPayloadFormat.SQLITE_JSONB.getParser();
    // Reserved element type (low nibble 13).
    assertThrows(IllegalArgumentException.class, () -> parse(parser, bytes(0x0D)));
    // Non-object top-level (a bare int).
    assertThrows(IllegalArgumentException.class, () -> parse(parser, bytes(0x13, 0x35)));
    // Truncated: object claims 5 payload bytes but only 1 follows.
    assertThrows(IllegalArgumentException.class, () -> parse(parser, bytes(0x5C, 0x17)));
    // Object payload ends after a label, with no value for it.
    assertThrows(IllegalArgumentException.class, () -> parse(parser, bytes(0x2C, 0x17, 0x61)));
    // Object label must be a text element, not an int.
    assertThrows(IllegalArgumentException.class, () -> parse(parser, bytes(0x3C, 0x13, 0x31, 0x01)));
  }

  @Test
  public void testSqliteTopLevelElementMustExactlyFillThePayload()
      throws Exception {
    JsonPayloadParser parser = JsonPayloadFormat.SQLITE_JSONB.getParser();
    // A bare empty object consumes the whole payload and is valid.
    assertEquals(parse(parser, bytes(0x0C)), Map.of());

    // SQLite's validity rule requires the outer element to exactly fill the BLOB. Previously a top-level
    // element declaring a short size decoded to a partial row and silently dropped the trailing bytes:
    // an empty object (size 0) followed by real data ...
    assertThrows(IllegalArgumentException.class, () -> parse(parser, bytes(0x0C, 0x17, 0x61, 0x13, 0x31)));
    // ... and a well-formed {"a": 1} object with a stray trailing byte.
    assertThrows(IllegalArgumentException.class, () -> parse(parser, bytes(0x4C, 0x17, 0x61, 0x13, 0x31, 0xFF)));
  }

  @Test
  public void testSqliteWideSizeDescriptors()
      throws Exception {
    JsonPayloadParser parser = JsonPayloadFormat.SQLITE_JSONB.getParser();
    // Descriptor 14: object size as a 4-byte big-endian int. Payload is text("a") + int("1").
    assertEquals(parse(parser, bytes(0xEC, 0x00, 0x00, 0x00, 0x04, 0x17, 0x61, 0x13, 0x31)), Map.of("a", 1));
    // Descriptor 15: object size as an 8-byte big-endian int.
    assertEquals(parse(parser,
            bytes(0xFC, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x17, 0x61, 0x13, 0x31)),
        Map.of("a", 1));

    // A uint64 size with the sign bit set decodes to a negative long and must be rejected, not wrapped.
    assertThrows(IllegalArgumentException.class, () -> parse(parser,
        bytes(0xFC, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x17)));
    // A large positive size must be rejected by comparison, without overflowing when added to the offset.
    assertThrows(IllegalArgumentException.class, () -> parse(parser,
        bytes(0xFC, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x17)));
  }

  @Test
  public void testSqliteJson5NumberVariants()
      throws Exception {
    JsonPayloadParser parser = JsonPayloadFormat.SQLITE_JSONB.getParser();
    // {"h": -0x10} -> int5 with a sign and hex radix -> -16
    assertEquals(parse(parser, bytes(0x8C, 0x17, 0x68, 0x54, 0x2D, 0x30, 0x78, 0x31, 0x30)), Map.of("h", -16));
    // {"p": +1.5} -> float5 with a leading '+' (rejected by Double.parseDouble until stripped)
    assertEquals(parse(parser, bytes(0x7C, 0x17, 0x70, 0x46, 0x2B, 0x31, 0x2E, 0x35)), Map.of("p", 1.5));
  }

  @Test
  public void testSqliteTextEscapeVariants()
      throws Exception {
    JsonPayloadParser parser = JsonPayloadFormat.SQLITE_JSONB.getParser();
    // TEXTJ \\u escape: {"u": "A"} -> "A"
    assertEquals(parse(parser, bytes(0x9C, 0x17, 0x75, 0x68, 0x5C, 0x75, 0x30, 0x30, 0x34, 0x31)), Map.of("u", "A"));
    // TEXT5 extensions, one payload each: \' -> ' , \v -> VT, \0 -> NUL, \q -> q (passthrough)
    assertEquals(parse(parser, bytes(0x5C, 0x17, 0x65, 0x29, 0x5C, 0x27)), Map.of("e", "'"));
    assertEquals(parse(parser, bytes(0x5C, 0x17, 0x65, 0x29, 0x5C, 0x76)), Map.of("e", "\u000B"));
    assertEquals(parse(parser, bytes(0x5C, 0x17, 0x65, 0x29, 0x5C, 0x30)), Map.of("e", "\0"));
    assertEquals(parse(parser, bytes(0x5C, 0x17, 0x65, 0x29, 0x5C, 0x71)), Map.of("e", "q"));
    // TEXT5 line continuation: "a\<LF>b" -> "ab"
    assertEquals(parse(parser, bytes(0x7C, 0x17, 0x65, 0x49, 0x61, 0x5C, 0x0A, 0x62)), Map.of("e", "ab"));
  }

  @Test
  public void testSqliteNestedElementCannotOverrunItsContainer() {
    JsonPayloadParser parser = JsonPayloadFormat.SQLITE_JSONB.getParser();
    // object(size 8){ text("a"), array(size 1){ text(size 4) }, "b": 7 }
    // The array declares a 1-byte payload, but the text element inside it declares 4 bytes -- which would run
    // past the array's end and swallow the object's remaining "b": 7 pair. Must be rejected, not silently
    // truncated.
    assertThrows(IllegalArgumentException.class,
        () -> parse(parser, bytes(0x8C, 0x17, 0x61, 0x1B, 0x47, 0x17, 0x62, 0x13, 0x37)));
  }

  // ------------------------------------------------------------------------------------------------------
  // AUTO detection + cross-format equivalence
  // ------------------------------------------------------------------------------------------------------

  @Test
  public void testAutoDetection()
      throws Exception {
    assertSame(JsonPayloadFormat.detectParser(utf8("{\"a\":1}"), 0, 7).getClass(), TextJsonPayloadParser.class);
    byte[] smile = smile(Map.of("a", 1));
    assertSame(JsonPayloadFormat.detectParser(smile, 0, smile.length).getClass(), SmileJsonPayloadParser.class);
    byte[] cbor = cborSelfDescribed(Map.of("a", 1));
    assertSame(JsonPayloadFormat.detectParser(cbor, 0, cbor.length).getClass(), CborJsonPayloadParser.class);
    byte[] pg = postgres("{\"a\":1}");
    assertSame(JsonPayloadFormat.detectParser(pg, 0, pg.length).getClass(), PostgresJsonbPayloadParser.class);
    byte[] sqlite = bytes(0x3C, 0x17, 0x62, 0x01);
    assertSame(JsonPayloadFormat.detectParser(sqlite, 0, sqlite.length).getClass(), SqliteJsonbPayloadParser.class);
  }

  @Test
  public void testSqliteDetectionRequiresAnExactlyFillingObject()
      throws Exception {
    JsonPayloadParser sqlite = JsonPayloadFormat.SQLITE_JSONB.getParser();
    // The OBJECT low nibble alone would claim one arbitrary binary byte in sixteen, so detection additionally
    // requires the declared top-level size to exactly fill the payload.
    assertTrue(sqlite.matches(bytes(0x4C, 0x17, 0x61, 0x13, 0x31), 0, 5));   // size 4, 4 bytes follow
    assertFalse(sqlite.matches(bytes(0x4C, 0x17, 0x61), 0, 3));              // size 4, only 2 bytes follow
    assertFalse(sqlite.matches(bytes(0x0C, 0x17, 0x61, 0x13, 0x31), 0, 5));  // size 0, but 4 bytes follow
    assertFalse(sqlite.matches(bytes(0x4C, 0x17, 0x61, 0x13, 0x31, 0xFF), 0, 6)); // size 4, trailing byte

    // Random binary that merely happens to carry the OBJECT nibble is no longer claimed ...
    assertFalse(sqlite.matches(bytes(0xFC, 0xDE, 0xAD, 0xBE, 0xEF), 0, 5));
    assertSame(JsonPayloadFormat.detectParser(bytes(0x0C, 0x17, 0x61, 0x13, 0x31), 0, 5).getClass(),
        TextJsonPayloadParser.class);

    // ... but the degenerate empty object, which does exactly fill its payload, still is.
    assertTrue(sqlite.matches(bytes(0x0C), 0, 1));

    // Every size descriptor participates in the same check. Each positive case is the object {"a": 1} whose
    // 4-byte payload is declared through a different-width size field, and each negative case is the same
    // payload with the declared size off by one.
    // Descriptor 12: 1-byte size.
    assertTrue(sqlite.matches(bytes(0xCC, 0x04, 0x17, 0x61, 0x13, 0x31), 0, 6));
    assertFalse(sqlite.matches(bytes(0xCC, 0x05, 0x17, 0x61, 0x13, 0x31), 0, 6));
    // Descriptor 13: 2-byte big-endian size.
    assertTrue(sqlite.matches(bytes(0xDC, 0x00, 0x04, 0x17, 0x61, 0x13, 0x31), 0, 7));
    assertFalse(sqlite.matches(bytes(0xDC, 0x00, 0x05, 0x17, 0x61, 0x13, 0x31), 0, 7));
    // A byte-order slip here would read 0x0400 rather than 0x0004.
    assertFalse(sqlite.matches(bytes(0xDC, 0x04, 0x00, 0x17, 0x61, 0x13, 0x31), 0, 7));
    // Descriptor 14: 4-byte big-endian size.
    assertTrue(sqlite.matches(bytes(0xEC, 0x00, 0x00, 0x00, 0x04, 0x17, 0x61, 0x13, 0x31), 0, 9));
    assertFalse(sqlite.matches(bytes(0xEC, 0x00, 0x00, 0x00, 0x05, 0x17, 0x61, 0x13, 0x31), 0, 9));
    // Descriptor 15: 8-byte big-endian size.
    assertTrue(sqlite.matches(
        bytes(0xFC, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x17, 0x61, 0x13, 0x31), 0, 13));
    assertFalse(sqlite.matches(
        bytes(0xFC, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x17, 0x61, 0x13, 0x31), 0, 13));
    // An 8-byte size with the sign bit set decodes negative and can never equal a length.
    assertFalse(sqlite.matches(bytes(0xFC, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x17), 0, 10));

    // Truncated headers must not read past the payload.
    assertFalse(sqlite.matches(bytes(0xCC), 0, 1));
    assertFalse(sqlite.matches(bytes(0xDC, 0x00), 0, 2));
    assertFalse(sqlite.matches(bytes(0xEC, 0x00), 0, 2));
    assertFalse(sqlite.matches(bytes(0xFC, 0x00, 0x00, 0x00), 0, 4));

    // Detection and parsing must agree: anything matches() claims here, parse() accepts.
    assertEquals(parse(sqlite, bytes(0xCC, 0x04, 0x17, 0x61, 0x13, 0x31)), Map.of("a", 1));
    assertEquals(parse(sqlite, bytes(0xDC, 0x00, 0x04, 0x17, 0x61, 0x13, 0x31)), Map.of("a", 1));
  }

  @Test
  public void testAutoDetectionFallbackAndAdversarialInput() {
    // Nothing matches -> TEXT fallback.
    assertSame(JsonPayloadFormat.detectParser(bytes(0xFF), 0, 1).getClass(), TextJsonPayloadParser.class);
    // Empty payload -> TEXT fallback; matches() must not throw on too-short input.
    assertSame(JsonPayloadFormat.detectParser(bytes(), 0, 0).getClass(), TextJsonPayloadParser.class);
    // Truncated Smile header must not be claimed by the Smile parser.
    assertSame(JsonPayloadFormat.detectParser(bytes(0x3A, 0x29), 0, 2).getClass(), TextJsonPayloadParser.class);
    // A lone 0x01 with no JSON body must not be claimed by the Postgres parser.
    assertSame(JsonPayloadFormat.detectParser(bytes(0x01, 0x02), 0, 2).getClass(), TextJsonPayloadParser.class);
  }

  @Test
  public void testCrossFormatEquivalence()
      throws Exception {
    // The same logical document must decode to an equal Map through every format.
    Map<String, Object> expected = new HashMap<>();
    expected.put("count", 7);
    expected.put("ok", true);

    byte[] text = utf8("{\"count\":7,\"ok\":true}");
    byte[] pg = postgres("{\"count\":7,\"ok\":true}");
    byte[] smile = smile(expected);
    byte[] cbor = new ObjectMapper(new CBORFactory()).writeValueAsBytes(expected);
    // object(size 12, descriptor 12){ text("count"), int("7"), text("ok"), true }
    byte[] sqlite = bytes(0xCC, 0x0C, 0x57, 0x63, 0x6F, 0x75, 0x6E, 0x74, 0x13, 0x37, 0x27, 0x6F, 0x6B, 0x01);

    assertEquals(parse(JsonPayloadFormat.TEXT.getParser(), text), expected);
    assertEquals(parse(JsonPayloadFormat.POSTGRES_JSONB.getParser(), pg), expected);
    assertEquals(parse(JsonPayloadFormat.SMILE.getParser(), smile), expected);
    assertEquals(parse(JsonPayloadFormat.CBOR.getParser(), cbor), expected);
    assertEquals(parse(JsonPayloadFormat.SQLITE_JSONB.getParser(), sqlite), expected);
  }

  @Test
  public void testAutoParserDelegates()
      throws Exception {
    JsonPayloadParser auto = JsonPayloadFormat.AUTO.getParser();
    assertTrue(auto.matches(bytes(0x00), 0, 1));
    assertEquals(parse(auto, postgres("{\"a\":1}")), Map.of("a", 1));
    assertEquals(parse(auto, utf8("{\"a\":1}")), Map.of("a", 1));
    assertEquals(parse(auto, smile(Map.of("a", 1))), Map.of("a", 1));
  }
}
