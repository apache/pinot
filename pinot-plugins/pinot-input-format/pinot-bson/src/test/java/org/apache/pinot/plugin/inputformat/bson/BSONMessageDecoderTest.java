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
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.bson.BsonBinaryWriter;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;


public class BSONMessageDecoderTest {

  @Test
  public void testDecodeScalarsAndLogicalTypes()
      throws Exception {
    ObjectId objectId = new ObjectId("5f2b1c0e1c9d440000a1b2c3");
    long epochMillis = 1_588_469_340_000L;
    Document document = new Document()
        .append("id", objectId)
        .append("name", "widget")
        .append("count", 7)
        .append("views", 1_588_469_340_000L)
        .append("price", 9.5d)
        .append("active", true)
        .append("amount", Decimal128.parse("123.456"))
        .append("createdAt", new Date(epochMillis))
        // The oplog / change-stream replication timestamp, the field a Kafka-forwarded change stream is most
        // likely to use as the Pinot time column.
        .append("ts", new BsonTimestamp((int) (epochMillis / 1000), 3));

    Set<String> fields = document.keySet();
    BSONMessageDecoder decoder = new BSONMessageDecoder();
    decoder.init(Map.of(), fields, "testTopic");

    GenericRow row = new GenericRow();
    decoder.decode(encode(document), row);

    assertEquals(row.getValue("id"), objectId.toHexString());
    assertEquals(row.getValue("name"), "widget");
    assertEquals(row.getValue("count"), 7);
    assertEquals(row.getValue("views"), 1_588_469_340_000L);
    assertEquals(row.getValue("price"), 9.5d);
    assertEquals(row.getValue("active"), true);
    assertEquals(row.getValue("amount"), new BigDecimal("123.456"));
    assertEquals(row.getValue("createdAt"), new Timestamp(epochMillis));
    assertEquals(row.getValue("ts"), new Timestamp(epochMillis));
  }

  @Test
  public void testDecodeNestedDocumentAndArray()
      throws Exception {
    Document document = new Document()
        .append("tags", List.of("a", "b", "c"))
        .append("meta", new Document("region", "us-east").append("shard", 3));

    Set<String> fields = document.keySet();
    BSONMessageDecoder decoder = new BSONMessageDecoder();
    decoder.init(Map.of(), fields, "testTopic");

    GenericRow row = new GenericRow();
    decoder.decode(encode(document), row);

    assertEquals((Object[]) row.getValue("tags"), new Object[]{"a", "b", "c"});
    Map<?, ?> meta = (Map<?, ?>) row.getValue("meta");
    assertEquals(meta.get("region"), "us-east");
    assertEquals(meta.get("shard"), 3);
  }

  @Test
  public void testDecodeWithFieldProjection()
      throws Exception {
    Document document = new Document().append("keep", 1).append("drop", 2);

    BSONMessageDecoder decoder = new BSONMessageDecoder();
    decoder.init(Map.of(), Set.of("keep"), "testTopic");

    GenericRow row = new GenericRow();
    decoder.decode(encode(document), row);

    assertEquals(row.getValue("keep"), 1);
    assertNull(row.getValue("drop"));
  }

  @Test
  public void testDecodeWithOffsetAndLength()
      throws Exception {
    Document document = new Document().append("k", "v");
    byte[] encoded = encode(document);

    // Embed the payload inside a larger buffer to exercise the offset/length decode path.
    byte[] padded = new byte[encoded.length + 8];
    int offset = 5;
    System.arraycopy(encoded, 0, padded, offset, encoded.length);

    BSONMessageDecoder decoder = new BSONMessageDecoder();
    decoder.init(Map.of(), Set.of("k"), "testTopic");

    GenericRow row = new GenericRow();
    GenericRow result = decoder.decode(padded, offset, encoded.length, row);
    assertSame(result, row);
    assertEquals(row.getValue("k"), "v");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testDecodeCorruptPayloadThrows()
      throws Exception {
    BSONMessageDecoder decoder = new BSONMessageDecoder();
    decoder.init(Map.of(), Set.of("k"), "testTopic");
    // Not a valid framed BSON document: the decode failure is wrapped in a RuntimeException.
    decoder.decode(new byte[]{1, 2, 3}, new GenericRow());
  }

  private static byte[] encode(Document document) {
    DocumentCodec codec = new DocumentCodec();
    BasicOutputBuffer buffer = new BasicOutputBuffer();
    try (BsonBinaryWriter writer = new BsonBinaryWriter(buffer)) {
      codec.encode(writer, document, EncoderContext.builder().build());
    }
    return buffer.toByteArray();
  }
}
