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

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.data.readers.AbstractRecordReaderTest;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.bson.BsonBinaryWriter;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Tests [BSONRecordReader] against the shared [AbstractRecordReaderTest] contract (happy path + gzip + rewind
/// over the generated schema), plus targeted negative / recovery cases driven by hand-built raw byte frames.
///
/// The generated `FLOAT` values are written as BSON doubles (BSON has no 32-bit float type), so the reader
/// reads them back as `Double`; the base assertions compare floating values with an epsilon, so this is
/// transparent.
public class BSONRecordReaderTest extends AbstractRecordReaderTest {

  @Override
  protected RecordReader createRecordReader(File file)
      throws Exception {
    BSONRecordReader recordReader = new BSONRecordReader();
    recordReader.init(file, _sourceFields, null);
    return recordReader;
  }

  @Override
  protected void writeRecordsToFile(List<Map<String, Object>> recordsToWrite)
      throws Exception {
    try (OutputStream out = new BufferedOutputStream(new FileOutputStream(_dataFile))) {
      for (Map<String, Object> record : recordsToWrite) {
        Document document = new Document();
        for (Map.Entry<String, Object> entry : record.entrySet()) {
          document.put(entry.getKey(), toBsonValue(entry.getValue()));
        }
        out.write(encode(document));
      }
    }
  }

  /// BSON's codec has no float codec, so normalize Float (and Floats inside arrays) to Double before encoding.
  private static Object toBsonValue(Object value) {
    if (value instanceof Float) {
      return ((Float) value).doubleValue();
    }
    if (value instanceof List) {
      List<Object> converted = new ArrayList<>();
      for (Object element : (List<?>) value) {
        converted.add(toBsonValue(element));
      }
      return converted;
    }
    return value;
  }

  @Override
  protected String getDataFileName() {
    return "data.bson";
  }

  // === Negative / recovery cases over hand-built raw frames ===

  @Test
  public void testEmptyFileHasNoRecords()
      throws Exception {
    try (BSONRecordReader reader = readerOver("empty.bson", new byte[0])) {
      assertFalse(reader.hasNext());
    }
  }

  @Test
  public void testTruncatedLengthPrefixThrows()
      throws Exception {
    // Only 2 of the 4 length-prefix bytes are present.
    assertInitThrowsIOException("truncated-prefix.bson", new byte[]{0x0C, 0x00});
  }

  @Test
  public void testLengthBelowMinimumThrows()
      throws Exception {
    // Declared length 2 is below the 5-byte minimum for a BSON document.
    assertInitThrowsIOException("invalid-length.bson", new byte[]{0x02, 0x00, 0x00, 0x00});
  }

  @Test
  public void testTruncatedBodyThrows()
      throws Exception {
    // Declared length 16 but only 2 body bytes follow the prefix.
    assertInitThrowsIOException("truncated-body.bson", new byte[]{0x10, 0x00, 0x00, 0x00, 0x01, 0x02});
  }

  @Test
  public void testRecoversAfterCorruptRecord()
      throws Exception {
    // A corrupt frame between two valid documents: length 8, then an unknown BSON element type (0x1F).
    byte[] corruptFrame = {0x08, 0x00, 0x00, 0x00, 0x1F, 0x00, 0x00, 0x00};
    byte[] content =
        concat(encode(new Document("a", 1)), corruptFrame, encode(new Document("b", 2)));
    try (BSONRecordReader reader = readerOver("recovery.bson", content)) {
      assertTrue(reader.hasNext());
      assertEquals(reader.next().getValue("a"), 1);

      // The corrupt frame surfaces as a skippable IOException; the reader then advances past it.
      assertTrue(reader.hasNext());
      assertThrows(IOException.class, reader::next);

      // The trailing valid document is still returned.
      assertTrue(reader.hasNext());
      assertEquals(reader.next().getValue("b"), 2);
      assertFalse(reader.hasNext());
    }
  }

  private BSONRecordReader readerOver(String name, byte[] content)
      throws IOException {
    BSONRecordReader reader = new BSONRecordReader();
    reader.init(writeTempFile(name, content), null, null);
    return reader;
  }

  private void assertInitThrowsIOException(String name, byte[] content)
      throws IOException {
    File file = writeTempFile(name, content);
    BSONRecordReader reader = new BSONRecordReader();
    assertThrows(IOException.class, () -> reader.init(file, null, null));
  }

  private File writeTempFile(String name, byte[] content)
      throws IOException {
    File file = new File(_tempDir, name);
    FileUtils.writeByteArrayToFile(file, content);
    return file;
  }

  private static byte[] encode(Document document) {
    DocumentCodec codec = new DocumentCodec();
    BasicOutputBuffer buffer = new BasicOutputBuffer();
    try (BsonBinaryWriter writer = new BsonBinaryWriter(buffer)) {
      codec.encode(writer, document, EncoderContext.builder().build());
    }
    return buffer.toByteArray();
  }

  private static byte[] concat(byte[]... chunks)
      throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (byte[] chunk : chunks) {
      out.write(chunk);
    }
    return out.toByteArray();
  }
}
