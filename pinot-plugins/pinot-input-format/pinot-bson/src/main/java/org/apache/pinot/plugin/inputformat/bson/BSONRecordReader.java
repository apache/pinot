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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordFetchException;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.data.readers.RecordReaderUtils;
import org.bson.Document;


/**
 * Record reader for a BSON file: a concatenation of framed BSON documents (the {@code mongodump} layout), each
 * self-delimited by a leading little-endian int32 byte length. Documents are read sequentially, with the next
 * document's bytes fetched ahead so {@link #hasNext} does not perform I/O. GZIP-compressed files are supported.
 */
public class BSONRecordReader implements RecordReader {
  // Minimum size of a BSON document: 4-byte length prefix + 1-byte terminating NUL of an empty document.
  private static final int MIN_DOCUMENT_LENGTH = 5;

  private File _dataFile;
  private BSONRecordExtractor _recordExtractor;
  private InputStream _inputStream;
  // Bytes of the next framed document, or null once the stream is exhausted.
  private byte[] _nextDocument;

  public BSONRecordReader() {
  }

  @Override
  public void init(File dataFile, @Nullable Set<String> fieldsToRead, @Nullable RecordReaderConfig recordReaderConfig)
      throws IOException {
    _dataFile = dataFile;
    _recordExtractor = new BSONRecordExtractor();
    _recordExtractor.init(fieldsToRead, null);
    open();
  }

  private void open()
      throws IOException {
    _inputStream = RecordReaderUtils.getBufferedInputStream(_dataFile);
    _nextDocument = readNextDocument();
  }

  @Override
  public boolean hasNext() {
    return _nextDocument != null;
  }

  @Override
  public GenericRow next(GenericRow reuse)
      throws IOException {
    byte[] documentBytes = _nextDocument;
    // Advance the look-ahead first so that, even if the current record is corrupt, the reader has already moved
    // past it and the caller can skip the failure and continue.
    try {
      _nextDocument = readNextDocument();
    } catch (IOException e) {
      _nextDocument = null;
      throw new RecordFetchException("Failed to read next BSON record", e);
    }
    Document document;
    try {
      document = BSONUtils.decodeDocument(documentBytes);
    } catch (RuntimeException e) {
      throw new IOException("Failed to decode BSON record", e);
    }
    _recordExtractor.extract(document, reuse);
    return reuse;
  }

  /// Reads the next framed BSON document in full, or returns `null` at a clean end-of-stream. Throws when the
  /// stream ends partway through a document, or the length prefix is invalid.
  @Nullable
  private byte[] readNextDocument()
      throws IOException {
    int b0 = _inputStream.read();
    if (b0 == -1) {
      return null;
    }
    int b1 = _inputStream.read();
    int b2 = _inputStream.read();
    int b3 = _inputStream.read();
    if ((b1 | b2 | b3) < 0) {
      throw new IOException("Truncated BSON document: incomplete length prefix");
    }
    // BSON length prefix is a little-endian int32 inclusive of these 4 bytes.
    int length = (b0 & 0xFF) | ((b1 & 0xFF) << 8) | ((b2 & 0xFF) << 16) | ((b3 & 0xFF) << 24);
    if (length < MIN_DOCUMENT_LENGTH) {
      throw new IOException("Invalid BSON document length: " + length);
    }
    byte[] document = new byte[length];
    document[0] = (byte) b0;
    document[1] = (byte) b1;
    document[2] = (byte) b2;
    document[3] = (byte) b3;
    int bytesRead = 4;
    while (bytesRead < length) {
      int count = _inputStream.read(document, bytesRead, length - bytesRead);
      if (count == -1) {
        throw new IOException("Truncated BSON document: expected " + length + " bytes");
      }
      bytesRead += count;
    }
    return document;
  }

  @Override
  public void rewind()
      throws IOException {
    _inputStream.close();
    open();
  }

  @Override
  public void close()
      throws IOException {
    _inputStream.close();
  }
}
