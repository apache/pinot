/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.local.segment.creator.impl.fwd;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Random;
import org.apache.pinot.segment.local.io.writer.impl.BaseChunkSVForwardIndexWriter;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkSVForwardIndexWriter;
import org.apache.pinot.segment.local.segment.index.readers.forward.BaseChunkSVForwardIndexReader.ChunkReaderContext;
import org.apache.pinot.segment.local.segment.index.readers.forward.VarByteChunkMVForwardIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.forward.VarByteChunkSVForwardIndexReader;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.V1Constants.Indexes;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;

/**
 * Forward index creator for raw (non-dictionary-encoded) single-value column of variable length
 * data type (STRING,
 * BYTES).
 */
public class MultiValueVarByteRawIndexCreator implements ForwardIndexCreator {

  private static final int DEFAULT_NUM_DOCS_PER_CHUNK = 1000;
  private static final int TARGET_MAX_CHUNK_SIZE = 1024 * 1024;

  private final VarByteChunkSVForwardIndexWriter _indexWriter;
  private final DataType _valueType;

  /**
   * Create a var-byte raw index creator for the given column
   *
   * @param baseIndexDir Index directory
   * @param compressionType Type of compression to use
   * @param column Name of column to index
   * @param totalDocs Total number of documents to index
   * @param valueType Type of the values
   * @param maxTotalContentLength max total content length
   * @param maxElements max number of elements
   */
  public MultiValueVarByteRawIndexCreator(File baseIndexDir, ChunkCompressionType compressionType,
      String column,
      int totalDocs, DataType valueType, int maxTotalContentLength, int maxElements)
      throws IOException {
    this(baseIndexDir, compressionType, column, totalDocs, valueType, false, maxTotalContentLength,
        maxElements,
        BaseChunkSVForwardIndexWriter.DEFAULT_VERSION);
  }

  /**
   * Create a var-byte raw index creator for the given column
   *
   * @param baseIndexDir Index directory
   * @param compressionType Type of compression to use
   * @param column Name of column to index
   * @param totalDocs Total number of documents to index
   * @param valueType Type of the values
   * @param deriveNumDocsPerChunk true if writer should auto-derive the number of rows per chunk
   * @param maxTotalContentLength max total content length
   * @param maxElements max number of elements
   * @param writerVersion writer format version
   */
  public MultiValueVarByteRawIndexCreator(File baseIndexDir, ChunkCompressionType compressionType,
      String column,
      int totalDocs, DataType valueType, boolean deriveNumDocsPerChunk, int maxTotalContentLength,
      int maxElements,
      int writerVersion)
      throws IOException {
    //we will prepend the actual content with numElements and length array containing length of each element
    int maxLength = Integer.BYTES + maxElements * Integer.BYTES + maxTotalContentLength;
    File file = new File(baseIndexDir,
        column + Indexes.RAW_MV_FORWARD_INDEX_FILE_EXTENSION);
    int numDocsPerChunk =
        deriveNumDocsPerChunk ? getNumDocsPerChunk(maxLength) : DEFAULT_NUM_DOCS_PER_CHUNK;
    _indexWriter = new VarByteChunkSVForwardIndexWriter(file, compressionType, totalDocs,
        numDocsPerChunk, maxLength,
        writerVersion);
    _valueType = valueType;
  }

  @VisibleForTesting
  public static int getNumDocsPerChunk(int lengthOfLongestEntry) {
    int overheadPerEntry =
        lengthOfLongestEntry + VarByteChunkSVForwardIndexWriter.CHUNK_HEADER_ENTRY_ROW_OFFSET_SIZE;
    return Math.max(TARGET_MAX_CHUNK_SIZE / overheadPerEntry, 1);
  }

  @Override
  public boolean isDictionaryEncoded() {
    return false;
  }

  @Override
  public boolean isSingleValue() {
    return false;
  }

  @Override
  public DataType getValueType() {
    return _valueType;
  }

  @Override
  public void putStringMV(final String[] values) {
    int totalBytes = 0;
    for (int i = 0; i < values.length; i++) {
      final String value = values[i];
      int length = value.getBytes().length;
      totalBytes += length;
    }
    byte[] bytes = new byte[Integer.BYTES + Integer.BYTES * values.length
        + totalBytes];//numValues, length array, concatenated bytes
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    //write the length
    byteBuffer.putInt(values.length);
    //write the length of each element
    for (final String value : values) {
      byteBuffer.putInt(value.getBytes().length);
    }
    //write the content of each element
    //todo:maybe there is a smart way to avoid 3 loops but at the cost of allocating more memory upfront and resize as needed
    for (final String value : values) {
      byteBuffer.put(value.getBytes());
    }
//    System.out.println("Inserting bytes of length:" + bytes.length);
    _indexWriter.putBytes(bytes);
  }

  @Override
  public void putBytesMV(final byte[][] values) {
    int totalBytes = 0;
    for (int i = 0; i < values.length; i++) {
      int length = values[i].length;
      totalBytes += length;
    }
    byte[] bytes = new byte[Integer.BYTES + Integer.BYTES * values.length
        + totalBytes];//numValues, length array, concatenated bytes
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    //write the length
    byteBuffer.putInt(values.length);
    //write the length of each element
    for (final byte[] value : values) {
      byteBuffer.putInt(value.length);
    }
    //write the content of each element
    //todo:maybe there is a smart way to avoid 3 loops but at the cost of allocating more memory upfront and resize as needed
    for (final byte[] value : values) {
      byteBuffer.put(value);
    }
//    System.out.println("Inserting bytes of length:" + bytes.length);
    _indexWriter.putBytes(bytes);
  }

  @Override
  public void close()
      throws IOException {
    _indexWriter.close();
  }

  private static void testSV() throws IOException {
    final File dir = new File(System.getProperty("java.io.tmpdir"));

    String column = "testCol";
    int numDocs = 10000;
    int maxLength = 100;
    File file = new File(dir, column + Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);
    file.delete();
    SingleValueVarByteRawIndexCreator creator = new SingleValueVarByteRawIndexCreator(dir,
        ChunkCompressionType.SNAPPY, column, numDocs, DataType.STRING, maxLength, true,
        BaseChunkSVForwardIndexWriter.DEFAULT_VERSION);
    Random random = new Random();
    for (int i = 0; i < numDocs; i++) {
      int length = random.nextInt(maxLength);
      char[] value = new char[length];
      Arrays.fill(value, 'a');
      creator.putString(new String(value));
    }
    creator.close();

    //read
    final PinotDataBuffer buffer = PinotDataBuffer
        .mapFile(file, true, 0, file.length(), ByteOrder.BIG_ENDIAN, "");
    VarByteChunkSVForwardIndexReader reader = new VarByteChunkSVForwardIndexReader(buffer,
        DataType.STRING);
    final ChunkReaderContext context = reader.createContext();
    for (int i = 0; i < numDocs; i++) {
      String value = reader.getString(i, context);
      System.out.println("value = " + value);
    }
  }
}
