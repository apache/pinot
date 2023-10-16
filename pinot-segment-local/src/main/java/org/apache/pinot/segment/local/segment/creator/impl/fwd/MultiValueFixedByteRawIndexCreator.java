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
package org.apache.pinot.segment.local.segment.creator.impl.fwd;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriter;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriterV4;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkWriter;
import org.apache.pinot.segment.spi.V1Constants.Indexes;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Raw (non-dictionary-encoded) forward index creator for multi-value column of fixed length data type (INT, LONG,
 * FLOAT, DOUBLE).
 */
public class MultiValueFixedByteRawIndexCreator implements ForwardIndexCreator {
  private static final int DEFAULT_NUM_DOCS_PER_CHUNK = 1000;
  private static final int TARGET_MAX_CHUNK_SIZE = 1024 * 1024;

  private final VarByteChunkWriter _indexWriter;
  private final DataType _valueType;

  /**
   * Create a var-byte raw index creator for the given column
   *
   * @param baseIndexDir Index directory
   * @param compressionType Type of compression to use
   * @param column Name of column to index
   * @param totalDocs Total number of documents to index
   * @param valueType Type of the values
   */
  public MultiValueFixedByteRawIndexCreator(File baseIndexDir, ChunkCompressionType compressionType, String column,
      int totalDocs, DataType valueType, int maxNumberOfMultiValueElements)
      throws IOException {
    this(baseIndexDir, compressionType, column, totalDocs, valueType, maxNumberOfMultiValueElements, false,
        ForwardIndexConfig.DEFAULT_RAW_WRITER_VERSION);
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
   * @param writerVersion writer format version
   */
  public MultiValueFixedByteRawIndexCreator(File baseIndexDir, ChunkCompressionType compressionType, String column,
      int totalDocs, DataType valueType, int maxNumberOfMultiValueElements, boolean deriveNumDocsPerChunk,
      int writerVersion)
      throws IOException {
    File file = new File(baseIndexDir, column + Indexes.RAW_MV_FORWARD_INDEX_FILE_EXTENSION);
    // Store the length followed by the values
    int totalMaxLength = Integer.BYTES + (maxNumberOfMultiValueElements * valueType.getStoredType().size());
    int numDocsPerChunk = deriveNumDocsPerChunk ? Math.max(
        TARGET_MAX_CHUNK_SIZE / (totalMaxLength + VarByteChunkForwardIndexWriter.CHUNK_HEADER_ENTRY_ROW_OFFSET_SIZE), 1)
        : DEFAULT_NUM_DOCS_PER_CHUNK;
    _indexWriter = writerVersion < VarByteChunkForwardIndexWriterV4.VERSION ? new VarByteChunkForwardIndexWriter(file,
        compressionType, totalDocs, numDocsPerChunk, totalMaxLength, writerVersion)
        : new VarByteChunkForwardIndexWriterV4(file, compressionType, TARGET_MAX_CHUNK_SIZE);
    _valueType = valueType;
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
  public void putIntMV(int[] values) {
    byte[] bytes = new byte[Integer.BYTES + values.length * Integer.BYTES];
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    //write the length
    byteBuffer.putInt(values.length);
    //write the content of each element
    for (int value : values) {
      byteBuffer.putInt(value);
    }
    _indexWriter.putBytes(bytes);
  }

  @Override
  public void putLongMV(long[] values) {
    byte[] bytes = new byte[Integer.BYTES + values.length * Long.BYTES];
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    //write the length
    byteBuffer.putInt(values.length);
    //write the content of each element
    for (long value : values) {
      byteBuffer.putLong(value);
    }
    _indexWriter.putBytes(bytes);
  }

  @Override
  public void putFloatMV(float[] values) {
    byte[] bytes = new byte[Integer.BYTES + values.length * Float.BYTES];
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    //write the length
    byteBuffer.putInt(values.length);
    //write the content of each element
    for (float value : values) {
      byteBuffer.putFloat(value);
    }
    _indexWriter.putBytes(bytes);
  }

  @Override
  public void putDoubleMV(double[] values) {
    byte[] bytes = new byte[Integer.BYTES + values.length * Double.BYTES];
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    //write the length
    byteBuffer.putInt(values.length);
    //write the content of each element
    for (double value : values) {
      byteBuffer.putDouble(value);
    }
    _indexWriter.putBytes(bytes);
  }

  @Override
  public void close()
      throws IOException {
    _indexWriter.close();
  }
}
