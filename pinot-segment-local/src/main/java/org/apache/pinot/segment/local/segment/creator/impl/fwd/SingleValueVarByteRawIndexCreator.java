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

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriter;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriterV4;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkWriter;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.readers.Vector;


/**
 * Raw (non-dictionary-encoded) forward index creator for single-value column of variable length data type (BIG_DECIMAL,
 * STRING, BYTES).
 */
public class SingleValueVarByteRawIndexCreator implements ForwardIndexCreator {
  private static final int DEFAULT_NUM_DOCS_PER_CHUNK = 1000;
  private static final int TARGET_MAX_CHUNK_SIZE = 1024 * 1024;

  private final VarByteChunkWriter _indexWriter;
  private final DataType _valueType;

  /**
   * Create a var-byte raw index creator for the given column
   * @param baseIndexDir Index directory
   * @param compressionType Type of compression to use
   * @param column Name of column to index
   * @param totalDocs Total number of documents to index
   * @param valueType Type of the values
   * @param maxLength length of longest entry (in bytes)
   * @throws IOException
   */
  public SingleValueVarByteRawIndexCreator(File baseIndexDir, ChunkCompressionType compressionType, String column,
      int totalDocs, DataType valueType, int maxLength)
      throws IOException {
    this(baseIndexDir, compressionType, column, totalDocs, valueType, maxLength, false,
        ForwardIndexConfig.DEFAULT_RAW_WRITER_VERSION);
  }

  /**
   * Create a var-byte raw index creator for the given column
   * @param baseIndexDir Index directory
   * @param compressionType Type of compression to use
   * @param column Name of column to index
   * @param totalDocs Total number of documents to index
   * @param valueType Type of the values
   * @param maxLength length of longest entry (in bytes)
   * @param deriveNumDocsPerChunk true if writer should auto-derive the number of rows per chunk
   * @param writerVersion writer format version
   * @throws IOException
   */
  public SingleValueVarByteRawIndexCreator(File baseIndexDir, ChunkCompressionType compressionType, String column,
      int totalDocs, DataType valueType, int maxLength, boolean deriveNumDocsPerChunk, int writerVersion)
      throws IOException {
    File file = new File(baseIndexDir, column + V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);
    int numDocsPerChunk = deriveNumDocsPerChunk ? getNumDocsPerChunk(maxLength) : DEFAULT_NUM_DOCS_PER_CHUNK;
    _indexWriter = writerVersion < VarByteChunkForwardIndexWriterV4.VERSION ? new VarByteChunkForwardIndexWriter(file,
        compressionType, totalDocs, numDocsPerChunk, maxLength, writerVersion)
        : new VarByteChunkForwardIndexWriterV4(file, compressionType, TARGET_MAX_CHUNK_SIZE);
    _valueType = valueType;
  }

  @VisibleForTesting
  public static int getNumDocsPerChunk(int lengthOfLongestEntry) {
    int overheadPerEntry = lengthOfLongestEntry + VarByteChunkForwardIndexWriter.CHUNK_HEADER_ENTRY_ROW_OFFSET_SIZE;
    return Math.max(TARGET_MAX_CHUNK_SIZE / overheadPerEntry, 1);
  }

  @Override
  public boolean isDictionaryEncoded() {
    return false;
  }

  @Override
  public boolean isSingleValue() {
    return true;
  }

  @Override
  public DataType getValueType() {
    return _valueType;
  }

  @Override
  public void putBigDecimal(BigDecimal value) {
    _indexWriter.putBigDecimal(value);
  }

  @Override
  public void putString(String value) {
    _indexWriter.putString(value);
  }

  @Override
  public void putBytes(byte[] value) {
    _indexWriter.putBytes(value);
  }

  @Override
  public void putVector(Vector value) {
    _indexWriter.putVector(value);
  }

  @Override
  public void close()
      throws IOException {
    _indexWriter.close();
  }
}
