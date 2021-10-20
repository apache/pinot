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
import org.apache.pinot.segment.local.io.writer.impl.BaseChunkSVForwardIndexWriter;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkSVForwardIndexWriter;
import org.apache.pinot.segment.spi.V1Constants.Indexes;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
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
   * @param maxRowLengthInBytes the length in bytes of the largest row
   */
  public MultiValueVarByteRawIndexCreator(File baseIndexDir, ChunkCompressionType compressionType,
      String column,
      int totalDocs, DataType valueType, int maxRowLengthInBytes)
      throws IOException {
    this(baseIndexDir, compressionType, column, totalDocs, valueType, false,
        BaseChunkSVForwardIndexWriter.DEFAULT_VERSION, maxRowLengthInBytes);
  }

  /**
   * Create a var-byte raw index creator for the given column
   *
   * @param baseIndexDir Index directory
   * @param compressionType Type of compression to use
   * @param column Name of column to index
   * @param totalDocs Total number of documents to index
   * @param valueType Type of the values
   * @param maxRowLengthInBytes the size in bytes of the largest row, the chunk size cannot be smaller than this
   * @param deriveNumDocsPerChunk true if writer should auto-derive the number of rows per
   *     chunk
   * @param writerVersion writer format version
   * @param maxRowLengthInBytes the length in bytes of the largest row
   */
  public MultiValueVarByteRawIndexCreator(File baseIndexDir, ChunkCompressionType compressionType,
      String column, int totalDocs, DataType valueType, boolean deriveNumDocsPerChunk, int writerVersion,
      int maxRowLengthInBytes)
      throws IOException {
    //we will prepend the actual content with numElements and length array containing length of each element
    int totalMaxLength = Integer.BYTES + Math.max(maxRowLengthInBytes, TARGET_MAX_CHUNK_SIZE);
    File file = new File(baseIndexDir,
        column + Indexes.RAW_MV_FORWARD_INDEX_FILE_EXTENSION);
    int numDocsPerChunk =
        deriveNumDocsPerChunk ? getNumDocsPerChunk(totalMaxLength) : DEFAULT_NUM_DOCS_PER_CHUNK;
    _indexWriter = new VarByteChunkSVForwardIndexWriter(file, compressionType, totalDocs,
        numDocsPerChunk, totalMaxLength,
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
    _indexWriter.putStrings(values);
  }

  @Override
  public void putBytesMV(final byte[][] values) {
    _indexWriter.putByteArrays(values);
  }

  @Override
  public void close()
      throws IOException {
    _indexWriter.close();
  }
}
