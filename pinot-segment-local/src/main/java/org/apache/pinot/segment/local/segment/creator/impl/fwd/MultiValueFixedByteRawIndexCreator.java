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
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriter;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriterV4;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriterV5;
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
   * @param deriveNumDocsPerChunk true if writer should auto-derive the number of rows per chunk
   * @param writerVersion writer format version
   * @param targetMaxChunkSizeBytes target max chunk size in bytes, applicable only for V4 or when
   *                                deriveNumDocsPerChunk is true
   */
  public MultiValueFixedByteRawIndexCreator(File baseIndexDir, ChunkCompressionType compressionType, String column,
      int totalDocs, DataType valueType, int maxNumberOfMultiValueElements, boolean deriveNumDocsPerChunk,
      int writerVersion, int targetMaxChunkSizeBytes, int targetDocsPerChunk)
      throws IOException {
    this(new File(baseIndexDir, column + Indexes.RAW_MV_FORWARD_INDEX_FILE_EXTENSION), compressionType, totalDocs,
        valueType, maxNumberOfMultiValueElements, deriveNumDocsPerChunk, writerVersion, targetMaxChunkSizeBytes,
        targetDocsPerChunk);
  }

  public MultiValueFixedByteRawIndexCreator(File indexFile, ChunkCompressionType compressionType, int totalDocs,
      DataType valueType, int maxNumberOfMultiValueElements, boolean deriveNumDocsPerChunk, int writerVersion)
      throws IOException {
    this(indexFile, compressionType, totalDocs, valueType, maxNumberOfMultiValueElements, deriveNumDocsPerChunk,
        writerVersion, ForwardIndexConfig.getDefaultTargetMaxChunkSizeBytes(),
        ForwardIndexConfig.getDefaultTargetDocsPerChunk());
  }

  public MultiValueFixedByteRawIndexCreator(File indexFile, ChunkCompressionType compressionType, int totalDocs,
      DataType valueType, int maxNumberOfMultiValueElements, boolean deriveNumDocsPerChunk, int writerVersion,
      int targetMaxChunkSizeBytes, int targetDocsPerChunk)
      throws IOException {
    if (writerVersion < VarByteChunkForwardIndexWriterV4.VERSION) {
      // Store the length followed by the values
      int totalMaxLength = Integer.BYTES + (maxNumberOfMultiValueElements * valueType.getStoredType().size());
      int numDocsPerChunk = deriveNumDocsPerChunk ? Math.max(targetMaxChunkSizeBytes / (totalMaxLength
          + VarByteChunkForwardIndexWriter.CHUNK_HEADER_ENTRY_ROW_OFFSET_SIZE), 1) : targetDocsPerChunk;
      _indexWriter =
          new VarByteChunkForwardIndexWriter(indexFile, compressionType, totalDocs, numDocsPerChunk, totalMaxLength,
              writerVersion);
    } else {
      if (writerVersion == VarByteChunkForwardIndexWriterV5.VERSION) {
        // Store only the values
        int totalMaxLength = maxNumberOfMultiValueElements * valueType.getStoredType().size();
        int chunkSize =
            ForwardIndexUtils.getDynamicTargetChunkSize(totalMaxLength, targetDocsPerChunk, targetMaxChunkSizeBytes);
        _indexWriter = new VarByteChunkForwardIndexWriterV5(indexFile, compressionType, chunkSize);
      } else {
        // Store the length followed by the values
        int totalMaxLength = Integer.BYTES + (maxNumberOfMultiValueElements * valueType.getStoredType().size());
        int chunkSize =
            ForwardIndexUtils.getDynamicTargetChunkSize(totalMaxLength, targetDocsPerChunk, targetMaxChunkSizeBytes);
        _indexWriter = new VarByteChunkForwardIndexWriterV4(indexFile, compressionType, chunkSize);
      }
    }
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
    _indexWriter.putIntMV(values);
  }

  @Override
  public void putLongMV(long[] values) {
    _indexWriter.putLongMV(values);
  }

  @Override
  public void putFloatMV(float[] values) {
    _indexWriter.putFloatMV(values);
  }

  @Override
  public void putDoubleMV(double[] values) {
    _indexWriter.putDoubleMV(values);
  }

  @Override
  public void close()
      throws IOException {
    _indexWriter.close();
  }
}
