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
import org.apache.pinot.segment.local.io.codec.CodecPipelineExecutor;
import org.apache.pinot.segment.local.io.writer.impl.FixedByteChunkForwardIndexWriter;
import org.apache.pinot.segment.local.io.writer.impl.FixedByteChunkForwardIndexWriterV7;
import org.apache.pinot.segment.local.io.writer.impl.FixedByteValueWriter;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/// Raw (non-dictionary-encoded) forward index creator for single-value column of fixed length
/// data type (`INT`, `LONG`, `FLOAT`, `DOUBLE`).
///
/// The creator holds a single [FixedByteValueWriter]; the concrete writer (legacy chunk format vs
/// V7 codec-pipeline format) is chosen by the constructor and all `put*` calls delegate to it
/// without branching. FLOAT/DOUBLE are supported only by the legacy writer; the V7 writer rejects
/// them at the writer level.
public class SingleValueFixedByteRawIndexCreator implements CompressionStatsTrackingForwardIndexCreator {
  private final FixedByteValueWriter _indexWriter;
  private final DataType _valueType;
  private final ChunkCompressionType _chunkCompressionType;

  /// Constructor for the class.
  ///
  /// @param baseIndexDir Index directory
  /// @param compressionType Type of compression to use
  /// @param column Name of column to index
  /// @param totalDocs Total number of documents to index
  /// @param valueType Type of the values
  public SingleValueFixedByteRawIndexCreator(File baseIndexDir, ChunkCompressionType compressionType, String column,
      int totalDocs, DataType valueType)
      throws IOException {
    this(baseIndexDir, compressionType, column, totalDocs, valueType, ForwardIndexConfig.getDefaultRawWriterVersion(),
        ForwardIndexConfig.getDefaultTargetDocsPerChunk());
  }

  /// Constructor for the class.
  ///
  /// @param baseIndexDir Index directory
  /// @param compressionType Type of compression to use
  /// @param column Name of column to index
  /// @param totalDocs Total number of documents to index
  /// @param valueType Type of the values
  /// @param writerVersion writer format version
  public SingleValueFixedByteRawIndexCreator(File baseIndexDir, ChunkCompressionType compressionType, String column,
      int totalDocs, DataType valueType, int writerVersion, int targetDocsPerChunk)
      throws IOException {
    File file = new File(baseIndexDir, column + V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);
    _indexWriter =
        new FixedByteChunkForwardIndexWriter(file, compressionType, totalDocs, targetDocsPerChunk, valueType.size(),
            writerVersion);
    _valueType = valueType;
    _chunkCompressionType = compressionType;
  }

  /// Creates a raw fixed-byte creator backed by the V7 codec-pipeline writer.
  ///
  /// This path is used for transform specs (`DELTA` / `DELTADELTA` / `T64` / `GORILLA`) and
  /// compression-only specs that a legacy [ChunkCompressionType] cannot represent (e.g. `ZSTD(5)`),
  /// where the existing raw forward-index header cannot carry the full pipeline.
  public SingleValueFixedByteRawIndexCreator(File baseIndexDir, String column, int totalDocs, DataType valueType,
      int targetDocsPerChunk, CodecPipelineExecutor executor)
      throws IOException {
    File file = new File(baseIndexDir, column + V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);
    _indexWriter = new FixedByteChunkForwardIndexWriterV7(file, executor, totalDocs, targetDocsPerChunk,
        valueType.size());
    _valueType = valueType;
    _chunkCompressionType = null;
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
  public void putInt(int value) {
    _indexWriter.putInt(value);
  }

  @Override
  public void putLong(long value) {
    _indexWriter.putLong(value);
  }

  @Override
  public void putFloat(float value) {
    _indexWriter.putFloat(value);
  }

  @Override
  public void putDouble(double value) {
    _indexWriter.putDouble(value);
  }

  @Override
  public void close()
      throws IOException {
    _indexWriter.close();
  }

  @Override
  public long getRawForwardIndexUncompressedValueSizeInBytes() {
    return _indexWriter.getRawForwardIndexUncompressedValueSizeInBytes();
  }

  @Override
  public ChunkCompressionType getRawForwardIndexChunkCompressionType() {
    return _chunkCompressionType;
  }

  @Override
  public void enableRawForwardIndexUncompressedValueSizeTracking() {
    _indexWriter.enableRawForwardIndexUncompressedValueSizeTracking();
  }
}
