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

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriter;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriterV4;
import org.apache.pinot.segment.spi.V1Constants.Indexes;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Raw (non-dictionary-encoded) forward index creator for multi-value column of variable length data type (STRING,
 * BYTES).
 */
public class MultiValueVarByteRawIndexCreator implements ForwardIndexCreator {
  private static final int TARGET_MAX_CHUNK_SIZE = 1024 * 1024;

  private final VarByteChunkForwardIndexWriter _indexWriter;
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
   * @param maxNumberOfElements the maximum number of elements in a row
   */
  public MultiValueVarByteRawIndexCreator(File baseIndexDir, ChunkCompressionType compressionType, String column,
      int totalDocs, DataType valueType, int maxRowLengthInBytes, int maxNumberOfElements)
      throws IOException {
    this(baseIndexDir, compressionType, column, totalDocs, valueType, ForwardIndexConfig.DEFAULT_RAW_WRITER_VERSION,
        maxRowLengthInBytes, maxNumberOfElements);
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
   * @param maxNumberOfElements the maximum number of elements in a row
   * @param writerVersion writer format version
   */
  public MultiValueVarByteRawIndexCreator(File baseIndexDir, ChunkCompressionType compressionType, String column,
      int totalDocs, DataType valueType, int writerVersion, int maxRowLengthInBytes, int maxNumberOfElements)
      throws IOException {
    //we will prepend the actual content with numElements and length array containing length of each element
    int totalMaxLength = getTotalRowStorageBytes(maxNumberOfElements, maxRowLengthInBytes);

    File file = new File(baseIndexDir, column + Indexes.RAW_MV_FORWARD_INDEX_FILE_EXTENSION);
    int numDocsPerChunk = Math.max(
        TARGET_MAX_CHUNK_SIZE / (totalMaxLength + VarByteChunkForwardIndexWriter.CHUNK_HEADER_ENTRY_ROW_OFFSET_SIZE),
        1);
    // TODO: Support V4 MV reader
    // Currently fall back to V2 for backward compatible
    if (writerVersion == VarByteChunkForwardIndexWriterV4.VERSION) {
      writerVersion = 2;
    }
    _indexWriter = new VarByteChunkForwardIndexWriter(file, compressionType, totalDocs, numDocsPerChunk, totalMaxLength,
        writerVersion);
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

  /**
   * The actual content in an MV array is prepended with 2 prefixes:
   * 1. elementLengthStoragePrefixInBytes - bytes required to store the length of each element in the largest array
   * 2. numElementsStoragePrefixInBytes - Number of elements in the array
   *
   * This function returns the total bytes needed to store (1) elementLengthStoragePrefixInBytes
   */
  public static int getElementLengthStoragePrefixInBytes(int maxNumberOfElements) {
    return Integer.BYTES * maxNumberOfElements;
  }

  /**
   * The actual content in an MV array is prepended with 2 prefixes:
   * 1. elementLengthStoragePrefixInBytes - bytes required to store the length of each element in the largest array
   * 2. numElementsStoragePrefixInBytes - Number of elements in the array
   *
   * This function returns the bytes needed to store (2) numElementsStoragePrefixInBytes
   */
  public static int getNumElementsStoragePrefix() {
    return Integer.BYTES;
  }

  /**
   * The actual content in an MV array is prepended with 2 prefixes:
   * 1. elementLengthStoragePrefixInBytes - bytes required to store the length of each element in the largest array
   * 2. numElementsStoragePrefixInBytes - Number of elements in the array
   *
   * This function returns the bytes needed to store the (1), (2) and the actual content.
   */
  public static int getTotalRowStorageBytes(int maxNumberOfElements, int maxRowDataLengthInBytes) {
    int elementLengthStoragePrefixInBytes = getElementLengthStoragePrefixInBytes(maxNumberOfElements);
    int numElementsStoragePrefixInBytes = getNumElementsStoragePrefix();
    int totalMaxLength = elementLengthStoragePrefixInBytes + numElementsStoragePrefixInBytes + maxRowDataLengthInBytes;
    Preconditions.checkArgument(
        (elementLengthStoragePrefixInBytes | maxRowDataLengthInBytes | totalMaxLength | maxNumberOfElements) > 0,
        "integer overflow detected");

    return totalMaxLength;
  }

  /**
   * The actual content in an MV array is prepended with 2 prefixes:
   * 1. elementLengthStoragePrefixInBytes - bytes required to store the length of each element in the largest array
   * 2. numberOfElementsStoragePrefix - Number of elements in the array
   *
   * This function returns the bytes needed to store the actual content.
   */
  public static int getMaxRowDataLengthInBytes(int totalMaxLength, int maxNumberOfElements) {
    return totalMaxLength - getNumElementsStoragePrefix() - getElementLengthStoragePrefixInBytes(maxNumberOfElements);
  }
}
