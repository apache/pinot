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
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 Same as MultiValueFixedByteRawIndexCreator, but without storing the number of elements for each row.
 */
public class MultiValueFixedByteRawIndexCreatorV2 extends MultiValueFixedByteRawIndexCreator {
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
  public MultiValueFixedByteRawIndexCreatorV2(File baseIndexDir, ChunkCompressionType compressionType, String column,
      int totalDocs, DataType valueType, int maxNumberOfMultiValueElements, boolean deriveNumDocsPerChunk,
      int writerVersion, int targetMaxChunkSizeBytes, int targetDocsPerChunk)
      throws IOException {
    super(baseIndexDir, compressionType, column, totalDocs, valueType, maxNumberOfMultiValueElements,
        deriveNumDocsPerChunk, writerVersion, targetMaxChunkSizeBytes, targetDocsPerChunk);
  }

  public MultiValueFixedByteRawIndexCreatorV2(File indexFile, ChunkCompressionType compressionType, int totalDocs,
      DataType valueType, int maxNumberOfMultiValueElements, boolean deriveNumDocsPerChunk, int writerVersion)
      throws IOException {
    super(indexFile, compressionType, totalDocs, valueType, maxNumberOfMultiValueElements, deriveNumDocsPerChunk,
        writerVersion);
  }

  public MultiValueFixedByteRawIndexCreatorV2(File indexFile, ChunkCompressionType compressionType, int totalDocs,
      DataType valueType, int maxNumberOfMultiValueElements, boolean deriveNumDocsPerChunk, int writerVersion,
      int targetMaxChunkSizeBytes, int targetDocsPerChunk)
      throws IOException {
    super(indexFile, compressionType, totalDocs, valueType, maxNumberOfMultiValueElements, deriveNumDocsPerChunk,
        writerVersion, targetMaxChunkSizeBytes, targetDocsPerChunk);
  }

  @Override
  protected int computeTotalMaxLength(int maxNumberOfMultiValueElements, DataType valueType) {
    return maxNumberOfMultiValueElements * valueType.getStoredType().size();
  }

  @Override
  public void putIntMV(int[] values) {
    byte[] bytes = new byte[values.length * Integer.BYTES];
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    //write the content of each element
    for (int value : values) {
      byteBuffer.putInt(value);
    }
    _indexWriter.putBytes(bytes);
  }

  @Override
  public void putLongMV(long[] values) {
    byte[] bytes = new byte[values.length * Long.BYTES];
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    //write the content of each element
    for (long value : values) {
      byteBuffer.putLong(value);
    }
    _indexWriter.putBytes(bytes);
  }

  @Override
  public void putFloatMV(float[] values) {
    byte[] bytes = new byte[values.length * Float.BYTES];
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    //write the content of each element
    for (float value : values) {
      byteBuffer.putFloat(value);
    }
    _indexWriter.putBytes(bytes);
  }

  @Override
  public void putDoubleMV(double[] values) {
    byte[] bytes = new byte[values.length * Double.BYTES];
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    //write the content of each element
    for (double value : values) {
      byteBuffer.putDouble(value);
    }
    _indexWriter.putBytes(bytes);
  }
}
