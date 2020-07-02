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
package org.apache.pinot.core.segment.creator.impl.fwd;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import org.apache.pinot.core.io.compression.ChunkCompressorFactory;
import org.apache.pinot.core.io.writer.impl.v1.VarByteChunkSingleValueWriter;
import org.apache.pinot.core.segment.creator.BaseSingleValueRawIndexCreator;
import org.apache.pinot.core.segment.creator.impl.V1Constants;


public class SingleValueVarByteRawIndexCreator extends BaseSingleValueRawIndexCreator {
  private static final int DEFAULT_NUM_DOCS_PER_CHUNK = 1000;
  private static final int TARGET_MAX_CHUNK_SIZE = 1024 * 1024;

  private final VarByteChunkSingleValueWriter _indexWriter;

  public SingleValueVarByteRawIndexCreator(File baseIndexDir, ChunkCompressorFactory.CompressionType compressionType,
      String column, int totalDocs, int maxLength)
      throws IOException {
    this(baseIndexDir, compressionType, column, totalDocs, maxLength, false);
  }

  public SingleValueVarByteRawIndexCreator(File baseIndexDir, ChunkCompressorFactory.CompressionType compressionType,
      String column, int totalDocs, int maxLength, boolean deriveNumDocsPerChunk)
      throws IOException {
    File file = new File(baseIndexDir, column + V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);
    int numDocsPerChunk = deriveNumDocsPerChunk ? getNumDocsPerChunk(maxLength) : DEFAULT_NUM_DOCS_PER_CHUNK;
    _indexWriter = new VarByteChunkSingleValueWriter(file, compressionType, totalDocs, numDocsPerChunk, maxLength);
  }

  @VisibleForTesting
  public static int getNumDocsPerChunk(int lengthOfLongestEntry) {
    int overheadPerEntry = lengthOfLongestEntry + VarByteChunkSingleValueWriter.CHUNK_HEADER_ENTRY_ROW_OFFSET_SIZE;
    return Math.max(TARGET_MAX_CHUNK_SIZE / overheadPerEntry, 1);
  }

  @Override
  public void index(int docId, String valueToIndex) {
    _indexWriter.setString(docId, valueToIndex);
  }

  @Override
  public void index(int docId, byte[] valueToIndex) {
    _indexWriter.setBytes(docId, valueToIndex);
  }

  @Override
  public void index(int docId, Object valueToIndex) {
    if (valueToIndex instanceof String) {
      _indexWriter.setString(docId, (String) valueToIndex);
    } else if (valueToIndex instanceof byte[]) {
      _indexWriter.setBytes(docId, (byte[]) valueToIndex);
    } else {
      throw new IllegalArgumentException(
          "Illegal data type for variable length indexing: " + valueToIndex.getClass().getName());
    }
  }

  @Override
  public void close()
      throws IOException {
    _indexWriter.close();
  }
}
