/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.segment.creator.impl.fwd;

import com.linkedin.pinot.core.io.compression.ChunkCompressorFactory;
import com.linkedin.pinot.core.io.writer.impl.FixedByteSingleValueMultiColWriter;
import com.linkedin.pinot.core.io.writer.impl.v1.FixedByteChunkSingleValueWriter;
import com.linkedin.pinot.core.segment.creator.BaseSingleValueRawIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import java.io.File;
import java.io.IOException;


/**
 * Implementation of {@link com.linkedin.pinot.core.segment.creator.SingleValueRawIndexCreator}
 * <ul>
 *   <li> Uses {@link FixedByteSingleValueMultiColWriter} as the underlying writer. </li>
 *   <li> Should be used for writing fixed byte data (int, long, float & double). </li>
 * </ul>
 */
public class SingleValueFixedByteRawIndexCreator extends BaseSingleValueRawIndexCreator {
  private static final int NUM_DOCS_PER_CHUNK = 1000; // TODO: Auto-derive this based on metadata.

  final FixedByteChunkSingleValueWriter _indexWriter;

  /**
   * Constructor for the class
   *
   * @param baseIndexDir Index directory
   * @param compressionType Type of compression to use
   * @param column Name of column to index
   * @param totalDocs Total number of documents to index
   * @param sizeOfEntry Size of entry (in bytes)
   * @throws IOException
   */
  public SingleValueFixedByteRawIndexCreator(File baseIndexDir, ChunkCompressorFactory.CompressionType compressionType,
      String column, int totalDocs, int sizeOfEntry) throws IOException {
    File file = new File(baseIndexDir, column + V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);
    _indexWriter =
        new FixedByteChunkSingleValueWriter(file, compressionType, totalDocs, NUM_DOCS_PER_CHUNK, sizeOfEntry);
  }

  @Override
  public void index(int docId, int valueToIndex) {
    _indexWriter.setInt(docId, valueToIndex);
  }

  @Override
  public void index(int docId, long valueToIndex) {
    _indexWriter.setLong(docId, valueToIndex);
  }

  @Override
  public void index(int docId, float valueToIndex) {
    _indexWriter.setFloat(docId, valueToIndex);
  }

  @Override
  public void index(int docId, double valueToIndex) {
    _indexWriter.setDouble(docId, valueToIndex);
  }

  @Override
  public void index(int docId, Object valueToIndex) {
    if (valueToIndex instanceof Integer) {
      index(docId, ((Integer) valueToIndex).intValue());
    } else if (valueToIndex instanceof Long) {
      index(docId, ((Long) valueToIndex).longValue());
    } else if (valueToIndex instanceof Float) {
      index(docId, ((Float) valueToIndex).floatValue());
    } else if (valueToIndex instanceof Double) {
      index(docId, ((Double) valueToIndex).doubleValue());
    } else {
      throw new IllegalArgumentException(
          "Illegal argument type for fixed length raw indexing: " + valueToIndex.getClass().getName());
    }
  }

  @Override
  public void close()
      throws IOException {
    _indexWriter.close();
  }
}
