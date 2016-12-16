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

import com.linkedin.pinot.core.io.compression.ChunkCompressor;
import com.linkedin.pinot.core.io.compression.ChunkCompressorFactory;
import com.linkedin.pinot.core.io.writer.impl.v1.VarByteSingleValueWriter;
import com.linkedin.pinot.core.segment.creator.SingleValueRawIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;


public class SingleValueVarLengthRawIndexCreator implements SingleValueRawIndexCreator, Closeable {
  private static final int NUM_DOCS_PER_CHUNK = 1000; // TODO: Auto-derive this based on metadata.
  VarByteSingleValueWriter _indexWriter;

  public SingleValueVarLengthRawIndexCreator(File baseIndexDir, String column, int totalDocs, int maxLength)
      throws IOException {
    File file = new File(baseIndexDir, column + V1Constants.Indexes.RAW_SV_FWD_IDX_FILE_EXTENTION);

    ChunkCompressor compressor = ChunkCompressorFactory.getCompressor("snappy");
    _indexWriter = new VarByteSingleValueWriter(file, compressor, totalDocs, NUM_DOCS_PER_CHUNK, maxLength);
  }

  @Override
  public void index(int docId, String valueToIndex)
      throws IOException {
    _indexWriter.setString(docId, valueToIndex);
  }

  @Override
  public void index(int docId, byte[] valueToIndex) {
    // TODO: Add support for byte[] values.
    throw new UnsupportedOperationException();
  }

  @Override
  public void close()
      throws IOException {
    _indexWriter.close();
  }
}
