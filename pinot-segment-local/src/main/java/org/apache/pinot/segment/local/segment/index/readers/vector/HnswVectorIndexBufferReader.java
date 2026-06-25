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
package org.apache.pinot.segment.local.segment.index.readers.vector;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.lucene.store.Directory;
import org.apache.pinot.segment.local.segment.index.readers.text.LuceneTextIndexBufferReader;
import org.apache.pinot.segment.local.segment.index.readers.text.LuceneTextIndexHeader;
import org.apache.pinot.segment.local.segment.index.readers.text.PinotBufferLuceneDirectory;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility for reading an HNSW vector index from a combined buffer ({@link PinotDataBuffer}).
 *
 * <p>The combined buffer uses the same {@code LUCENE_V2} layout as the text index (written by
 * {@code HnswVectorIndexCombined}). This class delegates header and metadata parsing to
 * {@link LuceneTextIndexBufferReader}, then constructs a {@link PinotBufferLuceneDirectory} that
 * Lucene's {@code DirectoryReader} can open without any filesystem access.</p>
 *
 * <p>Two entry-points are provided:</p>
 * <ul>
 *   <li>{@link #createLuceneDirectory} — a read-only {@link Directory} over the Lucene index
 *       files embedded in the buffer.</li>
 *   <li>{@link #extractDocIdMappingBuffer} — a sub-buffer view covering just the docId mapping
 *       bytes, or {@code null} if the mapping was not packed.</li>
 * </ul>
 */
public final class HnswVectorIndexBufferReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(HnswVectorIndexBufferReader.class);

  private HnswVectorIndexBufferReader() {
  }

  /**
   * Creates a Lucene {@link Directory} that reads all Lucene index files from a combined HNSW
   * buffer. The docId mapping file (a Pinot-only artifact with the
   * {@link V1Constants.Indexes#VECTOR_HNSW_INDEX_DOCID_MAPPING_FILE_EXTENSION} suffix) is
   * excluded from the directory view so that {@code DirectoryReader} does not see an unknown file.
   *
   * <p>The directory does <em>not</em> own the buffer — closing it is a no-op. The caller must
   * keep the buffer alive for as long as the directory (and any reader opened on it) is in use.</p>
   *
   * @param indexBuffer combined buffer in LUCENE_V2 format
   * @param column      column name; used to identify and exclude the mapping file
   * @return a read-only {@link Directory} backed by the buffer
   * @throws IOException if the buffer is malformed or the magic/version check fails
   */
  public static Directory createLuceneDirectory(PinotDataBuffer indexBuffer, String column)
      throws IOException {
    List<String> fileNames = LuceneTextIndexBufferReader.listFiles(indexBuffer);
    Map<String, LuceneTextIndexHeader.FileInfo> fileMap = new HashMap<>(fileNames.size());

    String mappingFileName = column + V1Constants.Indexes.VECTOR_HNSW_INDEX_DOCID_MAPPING_FILE_EXTENSION;
    for (String name : fileNames) {
      if (name.equals(mappingFileName)) {
        // Skip: not a Lucene index file; presenting it to DirectoryReader triggers an error.
        continue;
      }
      LuceneTextIndexHeader.FileInfo info = LuceneTextIndexBufferReader.getFileInfo(indexBuffer, name);
      if (info != null) {
        fileMap.put(name, info);
      }
    }

    LOGGER.debug("Creating buffer-backed Lucene directory for HNSW column '{}' with {} file(s)", column,
        fileMap.size());
    return new PinotBufferLuceneDirectory(indexBuffer, fileMap, column);
  }

  /**
   * Extracts a sub-buffer covering the docId mapping bytes packed inside the combined buffer.
   *
   * <p>Returns {@code null} when the mapping was not included (e.g. the index was built from an
   * in-memory segment that had not yet materialised a mapping file). The caller must build the
   * mapping from the Lucene index in that case.</p>
   *
   * <p>The returned sub-buffer is a {@link PinotDataBuffer#view view} of the original buffer and
   * shares its lifetime. The caller must not close it independently.</p>
   *
   * @param indexBuffer combined buffer in LUCENE_V2 format
   * @param column      column name; used to identify the mapping file entry
   * @return sub-buffer for the mapping bytes, or {@code null} if not present
   * @throws IOException if header parsing fails
   */
  @Nullable
  public static PinotDataBuffer extractDocIdMappingBuffer(PinotDataBuffer indexBuffer, String column)
      throws IOException {
    String mappingFileName = column + V1Constants.Indexes.VECTOR_HNSW_INDEX_DOCID_MAPPING_FILE_EXTENSION;
    LuceneTextIndexHeader.FileInfo fileInfo = LuceneTextIndexBufferReader.getFileInfo(indexBuffer, mappingFileName);
    if (fileInfo == null) {
      return null;
    }
    return indexBuffer.view(fileInfo.getOffset(), fileInfo.getOffset() + fileInfo.getSize());
  }
}
